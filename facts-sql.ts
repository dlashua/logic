import type { Knex } from "knex";
// eslint-disable-next-line import/no-named-as-default
import knex from "knex";
import type { Subst, Term } from "./core.ts";
import { isVar, unify, walk } from "./core.ts";

let runcnt = 0;

const DISABLE_CACHE = false;
const DISABLE_SUBSUME = false;
const DISABLE_PATTERN_MERGE = false;
const AUTO_RUN = true;

// --- Logging configuration ---
const LOG_ENABLED = false; // Set to true to enable logging globally
const LOG_IDS = new Set<string>([
  // Add log identifiers here to disable them, e.g.:
  "PATTERNS BEFORE CHECK",
  "WORKING PATTERN",
  "TABLE NOT IN PATTERNS. NEED RUN.",
  "ADDING PATTERN",
  "MERGING PATTERN",
  "TABLE IN PATTERNS. NO RUN.",
  "SKIPPING QUERY",
  "ROW RETURNED",
  "RUNNING GOAL",
  "GOAL RUN FINISHED",
  "QUERY",
  "STARTING GOAL",
  "YIELD THROUGH",
  "YIELD",
  "NO YIELD",
  "UNIFY",
  "AFTER WALK",
  "NO UNIFY (grounded mismatch)",
  "PATTERNS_BEFORE_RUN",
]);

/**
 * Logging function with identifier-based filtering and global enable/disable.
 * Usage: log("ROW_RETURNED", { ... })
 */
function log(
  id: string,
  ...args: Record<string, unknown>[]
) {
  if (!LOG_ENABLED) return;
  if (LOG_IDS.has(id)) return;
  if (args.length === 0) {
    console.dir(
      {
        log: id,
      },
      {
        depth: null,
      },
    );
  } else if (args.length === 1) {
    console.dir(
      {
        log: id,
        ...args[0],
      },
      {
        depth: null,
      },
    );
  } else {
    console.dir(
      {
        log: id,
        args,
      },
      {
        depth: null,
      },
    );
  }
}

// Pattern interface at top-level for clarity
interface Pattern extends Record<string, string | object> {
  table: string;
  params: Record<string, Term>;
  queries: string[];
}

// Helper: walk all params and collect walkedQ and whereClauses
async function walkParamsAndCollect(
  params: Record<string, Term>,
  subst: Subst
): Promise<{
  walkedQ: Record<string, Term>;
  whereClauses: { col: string; val: Term }[];
}> {
  const walkedQ: Record<string, Term> = {};
  const whereClauses: { col: string; val: Term }[] = [];
  for (const col of Object.keys(params)) {
    walkedQ[col] = await walk(params[col], subst);
    if (!isVar(walkedQ[col])) {
      whereClauses.push({
        col,
        val: walkedQ[col],
      });
    }
  }
  return {
    walkedQ,
    whereClauses,
  };
}

// Helper to build selectCols, whereClauses, walkedQ
async function buildQueryParts(params: Record<string, Term>, subst: Subst) {
  /**
   * @todo test without sorting
   */
  const selectCols = Object.keys(params).sort(); // #9: sort for cache key consistency
  // Use new helper
  const { walkedQ, whereClauses } = await walkParamsAndCollect(params, subst);
  // #9: sort whereClauses for cache key consistency
  whereClauses.sort((a, b) => a.col.localeCompare(b.col));
  return {
    selectCols,
    whereClauses,
    walkedQ,
  };
}

// Helper to find a subsumed cache entry
// Use filterValidWhereClauses in findSubsumedCacheEntry for consistency
function findSubsumedCacheEntry(
  recordCache: Map<string, any>,
  table: string,
  selectCols: string[],
  whereClauses: { col: string, val: Term }[]
) {
  // Precompute sets/maps for the query
  const selectSet = new Set(selectCols);
  // Use filtered whereClauses for consistency
  const validWhereClauses = filterValidWhereClauses(whereClauses);
  const whereMap = new Map(validWhereClauses.map(({ col, val }) => [col, val]));
  for (const [otherKey, cachedRows] of recordCache.entries()) {
    const other = JSON.parse(otherKey);
    if (other.table !== table) continue;
    const otherSelectArr = other.select as string[];
    const otherSelectSet = new Set(otherSelectArr);
    // Check if cached select is a superset of requested select
    if (!isSuperset(otherSelectSet, selectSet)) continue;
    // Only use cache if cached select includes all columns needed for whereClauses
    // Use isSuperset for where columns as well
    const whereColsSet = new Set(validWhereClauses.map(wc => wc.col));
    if (!isSuperset(otherSelectSet, whereColsSet)) continue;
    // Only use cache if cached where is a subset of requested where
    const otherWhereArr: [string, any][] = other.where as [string, any][];
    let cachedWhereIsSubset = true;
    for (const [col, val] of otherWhereArr) {
      if (!whereMap.has(col) || whereMap.get(col) !== val) {
        cachedWhereIsSubset = false;
        break;
      }
    }
    if (!cachedWhereIsSubset) continue;
    return {
      otherKey,
      cachedRows,
    };
  }
  return null;
}

// Helper to merge or add a pattern
async function mergeOrAddPattern(
  patterns: Pattern[],
  table: string,
  queryObj: Record<string, Term>,
  subst: Subst,
) {
  const walkedQ = await walkAllKeys(queryObj, subst);
  if(!DISABLE_PATTERN_MERGE) {
    pattern_loop: for (const pat of patterns) {
      if (pat.table !== table) continue;
  
      if(!allParamsGrounded(pat.params)) {
        pat.params = await walkAllKeys(pat.params, subst);
      }
  
      // arePatternsCompatible FN
      for (const col in pat.params) {
        if (!(col in walkedQ)) continue;
        const vPat = pat.params[col];
        const vNew = walkedQ[col];
        if (isVar(vPat) && isVar(vNew)) {
          if (vPat.id !== vNew.id) {
            continue pattern_loop;
          }
        } else if (isVar(vPat) !== isVar(vNew)) {
          continue pattern_loop;
        } else if (!isVar(vPat) && vPat !== vNew) {
          continue pattern_loop;
        }
      }
      
      // Merge walkedQ into pat.params, preferring existing non-variable values
      for (const col in walkedQ) {
        if (!(col in pat.params) || isVar(pat.params[col])) {
          pat.params[col] = walkedQ[col];
        }
      }
      // Optionally, merge queries if needed (not shown here)
      return true; 
    }
  }
  patterns.push({
    table,
    params: {
      ...walkedQ,
    },
    queries: [],
  });

  return false;
}

export const makeRelDB = async (
  knex_connect_options: Knex.Config,
  opts?: Record<string, string>,
) => {
  opts ??= {};

  const db = knex(knex_connect_options);
  // Grouped state for patterns and queries
  const state = {
    patterns: [] as Pattern[],
    queries: [] as string[],
    realQueries: [] as string[],
    cacheQueries: [] as string[],
    recordCache: new Map<string, any>(),
  };

  // Generalized cache/query helper for both rel and relSym
  const cacheOrQuery = async <T>({
    cacheKeyObj,
    getFn,
    subsumeFn,
    sym = false,
  }: {
    cacheKeyObj: any;
    getFn: () => Promise<{
      rows: T[];
      queryString: string;
    }>;
    subsumeFn?: () => { rows: T[]; otherKey: string } | null;
    sym?: boolean;
  }): Promise<{
    rows: T[];
    cacheType: string;
    cacheKey: string;
    queryString: string;
  }> => {
    const cacheKey = JSON.stringify(cacheKeyObj);
    if (!DISABLE_CACHE && state.recordCache.has(cacheKey)) {
      const sqlQuery = `CACHE HIT ${cacheKey}`;
      state.queries.push(sqlQuery);
      state.cacheQueries.push(sqlQuery);
      return {
        rows: state.recordCache.get(cacheKey),
        cacheType: "hit",
        cacheKey,
        queryString: sqlQuery,
      };
    } else if (!DISABLE_SUBSUME && subsumeFn) {
      const subsumed = subsumeFn();
      if (subsumed) {
        const sqlQuery = `CACHE SUBSUMED ${cacheKey} ${subsumed.otherKey}`;
        state.queries.push(sqlQuery);
        state.cacheQueries.push(sqlQuery);
        return {
          rows: subsumed.rows,
          cacheType: "subsumed",
          cacheKey,
          queryString: sqlQuery,
        };
      }
    }
    const { rows, queryString } = await getFn();
    log("QUERY", {
      sql: queryString 
    });
    state.queries.push(queryString);
    state.realQueries.push(queryString);
    state.recordCache.set(cacheKey, rows);
    return {
      rows,
      cacheType: "miss",
      cacheKey,
      queryString,
    };
  };

  const inner_run = async function* factsSqlRun (
    s: Subst,
    patterns: Pattern[],
    myruncnt: number,
  ) {
    async function* runPatterns(
      idx: number,
      subst: Subst,
    ): AsyncGenerator<Subst> {
      if (idx >= patterns.length) {
        yield subst;
        return;
      }
      const q = patterns[idx];
      log("WORKING PATTERN", {
        ...q,
        myruncnt,
      });
      // Use helper to build query parts
      const { selectCols, whereClauses, walkedQ } = await buildQueryParts(q.params, subst);
      // Filter out undefined values in whereClauses to avoid undefined bindings
      const validWhereClauses = filterValidWhereClauses(whereClauses);
      if (validWhereClauses.length !== whereClauses.length) {
        log("WARNING: Undefined value in whereClauses", {
          whereClauses,
          validWhereClauses,
        });
      }
      const cacheKeyObj = {
        table: q.table,
        select: selectCols,
        where: validWhereClauses.map(({ col, val }) => [col, val]),
      };
      const getFn = async () => {
        let k = db(q.table).select(selectCols);
        for (const { col, val } of validWhereClauses) {
          // @ts-expect-error knex types are weird
          k = k.where(col, "=", val);
        }
        const queryString = k.toString();
        const rows = await k;
        return {
          rows,
          queryString,
        };
      };
      const subsumeFn = () => {
        const subsumed = findSubsumedCacheEntry(state.recordCache, q.table, selectCols, validWhereClauses);
        if (subsumed) {
          const filteredRows = subsumed.cachedRows
            .filter((row: any) =>
              validWhereClauses.every(({ col, val }) => row[col] === val),
            )
            .map((row: any) => pick(row, selectCols));
          return {
            rows: filteredRows,
            otherKey: subsumed.otherKey,
          };
        }
        return null;
      };
      const { rows } = await cacheOrQuery({
        cacheKeyObj,
        getFn,
        subsumeFn,
      });
      // If no rows, skip yielding
      if (!rows || rows.length === 0) {
        return;
      }
      // Unify all selectCols (undo optimization)
      for (const row of rows) {
        log("ROW RETURNED", {
          row,
          myruncnt: myruncnt ?? 'N/A',
        });
        // Use unifyRowWithWalkedQ helper for unification and memoization
        const unifiedSubst = await unifyRowWithWalkedQ(
          selectCols,
          walkedQ,
          row,
          subst,
          myruncnt,
        );
        if (unifiedSubst) {
          yield* runPatterns(idx + 1, unifiedSubst);
        }
        // If unification fails, do not yield
      }
    }
    yield* runPatterns(0, s);
  };

  const rel = async (table: string) => {
    return function goal(queryObj: Record<string, Term>) {
      const myruncnt = runcnt++;

      return async function* factsSql (s: Subst) {
        await mergeOrAddPattern(state.patterns, table, queryObj, s);
        log("PATTERNS_BEFORE_RUN", {
          table,
          p: state.patterns 
        })

        if (AUTO_RUN) {
          const thispatterns = [...state.patterns];
          state.patterns.splice(0, state.patterns.length);

          for await (const s3 of inner_run(s, thispatterns, myruncnt)) {
            yield s3;
          }
        } else {
          yield s;
        }
      };
    };
  };

  // --- Symmetric SQL relation ---
  // relSym: requires explicit [key1, key2] argument for symmetric keys
  const relSym = async (table: string, keys: [string, string]) => {
    return function goal(queryObj: Record<string, Term<string | number>>) {
      const values = Object.values(queryObj);
      return async function* (s: Subst) {
        // sym relationships are only binary
        if (values.length > 2) return;
        const walkedValues: Term[] = await Promise.all(values.map(x => walk(x, s)));
        // sym relationships can't point to themselves
        if(walkedValues[0] === walkedValues[1]) return;
        // cheating type because I don't know another way to do this
        const gv = walkedValues.filter(x => !isVar(x)) as (string | number)[];
        const cacheKeyObj = {
          table,
          select: keys,
          where: gv,
          sym: true,
        };
        const getFn = async () => {
          const k = db(table).select(keys).where(
            (q) => gv.map(onegv => q.andWhere(
              (q) => keys.map(onekey => q.orWhere(
                (q) => q.where(onekey, onegv)
              ))
            ))
          );
          const queryString = k.toString();
          const rows = await k;
          return {
            rows,
            queryString,
          };
        };
        const { rows } = await cacheOrQuery({
          cacheKeyObj,
          getFn,
          sym: true,
        });
        for (const row of rows) {
   
          if (gv.length === 2) {
            if (keys.every(k => gv.includes(row[k]))) {
              yield s;
            }
            continue;
          }
          
          // Try both possible assignments for symmetric relation
          // First assignment: keys[0] <-> queryObj[keys[0]], keys[1] <-> queryObj[keys[1]]
          const s2 = new Map(s);
          const unified1 = await unify(walkedValues[0], row[keys[0]], s2);
          if (unified1) {
            const unified2 = await unify(walkedValues[1], row[keys[1]], unified1);
            if (unified2) {
              yield unified2;
              continue; // If first assignment works, skip the second
            }
          }
          
          // Second assignment: keys[0] <-> queryObj[keys[1]], keys[1] <-> queryObj[keys[0]]
          const s3 = new Map(s);
          const unified3 = await unify(walkedValues[1], row[keys[0]], s3);
          if (unified3) {
            const unified4 = await unify(walkedValues[0], row[keys[1]], unified3);
            if (unified4) {
              yield unified4;
            }
          }
          // If neither worked, do not yield
        }
      };
    };
  };

  const run = () => (s: Subst) => {
    return inner_run(s, state.patterns,-1)
  };

  return {
    rel,
    relSym,
    db,
    run,
    queries: state.queries,
    realQueries: state.realQueries,
    cacheQueries: state.cacheQueries,
  };
};

// Helper to filter out undefined values from whereClauses
function filterValidWhereClauses(whereClauses: { col: string; val: Term }[]) {
  return whereClauses.filter(({ val }) => val !== undefined);
}

// Helper: check if all elements of subset are in superset
function isSuperset(superset: Set<string>, subset: Iterable<string>) {
  for (const elem of subset) {
    if (!superset.has(elem)) return false;
  }
  return true;
}

// Helper: pick a subset of keys from an object
function pick<T extends object, K extends keyof T>(obj: T, keys: K[]): Pick<T, K> {
  const out = {} as Pick<T, K>;
  for (const k of keys) {
    if (k in obj) out[k] = obj[k];
  }
  return out;
}

// Helper: walk all keys of an object with a subst and return a new object
async function walkAllKeys<T extends Record<string, Term>>(
  obj: T,
  subst: Subst
): Promise<Record<string, Term>> {
  const result: Record<string, Term> = {};
  for (const key of Object.keys(obj)) {
    result[key] = await walk(obj[key], subst);
  }
  return result;
}

// Helper: unify all selectCols in a row with walkedQ and subst
async function unifyRowWithWalkedQ(
  selectCols: string[],
  walkedQ: Record<string, Term>,
  row: Record<string, any>,
  subst: Subst,
  myruncnt?: number,
): Promise<Subst | null> {
  let s2: Subst = new Map(subst);
  for (const col of selectCols) {
    if (!isVar(walkedQ[col])) {
      if (walkedQ[col] === row[col]) {
        continue;
      } else {
        log("NO UNIFY (grounded mismatch)", {
          myruncnt: myruncnt ?? 'N/A',
          col,
          left: walkedQ[col] as string | number | object,
          right: row[col],
        });
        return null;
      }
    } else {
      const unified = await unify(walkedQ[col], row[col], s2);
      if (unified) {
        log("UNIFY", {
          myruncnt: myruncnt ?? 'N/A',
          col,
          left: walkedQ[col] as string | number | object,
          right: row[col],
        });
        s2 = unified;
      } else {
        log("NO UNIFY", {
          myruncnt: myruncnt ?? 'N/A',
          col,
          left: walkedQ[col] as string | number | object,
          right: row[col],
        });
        return null;
      }
    }
  }
  // Use walkAllKeys to check if all selectCols are grounded after unification
  // const selectColsGrounded = await walkAllKeys(walkedQ, s2);
  // if (allParamsGrounded(selectColsGrounded)) {
  // Could be used for further optimizations or logging
  // log("ALL SELECT COLS GROUNDED", { myruncnt, selectColsGrounded });
  // }
  return s2;
}


// Helper: check if all query parameters are grounded (no variables)
function allParamsGrounded(params: Record<string, Term>): boolean {
  for (const key in params) {
    if (isVar(params[key])) return false;
  }
  return true;
}
