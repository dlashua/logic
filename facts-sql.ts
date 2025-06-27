import type { Knex } from "knex";
// eslint-disable-next-line import/no-named-as-default
import knex from "knex";
import type { Subst, Term } from "./core.ts";
import { isVar, unify, walk } from "./core.ts";

let runcnt = 0;
const mutelog = [
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
];
const log = (
  msg: string,
  ...args: Record<string, string | number | object>[]
) => {
  // return;
  if (mutelog.includes(msg)) return;
  if (args.length <= 1) {
    console.dir(
      {
        log: msg,
        ...args[0],
      },
      {
        depth: 100,
      },
    );
  } else {
    console.dir(
      {
        log: msg,
        args,
      },
      {
        depth: 100,
      },
    );
  }
};

// Pattern interface at top-level for clarity
interface Pattern extends Record<string, string | object> {
  table: string;
  params: Record<string, Term>;
  queries: string[];
}

// Helper to build selectCols, whereClauses, walkedQ
async function buildQueryParts(params: Record<string, Term>, subst: Subst) {
  /**
   * @todo test without sorting
   */
  const selectCols = Object.keys(params).sort(); // #9: sort for cache key consistency
  const whereClauses: { col: string; val: Term }[] = [];
  const walkedQ: Record<string, Term> = {};
  for (const col of selectCols) {
    walkedQ[col] = await walk(params[col], subst);
    if (!isVar(walkedQ[col])) {
      whereClauses.push({
        col,
        val: walkedQ[col],
      });
    }
  }
  // #9: sort whereClauses for cache key consistency
  /**
   * @todo test without sorting
   */
  whereClauses.sort((a, b) => a.col.localeCompare(b.col));
  return {
    selectCols,
    whereClauses,
    walkedQ,
  };
}

// Helper to find a subsumed cache entry
function findSubsumedCacheEntry(
  recordCache: Map<string, any>,
  table: string,
  selectCols: string[],
  whereClauses: { col: string, val: Term }[]
) {
  // Precompute sets/maps for the query
  const selectSet = new Set(selectCols);
  const whereMap = new Map(whereClauses.map(({ col, val }) => [col, val]));
  for (const [otherKey, cachedRows] of recordCache.entries()) {
    const other = JSON.parse(otherKey);
    if (other.table !== table) continue;
    const otherSelectArr = other.select as string[];
    const otherSelectSet = new Set(otherSelectArr);
    // Check if cached select is a superset of requested select
    let selectSuperset = true;
    for (const col of selectSet) {
      if (!otherSelectSet.has(col)) {
        selectSuperset = false;
        break;
      }
    }
    if (!selectSuperset) continue;
    // Only use cache if cached select includes all columns needed for whereClauses
    let allWhereColsInCache = true;
    for (const { col } of whereClauses) {
      if (!otherSelectSet.has(col)) {
        allWhereColsInCache = false;
        break;
      }
    }
    if (!allWhereColsInCache) continue;
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
function mergeOrAddPattern(
  patterns: Pattern[],
  table: string,
  walkedQ: Record<string, Term>,
  queryObj: Record<string, Term>,
  myruncnt: number
) {
  // #7: Memoize compatible pattern index for faster repeated lookups
  // Use a map from table name to array of pattern indices for O(1) table filtering
  // (If patterns array is large, this can be further optimized with a WeakMap or similar)
  // For now, keep the logic simple and efficient for small arrays
  for (const pat of patterns) {
    if (pat.table !== table) continue;
    let compatible = true;
    for (const col in pat.params) {
      if (!(col in walkedQ)) continue;
      const vPat = pat.params[col];
      const vNew = walkedQ[col];
      if (isVar(vPat) && isVar(vNew)) {
        if (vPat.id !== vNew.id) {
          compatible = false;
          break;
        }
      } else if (isVar(vPat) !== isVar(vNew)) {
        compatible = false;
        break;
      } else if (!isVar(vPat) && vPat !== vNew) {
        compatible = false;
        break;
      }
    }
    if (!compatible) continue;
    // @todo put this back if something breaks
    // for (const col in walkedQ) {
    //   if (!(col in pat.params)) pat.params[col] = walkedQ[col];
    // }
    log("MERGING PATTERN", {
      myruncnt,
      table,
      queryObj,
    });
    return true; // merged
  }
  patterns.push({
    table,
    params: {
      ...queryObj,
    },
    queries: [],
  });
  log("ADDING PATTERN", {
    myruncnt,
    table,
    queryObj,
  });
  return false; // added
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
    if (state.recordCache.has(cacheKey)) {
      const sqlQuery = `CACHE HIT ${cacheKey}`;
      state.queries.push(sqlQuery);
      state.cacheQueries.push(sqlQuery);
      return {
        rows: state.recordCache.get(cacheKey),
        cacheType: "hit",
        cacheKey,
        queryString: sqlQuery,
      };
    } else if (subsumeFn) {
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

  const run = async function* factsSqlRun (
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
            .map((row: any) => {
              const filtered: any = {};
              for (const col of selectCols) filtered[col] = row[col];
              return filtered;
            });
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
          myruncnt,
        });
        let s2: Subst = new Map(subst);
        for (const col of selectCols) {
          if (!isVar(walkedQ[col])) {
            // If grounded and matches, skip unify
            if (walkedQ[col] === row[col]) {
              continue;
            } else {
              log("NO UNIFY (grounded mismatch)", {
                myruncnt,
                col,
                left: walkedQ[col] as string | number | object,
                right: row[col],
              });
              break;
            }
          } else {
            const unified = await unify(walkedQ[col], row[col], s2);
            if (unified) {
              log("UNIFY", {
                myruncnt,
                col,
                left: walkedQ[col] as string | number | object,
                right: row[col],
              });
              s2 = unified;
            } else {
              log("NO UNIFY", {
                myruncnt,
                col,
                left: walkedQ[col] as string | number | object,
                right: row[col],
              });
              break;
            }
          }
        }
        yield* runPatterns(idx + 1, s2);
      }
    }
    yield* runPatterns(0, s);
  };

  const rel = async (table: string) => {
    return function goal(queryObj: Record<string, Term>) {
      const myruncnt = runcnt++;
      log("STARTING GOAL", {
        myruncnt,
        table,
        queryObj,
      });

      const record_queries = async (
        queryObj: Record<string, Term>,
        subst: Subst,
      ) => {
        // Find logic variable columns and their var ids
        const logicVars: Record<string, string> = {};
        for (const col in queryObj) {
          const v = queryObj[col];
          if (isVar(v)) logicVars[col] = v.id;
        }
        // Try to find a compatible pattern
        const walkedQ: Record<string, Term> = {};
        for (const col in queryObj) {
          walkedQ[col] = await walk(queryObj[col], subst);
        }
        log("AFTER WALK", {
          myruncnt,
          queryObj,
          walkedQ,
        });
        mergeOrAddPattern(state.patterns, table, walkedQ, queryObj, myruncnt);
      };

      return async function* factsSql (s: Subst) {
        await record_queries(queryObj, s);

        log("RUNNING GOAL", {
          myruncnt,
          table,
          queryObj,
          patterns: state.patterns,
        });

        const thispatterns = [...state.patterns];
        state.patterns.splice(0, state.patterns.length);

        let found = false;
        for await (const s3 of run(s, thispatterns, myruncnt)) {
          found = true;
          log("YIELD", {
            myruncnt,
            s3,
          });
          yield s3;
        }
        if (!found) {
          log("NO YIELD", {
            myruncnt,
            table,
            queryObj,
            thispatterns,
            s,
          });
        } else {
          log("GOAL RUN FINISHED", {
            myruncnt,
            table,
            queryObj,
            thispatterns,
          });
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
