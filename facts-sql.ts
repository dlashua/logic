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
  const selectCols: string[] = [];
  const whereClauses: { col: string; val: Term }[] = [];
  const walkedQ: Record<string, Term> = {};
  for (const col in params) {
    walkedQ[col] = await walk(params[col], subst);
    if (isVar(walkedQ[col])) {
      selectCols.push(col);
    } else {
      whereClauses.push({
        col,
        val: walkedQ[col] 
      });
    }
  }
  // Ensure selectCols includes all columns from whereClauses
  for (const { col } of whereClauses) {
    if (!selectCols.includes(col)) {
      selectCols.push(col);
    }
  }
  return {
    selectCols,
    whereClauses,
    walkedQ 
  };
}

// Helper to find a subsumed cache entry
function findSubsumedCacheEntry(
  recordCache: Map<string, any>,
  table: string,
  selectCols: string[],
  whereClauses: { col: string, val: Term }[]
) {
  for (const [otherKey, cachedRows] of recordCache.entries()) {
    const other = JSON.parse(otherKey);
    if (other.table !== table) continue;
    const selectSet = new Set(selectCols);
    const otherSelectSet = new Set(other.select as string[]);
    // Check if cached select is a superset of requested select
    const selectSuperset = [...selectSet].every((col: string) =>
      otherSelectSet.has(col)
    );
    // Map where clauses for both queries
    const whereMap = new Map(whereClauses.map(({ col, val }) => [col, val]));
    const otherWhereArr: [string, any][] = other.where as [string, any][];
    const otherWhereMap = new Map(otherWhereArr);
    // Only use cache if cached select includes all columns needed for whereClauses
    const allWhereColsInCache = whereClauses.every(({ col }) =>
      otherSelectSet.has(col)
    );
    if (!allWhereColsInCache) continue;
    if (!selectSuperset) continue;
    // Only use cache if cached where is a subset of requested where
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
      cachedRows 
    };
  }
  return null;
}

// Helper to merge or add a pattern
function mergeOrAddPattern(patterns: Pattern[], table: string, walkedQ: Record<string, Term>, queryObj: Record<string, Term>, myruncnt: number) {
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
    for (const col in walkedQ) {
      if (!(col in pat.params)) pat.params[col] = walkedQ[col];
    }
    log("MERGING PATTERN", {
      myruncnt,
      table,
      queryObj 
    });
    return true; // merged
  }
  patterns.push({
    table,
    params: {
      ...queryObj 
    },
    queries: []
  });
  log("ADDING PATTERN", {
    myruncnt,
    table,
    queryObj 
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

  const run = async function* factsSqlRun (
    s: Subst,
    patterns: Pattern[],
    myruncnt: number,
  ) {
    async function* runPatterns (
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
      let rows;

      // Filter out undefined values in whereClauses to avoid undefined bindings
      const validWhereClauses = whereClauses.filter(
        ({ val }) => val !== undefined,
      );
      if (validWhereClauses.length !== whereClauses.length) {
        log("WARNING: Undefined value in whereClauses", {
          whereClauses,
          validWhereClauses,
        });
      }

      const cacheKey = JSON.stringify({
        table: q.table,
        select: selectCols,
        where: validWhereClauses.map(({ col, val }) => [col, val]),
      });

      let k = db(q.table).select(selectCols);
      for (const { col, val } of validWhereClauses) {
        // @ts-expect-error knex types are weird
        k = k.where(col, "=", val);
      }
      const sqlStr = k.toString();

      if (state.recordCache.has(cacheKey)) {
        rows = state.recordCache.get(cacheKey);
        const sqlQuery = `CACHE HIT ${sqlStr} ${cacheKey}`;
        q.queries.push(sqlQuery);
        state.queries.push(sqlQuery);
        state.cacheQueries.push(sqlQuery);
      } else {
        // Use helper for cache subsumption
        const subsumed = findSubsumedCacheEntry(state.recordCache, q.table, selectCols, validWhereClauses);
        if (subsumed) {
          rows = subsumed.cachedRows
            .filter((row: any) =>
              validWhereClauses.every(({ col, val }) => row[col] === val),
            )
            .map((row: any) => {
              const filtered: any = {};
              for (const col of selectCols) filtered[col] = row[col];
              return filtered;
            });
          const sqlQuery = `CACHE SUBSUMED ${sqlStr} ${subsumed.otherKey}`;
          q.queries.push(sqlQuery);
          state.queries.push(sqlQuery);
          state.cacheQueries.push(sqlQuery);
        } else {
          log("QUERY", {
            sql: sqlStr,
            myruncnt,
          });
          q.queries.push(sqlStr);
          state.queries.push(sqlStr);
          state.realQueries.push(sqlStr);
          rows = await k;
          // Cache the result (including empty array for no rows)
          state.recordCache.set(cacheKey, rows);
        }
      }

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
        let ok = true;
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
              ok = false;
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
              ok = false;
              break;
            }
          }
        }
        if (ok) {
          yield* runPatterns(idx + 1, s2);
        }
      }
    }
    yield* runPatterns(0, s);
  };

  const rel = async (table: string, primaryKey = "id") => {
    await Promise.resolve(null);
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

  return {
    rel,
    db,
    run,
    queries: state.queries,
    realQueries: state.realQueries,
    cacheQueries: state.cacheQueries,
  };
};
