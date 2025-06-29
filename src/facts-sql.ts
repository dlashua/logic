import type { Knex } from "knex";
 
import knex from "knex";
import {
  EOSseen,
  EOSsent,
  Subst,
  Term,
  isVar,
  unify,
  walk
} from "./core.ts";

// --- Logging configuration ---
const LOG_ENABLED = true; // Set to true to enable logging globally
const LOG_IDS = new Set<string>([]); // No log filtering

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

// Helper: check if all query parameters are grounded (no variables)
function allParamsGrounded(params: Record<string, Term>): boolean {
  for (const key in params) {
    if (isVar(params[key])) return false;
  }
  return true;
}

// Helper: unify all selectCols in a row with walkedQ and subst
async function unifyRowWithWalkedQ(
  selectCols: string[],
  walkedQ: Record<string, Term>,
  row: Record<string, any>,
  subst: Subst,
): Promise<Subst | null> {
  let s2: Subst = new Map(subst);
  for (const col of selectCols) {
    if (!isVar(walkedQ[col])) {
      if (walkedQ[col] === row[col]) {
        continue;
      } else {
        return null;
      }
    } else {
      const unified = await unify(walkedQ[col], row[col], s2);
      if (unified) {
        s2 = unified;
      } else {
        return null;
      }
    }
  }
  return s2;
}

export const makeRelDB = async (
  knex_connect_options: Knex.Config,
  opts?: Record<string, string>,
) => {
  opts ??= {};
  const db = knex(knex_connect_options);
  // Simple cache for queries
  const recordCache = new Map<string, any>();

  // Query logging arrays
  const queries: string[] = [];
  const realQueries: string[] = [];
  const cacheQueries: string[] = [];

  // Helper to build selectCols, whereClauses, walkedQ
  async function buildQueryParts(params: Record<string, Term>, subst: Subst) {
    const selectCols = Object.keys(params).sort();
    const walkedQ: Record<string, Term> = {};
    const whereClauses: { col: string; val: Term }[] = [];
    for (const col of selectCols) {
      walkedQ[col] = await walk(params[col], subst);
      if (!isVar(walkedQ[col])) {
        whereClauses.push({
          col,
          val: walkedQ[col] 
        });
      }
    }
    return {
      selectCols,
      whereClauses,
      walkedQ 
    };
  }

  // Helper: normalize a query key for cache
  function makeCacheKey(table: string, selectCols: string[], whereClauses: { col: string; val: Term }[]) {
    return JSON.stringify({
      table,
      select: [...selectCols].sort(),
      where: [...whereClauses].sort((a, b) => a.col.localeCompare(b.col)),
    });
  }

  // Helper: build a row cache key for fully grounded queries
  function makeRowCacheKey(table: string, params: Record<string, Term>) {
    const key = Object.keys(params).sort().map(k => `${k}:${params[k]}`).join("|");
    return `${table}|${key}`;
  }
  // Row cache for fully grounded queries
  const rowCache = new Map<string, any>();

  // Main relation generator: exact query and row cache, with logging
  const rel = async (table: string) => {
    let nextGoalId = 1;
    const patterns = [];
    return function goal(queryObj: Record<string, Term>) {
      const goalId = nextGoalId++;
      console.log("MADE GOAL", goalId, queryObj);
      gatherAndMerge(queryObj);

      async function* run(s: Subst, queryObj: Record<string, Term>) {
        const { selectCols, whereClauses, walkedQ } = await buildQueryParts(queryObj, s);
        const cacheKey = makeCacheKey(table, selectCols, whereClauses);
        const rowKey = allParamsGrounded(walkedQ) ? makeRowCacheKey(table, walkedQ) : null;
        let rows;
        let cacheType = null;
        // If all params are grounded, try row cache first
        if (rowKey) {
          if (rowCache.has(rowKey)) {
            rows = [rowCache.get(rowKey)];
            cacheType = 'row';
          }
        }
        // Otherwise, try exact query cache
        if (!rows && recordCache.has(cacheKey)) {
          rows = recordCache.get(cacheKey);
          cacheType = 'query';
        }
        // Otherwise, hit the DB
        if (!rows) {
          let k = db(table).select(selectCols);
          for (const { col, val } of whereClauses) {
            k = k.where(col, val as any);
          }
          rows = await k;
          recordCache.set(cacheKey, rows);
          // If all params grounded and single row, cache in rowCache
          if (allParamsGrounded(walkedQ) && rows.length === 1) {
            const rowKey = makeRowCacheKey(table, walkedQ);
            rowCache.set(rowKey, rows[0]);
          }
          // Log real query
          const sql = k.toString();
          queries.push(sql);
          realQueries.push(sql);
        } else {
          // Log cache hit
          let desc = '';
          if (cacheType === 'row') {
            desc = `[ROW CACHE] ${table} ${JSON.stringify(walkedQ)}`;
          } else if (cacheType === 'query') {
            desc = `[QUERY CACHE] ${table} select=${JSON.stringify(selectCols)} where=${JSON.stringify(whereClauses)}`;
          } else {
            desc = `[CACHE] ${table}`;
          }
          queries.push(desc);
          cacheQueries.push(desc);
        }
        if (!rows || rows.length === 0) {
          return;
        }
        for (const row of rows) {
          const unifiedSubst = await unifyRowWithWalkedQ(selectCols, walkedQ, row, s);
          if (unifiedSubst) {
            yield unifiedSubst;
          }
        }
      };
      
      function gatherAndMerge(queryObj: Record<string, Term>) {
        const matches = patterns.filter(x => {
          
        })
        
        patterns.push({
          table,
          goalIds: [goalId],
          queryObj,
          rows: [],
          ran: false,
        });
      };

      // return async function* factsSql(s: Subst) {
      //   console.log("IN GOAL", goalId, queryObj);
      //   if (s === null) {
      //     if(EOS) {
      //       console.log("null after EOS!");
      //     }

      //     EOSseen(`facts-sql rel ${goalId}`);
      //     EOS = true;          

      //     // const thisGoalPatterns = patterns.filter(x => x.goalId === goalId);
      //     // console.log("thisGoalPatterns", thisGoalPatterns);
      //     // if(thisGoalPatterns.length === 0) {
      //     //   console.log("allPatterns", patterns);
      //     // }
      //     console.log("PATTERNS", patterns);
      //     let s2 = patterns.find(x => x.goalId === 1)?.s;
      //     if(!s2) return;
      //     while (patterns.length > 0) {
      //       const pattern = patterns.shift();
      //       for await (s2 of run(s2, pattern.queryObj)) {
      //         console.log("YIELD", s2);
      //         yield s2;
      //       }
      //     }
          
      //     EOSsent(`facts-sql rel ${goalId}`);
      //     yield null;
      //     return;
      //   }
      //   if(EOS) {
      //     console.log(`record after EOS ${goalId}`);
      //   }

      //   gather(queryObj, s);
      //   yield s;
      // }

      return async function* factsSql(s: Subst) {
        console.log("IN GOAL", goalId, queryObj);
        if (s === null) {
          EOSseen(`facts-sql rel ${goalId}`);
          yield null;
          return
        }

        if(patterns.length === 0) {
          console.log("NO PATTERNS");
          // yield s;
          return;
        }

        const thisPatterns = patterns.filter(x => x.goalIds.includes(goalId));

        for (const pattern of thisPatterns) {
          const pidx = patterns.indexOf(pattern);
          patterns.splice(pidx, 1)
          let s2 = s;
          for await (s2 of run(s2, pattern.queryObj)) {
            yield s2;
          }
        }
        return;
      }
   
    };
  };

  // --- Symmetric SQL relation (unchanged, but simplified) ---
  const relSym = async (table: string, keys: [string, string]) => {
    return function goal(queryObj: Record<string, Term<string | number>>) {
      const values = Object.values(queryObj);
      return async function* (s: Subst) {
        if (s === null) {
          EOSseen("facts-sql relSym");
          yield null;
          return;
        }
        if (values.length > 2) return;
        const walkedValues: Term[] = await Promise.all(values.map(x => walk(x, s)));
        if(walkedValues[0] === walkedValues[1]) return;
        const gv = walkedValues.filter(x => !isVar(x)) as (string | number)[];
        let rows;
        if (gv.length === 2) {
          const k = db(table).select(keys)
            .where(keys[0], gv[0])
            .andWhere(keys[1], gv[1]);
          rows = await k;
        } else {
          const k = db(table).select(keys);
          rows = await k;
        }
        for (const row of rows) {
          if (gv.length === 2) {
            if (keys.every(k => gv.includes(row[k]))) {
              yield s;
            }
            continue;
          }
          const s2 = new Map(s);
          const unified1 = await unify(walkedValues[0], row[keys[0]], s2);
          if (unified1) {
            const unified2 = await unify(walkedValues[1], row[keys[1]], unified1);
            if (unified2) {
              yield unified2;
              continue;
            }
          }
          const s3 = new Map(s);
          const unified3 = await unify(walkedValues[1], row[keys[0]], s3);
          if (unified3) {
            const unified4 = await unify(walkedValues[0], row[keys[1]], unified3);
            if (unified4) {
              yield unified4;
            }
          }
        }
      };
    };
  };

  return {
    rel,
    relSym,
    db,
    queries,
    realQueries,
    cacheQueries,
  };
};
