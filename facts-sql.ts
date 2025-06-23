import { Subst, walk, isVar, unify, Term} from "./logic_lib.ts";
import knex, { Knex } from "knex";

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
const log = (msg: string, ...args: (Record<string, string | number | object>)[]) => {
  if (mutelog.includes(msg)) return;
  if(args.length <= 1) {
    console.dir({
      log: msg,
      ...args[0],
    }, {
      depth: 100,
    });
  } else {
    console.dir({
      log: msg,
      args,
    }, {
      depth: 100,
    });
  }
}

export const makeRelDB = async (knex_connect_options: Knex.Config, opts?: Record<string, string>) => {
  opts ??= {};
  opts.queryMode = "each";

  await Promise.resolve(null);
  const db = knex(knex_connect_options);
    interface Pattern extends Record<string, string | object> {
        table: string;
        params: Record<string, Term>;
        queries: string[],
    }
    const patterns: Pattern[] = [];
    const queries: string[] = [];
    const realQueries: string[] = [];
    const cacheQueries: string[] = [];
    // Record cache for fully grounded queries
    const recordCache = new Map<string, any>();

    const run = async function* (s: Subst, patterns: Pattern[], myruncnt: number) {
      async function* runPatterns(idx: number, subst: Subst): AsyncGenerator<Subst> {
        if (idx >= patterns.length) {
          yield subst;
          return;
        }
        const q = patterns[idx];
        log("WORKING PATTERN", {
          ...q,
          myruncnt,
        });
        const selectCols: string[] = [];
        const whereClauses: { col: string; val: Term }[] = [];
        const walkedQ: Record<string, Term> = {};
        for (const col in q.params) {
          walkedQ[col] = await walk(q.params[col], subst);
          if (isVar(walkedQ[col])) {
            selectCols.push(col);
          } else {
            whereClauses.push({
              col,
              val: walkedQ[col],
            });
          }
        }
        let rows;

        if (!selectCols.length) {
          selectCols.push(...Object.keys(walkedQ));
        }

        // Ensure selectCols includes all columns from whereClauses
        for (const { col } of whereClauses) {
          if (!selectCols.includes(col)) {
            selectCols.push(col);
          }
        }

        // Filter out undefined values in whereClauses to avoid undefined bindings
        const validWhereClauses = whereClauses.filter(({ val }) => val !== undefined);
        if (validWhereClauses.length !== whereClauses.length) {
          log("WARNING: Undefined value in whereClauses", {
            whereClauses,
            validWhereClauses, 
          });
        }

        const cacheKey = JSON.stringify({
          table: q.table,
          select: selectCols,
          where: validWhereClauses.map(({ col, val }) => [col,
            val]),
        });

        let k = db(q.table).select(selectCols);
        for (const { col, val } of validWhereClauses) {
          // @ts-expect-error knex types are weird
          k = k.where(col, '=', val);
        }
        const sqlStr = k.toString();

        if (recordCache.has(cacheKey)) {
          rows = recordCache.get(cacheKey);
          const sqlQuery = `CACHE HIT ${sqlStr} ${cacheKey}`;
          q.queries.push(sqlQuery);
          queries.push(sqlQuery);
          cacheQueries.push(sqlQuery);
        } else {
          // Subsumption: look for a broader or narrower cache entry
          let subsumed = false;
          for (const [otherKey,
            cachedRows] of recordCache.entries()) {
            const other = JSON.parse(otherKey);
            if (other.table !== q.table) continue;
            const selectColsArr: string[] = selectCols;
            const otherSelectArr: string[] = other.select as string[];
            const selectSet = new Set(selectColsArr);
            const otherSelectSet = new Set(otherSelectArr);
            // Check if cached select is a superset or subset of requested select
            const selectSuperset = [...selectSet].every((col: string) => otherSelectSet.has(col));
            const selectSubset = [...otherSelectSet].every((col: string) => selectSet.has(col));
            // Map where clauses for both queries
            const whereMap = new Map(whereClauses.map(({ col, val }) => [col,
              val]));
            const otherWhereArr: [string, any][] = other.where as [string, any][];
            const otherWhereMap = new Map(otherWhereArr);
            // Check if cached where is a subset or superset of requested where
            let whereSubset = true;
            for (const [col,
              val] of otherWhereArr) {
              if (!whereMap.has(col) || whereMap.get(col) !== val) {
                whereSubset = false;
                break;
              }
            }
            let whereSuperset = true;
            for (const { col, val } of whereClauses) {
              if (!otherWhereMap.has(col) || otherWhereMap.get(col) !== val) {
                whereSuperset = false;
                break;
              }
            }
            // Only use cache if cached select includes all columns needed for whereClauses
            const allWhereColsInCache = whereClauses.every(({ col }) => otherSelectSet.has(col));
            if (!allWhereColsInCache) continue;
            // Only use cache if cached select is a superset of requested select
            if (!selectSuperset) continue;
            // Only use cache if cached where is a subset of requested where
            let cachedWhereIsSubset = true;
            for (const [col,
              val] of otherWhereArr) {
              if (!whereMap.has(col) || whereMap.get(col) !== val) {
                cachedWhereIsSubset = false;
                break;
              }
            }
            if (!cachedWhereIsSubset) continue;
            // Use the cache
            rows = cachedRows.filter((row: any) =>
              whereClauses.every(({ col, val }) => row[col] === val),
            ).map((row: any) => {
              const filtered: any = {};
              for (const col of selectCols) filtered[col] = row[col];
              return filtered;
            });
            const sqlQuery = `CACHE SUBSUMED ${sqlStr} ${otherKey}`;
            q.queries.push(sqlQuery);
            queries.push(sqlQuery);
            cacheQueries.push(sqlQuery);
            subsumed = true;
            break;
          }
          if (!subsumed) {

            log("QUERY", {
              sql: sqlStr,
              myruncnt,
            });
            q.queries.push(sqlStr);
            queries.push(sqlStr);
            realQueries.push(sqlStr);
            rows = await k;
            // Cache the result (including empty array for no rows)
            recordCache.set(cacheKey, rows);
          }
        }
        
        // If no rows, skip yielding
        if (!rows || rows.length === 0) {
          return;
        }

        for (const row of rows) {
          log("ROW RETURNED", {
            row,
            myruncnt,
          });
          let s2: Subst = new Map(subst);
          let ok = false;
          for (const col of selectCols) {
            const origVal = q.params[col];
            if (isVar(origVal)) {
              const unified = await unify(walkedQ[col], row[col], s2);
              if (unified) {
                log("UNIFY", {
                  myruncnt,
                  col,
                  left: walkedQ[col] as string | number | object,
                  right: row[col],
                });
                s2 = unified;
                ok = true;
              } else {
                log("NO UNIFY", {
                  myruncnt,
                  col,
                  left: walkedQ[col] as string | number | object,
                  right: row[col],
                });
              }
            } else {
              break;
            }
          }
          if (ok) {
            yield* runPatterns(idx + 1, s2);
          }
        }
      }
      yield* runPatterns(0, s);
    };


    const makeRel = async (table: string, primaryKey = "id") => {
      await Promise.resolve(null);
      return function goal(queryObj: Record<string, Term>) {
        const myruncnt = runcnt++;
        log("STARTING GOAL", {
          myruncnt,
          table,
          queryObj,
        });

        const record_queries = async function (queryObj: Record<string, Term>, subst: Subst) {
          // Check if all values in queryObj are grounded (not variables)
          // let allGrounded = true;
          // for (const col in queryObj) {
          //     if (isVar(queryObj[col])) {
          //         allGrounded = false;
          //         break;
          //     }
          // }
          // if (allGrounded) {
          //     log("ALL TERMS GROUNDED");
          //     // All terms are grounded, do nothing and yield nothing
          //     return;
          // }
          // Find logic variable columns and their var ids
          const logicVars: Record<string, string> = {};
          for (const col in queryObj) {
            const v = queryObj[col];
            if (isVar(v)) logicVars[col] = v.id;
          }
          // Try to find a compatible pattern
          let merged = false;
          const walkedQ: Record<string, Term> = {};
          for (const col in queryObj) {
            walkedQ[col] = await walk(queryObj[col], subst);
          }
          log("AFTER WALK", {
            myruncnt,
            queryObj,
            walkedQ,
          });
          for (const pat of patterns) {
            if (pat.table !== table) continue;
            // Only check columns that exist in both pat and queryObj
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
            if (!compatible) {
              continue;
            }
            for (const col in walkedQ) {
              if (!(col in pat.params)) pat.params[col] = walkedQ[col];
            }
            merged = true;
            break;
          }
          if (!merged) {
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
          } else {
            log("MERGING PATTERN", {
              myruncnt,
              table,
              queryObj,
            });
          }
        };

        
        return async function* (s: Subst) {
          await record_queries(queryObj, s);
          if (!(
            opts.queryMode === "each"
            || (
              opts.queryMode === "last"
              && myruncnt !== runcnt -1
            )
            || (
              opts.queryMode === "first"
              && myruncnt !== 0
            )
          )) {
            log("YIELD THROUGH", {
              myruncnt,
              table,
              queryObj,
            });
            yield s;
            return;
          }

          log("RUNNING GOAL", {
            myruncnt,
            table,
            queryObj,
            patterns,
          });
                
          const thispatterns = [...patterns];
          patterns.splice(0, patterns.length);

          let found = false;
          for await (const s3 of run(s, thispatterns, myruncnt)) {
            found = true;
            log("YIELD", {
              myruncnt,
              s3,
            });
            yield s3;
          }
          if(!found) {
            log("NO YIELD", {
              myruncnt,
              table,
              queryObj,
              thispatterns,
              s,
            });  
            // yield s;
          } else {
            log("GOAL RUN FINISHED", {
              myruncnt,
              table,
              queryObj,
              thispatterns,
            });
          }
        }

      };
    };

    return {
      makeRel,
      db,
      run,
      queries, 
      realQueries,
      cacheQueries,
    };
}