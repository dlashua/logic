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
    const run = async function* (s: Subst, patterns: Pattern[], myruncnt) {
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
          // log("W", col, v);
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
          // log("SKIPPING QUERY", {
          //   q,
          //   walkedQ,
          //   myruncnt,
          // });
          // rows = [walkedQ];
          selectCols.push(...Object.keys(walkedQ));
          // yield* runPatterns(idx + 1, subst);
          // return;
        } 
        let k = db(q.table).select(selectCols);
        for (const { col, val } of whereClauses) {
          // @ts-expect-error knex types are weird
          k = k.where(col, '=', val);
        }
  
        const sqlQuery = k.toString();
        log("QUERY", {
          sql: sqlQuery,
          myruncnt,
        });
        q.queries.push(sqlQuery);
        queries.push(sqlQuery);
        rows = await k;

        
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
              log("UNIFY", {
                myruncnt,
                col,
                left: walkedQ[col],
                right: row[col],
              });
              const unified = await unify(walkedQ[col], row[col], s2);
              if (unified) {
                s2 = unified;
                ok = true;
              }
            } else {
              break;
            }
          }
          if (ok) {
            yield* runPatterns(idx + 1, s2);
          }
        }
        // patterns.splice(idx, 1);
      }
      yield* runPatterns(0, s);
      // process.exit();
      // patterns.splice(0, patterns.length);
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

        const record_queries = async function (queryObj: Record<string, Term>, subst) {
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
          if (
            opts.queryMode != "each"
            && myruncnt > 1000) {
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
    };
}