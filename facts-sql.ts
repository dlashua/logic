import { and, eq, lvar, makeFacts, createLogicVarProxy, mapInline, Subst, Term, walk, isVar, unify, run, or, ifte, disj, Rel, fresh } from "./logic_lib.ts";
import knex, { Knex } from "knex";

let runcnt = 0;
const mutelog = [
    // "PATTERNS BEFORE CHECK",
    "WORKING PATTERN",
    // "TABLE NOT IN PATTERNS. NEED RUN.",
    "ADDING PATTERN",
    "MERGING PATTERN",
    // "TABLE IN PATTERNS. NO RUN.",
    "SKIPPING QUERY",
    "ROW RETURNED",
    "RUNNING GOAL",
    "GOAL RUN FINISHED",
    "QUERY",
    "STARTING GOAL",
    "YIELD THROUGH",
];
const log = (msg, ...args) => {
    if (mutelog.includes(msg)) return;
    if(args.length <= 1) {
        console.dir({
            log: msg,
            ...args[0],
        }, {depth: 100});
    } else {
        console.dir({
            log: msg,
            ...args,
        }, {depth: 100});
    }
}

export const makeRelDB = async (knex_connect_options: Knex.Config) => {
    const db = knex(knex_connect_options);
    type Pattern = {
        table: string;
        params: Record<string, any>;
        queries: [],
    };
    const patterns: Pattern[] = [];
    const queries = [];
    const run = async function* (s: any, patterns) {
        async function* runPatterns(idx: number, subst: Subst): AsyncGenerator<Subst> {
            if (idx >= patterns.length) {
                yield subst;
                return;
            }
            const q = patterns[idx];
            log("WORKING PATTERN", q);
            const selectCols: string[] = [];
            const whereClauses: { col: string; val: any }[] = [];
            const walkedQ = {};
            for (const col in q.params) {
                walkedQ[col] = await walk(q.params[col], subst);
                // log("W", col, v);
                if (isVar(walkedQ[col])) {
                    selectCols.push(col);
                } else {
                    whereClauses.push({ col, val: walkedQ[col] });
                }
            }
            if (!selectCols.length) {
                log("SKIPPING QUERY", {q, walkedQ});
                yield* runPatterns(idx + 1, subst);
                return;
            }
            let k = db(q.table).select(selectCols);
            for (const { col, val } of whereClauses) {
                k = k.where(col, '=', val);
            }
            log("QUERY", {sql: k.toString()});
            q.queries.push(k.toString());
            queries.push(k.toString());
            const rows = await k;
            for (const row of rows) {
                log("ROW RETURNED", {row});
                let s2: Subst = new Map(subst as Subst);
                let ok = false;
                for (const col of selectCols) {
                    const origVal = q.params[col];
                    if (isVar(origVal)) {
                        const unified = await unify(walkedQ[col], row[col], s2);
                        if (unified) {
                            s2 = unified;
                            ok = true;
                        }
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


    const makeRel = async (table: string, primaryKey: string = "id") => {
        return function goal(queryObj: Record<string, any>) {
            const myruncnt = runcnt++;
            log("STARTING GOAL", {myruncnt, table, queryObj});

            const record_queries = function (queryObj) {
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
                for (const pat of patterns) {
                    if (pat.table !== table) continue;
                    // Only check columns that exist in both pat and queryObj
                    let compatible = true;
                    for (const col in pat.params) {
                        if (!(col in queryObj)) continue;
                        const vPat = pat.params[col];
                        const vNew = queryObj[col];
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
                    for (const col in queryObj) {
                        if (!(col in pat.params)) pat.params[col] = queryObj[col];
                    }
                    merged = true;
                    break;
                }
                if (!merged) {
                    patterns.push({ table, params: { ...queryObj }, queries: [] });
                    log("ADDING PATTERN", {myruncnt, table, queryObj});
                } else {
                    log("MERGING PATTERN", {myruncnt, table, queryObj});
                }
            };

            record_queries(queryObj);

            return async function* (s: any) {
                if (myruncnt !== runcnt - 1) {
                    log("YIELD THROUGH", {myruncnt, table, queryObj});
                    if(s) yield s;
                    return;
                }

                log("RUNNING GOAL", {myruncnt, table, queryObj, patterns});
                
                // const thispatterns = [...patterns];
                // patterns.splice(0, patterns.length);

                for await (const s3 of run(s, patterns)) {
                    if(s3) yield s3;
                }
                log("GOAL RUN FINISHED", {myruncnt, table, queryObj, patterns});
            }

        };
    };

    return { makeRel, db, run, queries };
}