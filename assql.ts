import { find } from "lodash";
import { and, eq, lvar, makeFacts, createLogicVarProxy, mapInline, Subst, Term, walk, isVar, unify, run, or, ifte, disj, Rel, fresh } from "./logic_lib.ts";
import knex, { Knex } from "knex";


const $ = createLogicVarProxy();

/**
 * makeSQLRelObj: Like makeSQLRel, but query is an object with keys as column names.
 * Smarter version: merges patterns with the same table and primary key variable.
 */
let runcnt = 0;

export const makeRelDB = async (knex_connect_options: Knex.Config) => {
    const db = knex(knex_connect_options);
    type Pattern = {
        table: string;
        params: Record<string, any>;
    };
    const patterns: Pattern[] = [];
    const run = async function* (s: any) {
        async function* runPatterns(idx: number, subst: Subst): AsyncGenerator<Subst> {
            if (idx >= patterns.length) {
                yield subst;
                return;
            }
            const q = patterns[idx];
            const selectCols: string[] = [];
            const whereClauses: { col: string; val: any }[] = [];
            for (const col in q.params) {
                const v = walk(q.params[col], subst);
                if (isVar(v)) {
                    selectCols.push(col);
                } else {
                    whereClauses.push({ col, val: v });
                }
            }
            if (!selectCols.length) {
                // console.log("Q", "NONE", q);
                yield* runPatterns(idx + 1, subst);
                return;
            }
            let k = db(q.table).select(selectCols);
            for (const { col, val } of whereClauses) {
                k = k.where(col, '=', val);
            }
            console.log("Q", k.toString());
            const rows = await k;
            for (const row of rows) {
                console.log("R", JSON.stringify(row));
                let s2: Subst = new Map(subst as Subst);
                let ok = false;
                for (const col of selectCols) {
                    const origVal = q.params[col];
                    if (isVar(origVal)) {
                        const walked = walk(origVal, s2);
                        const unified = unify(walked, row[col], s2);
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
        // patterns.splice(0, patterns.length);
    };
    const makeRel = async (table: string, primaryKey: string = "id") => {
        return function goal(queryObj: Record<string, any>) {
            const record_queries = async function* (s: any) {
                // Check if all values in queryObj are grounded (not variables)
                // let allGrounded = true;
                // for (const col in queryObj) {
                //     if (isVar(queryObj[col])) {
                //         allGrounded = false;
                //         break;
                //     }
                // }
                // if (allGrounded) {
                //     console.log("ALL TERMS GROUNDED");
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
                if (!merged) patterns.push({ table, params: { ...queryObj } });
                // console.dir({ name: "PAT", patterns, s: JSON.stringify(s.entries()) }, { depth: 100 });
                yield s;
            };
            if (patterns.map(x => x.table).includes(table)) {
                return record_queries;
            }
            return async function* (s: any) {
                const myruncnt = runcnt++;
                // console.log("RUNNING", myruncnt, s);
                for await (const new_s of run(s)) {
                    if (new_s) {
                        // console.log("RUN YIELD", myruncnt, new_s);
                        yield* record_queries(new_s);
                    }
                }
                // console.log("DONE RUNNING", myruncnt);
            }
        };
    };

    return { makeRel, run };
}

const taps = (msg) =>
    async function* (s: Subst) {
        console.log("TAP", msg, s);
        yield s;
    };


const relDB = await makeRelDB({
    client: 'better-sqlite3',
    connection: {
        filename: './test.db'
    },
    useNullAsDefault: true
});
const P = await relDB.makeRel("people");
const F = await relDB.makeRel("friends");

const person_color = Rel((p, c) =>
    P({ name: p, color: c })
)

const friends =
    Rel((f1, f2) =>
        fresh((f1_id, f2_id) =>
            and(
                P({ id: f1_id, name: f1 }),
                F({ f1: f1_id, f2: f2_id }),
                P({ id: f2_id, name: f2 }),
            )
        ),
    )

const x = and(
    person_color($.name, $.color),
    friends($.name, $.f_name),
    person_color($.f_name, $.f_color),
    relDB.run,
)

const m = new Map();
let outid = 0;
for await (const subst of x(m)) {
    // Now run the DB queries for this substitution
    // for await (const dbSubst of T.run(subst)) {
    // dbSubst is a substitution unified with DB results
    console.log("OUT", outid++, subst);
    // }
}

/**
 * runGoal: logic goal that runs T.run for the current substitution and yields all resulting substitutions.
 * Usage: and(T({...}), runGoal(T), ...)
 */
export function runGoal(T: { run: (s: Subst) => AsyncGenerator<Subst> }) {
    return async function* (s: Subst) {
        for await (const s2 of T.run(s)) {
            yield s2;
        }
    };
}







