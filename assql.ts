import { find } from "lodash";
import { and, eq, lvar, makeFacts, createLogicVarProxy, mapInline, Subst, Term, walk, isVar, unify, run } from "./logic_lib.ts";
import knex, { Knex } from "knex";


const $ = createLogicVarProxy();

/**
 * makeSQLRelObj: Like makeSQLRel, but query is an object with keys as column names.
 * Smarter version: merges patterns with the same table and primary key variable.
 */
export const makeRelDB = async (knex_connect_options: Knex.Config) => {
    const db = knex(knex_connect_options);
    return (table: string, primaryKey: string = "id") => {
        type Pattern = {
            table: string;
            params: Record<string, any>;
        };
        const patterns: Pattern[] = [];
        const fn = function goal(queryObj: Record<string, any>) {
            return async function* (s: any) {
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
                // console.dir({ name: "PAT", patterns }, { depth: 100 });
                yield s;
            };
        };
        let qcnt = 0;
        fn.run = async function* (s: any) {
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
                    // No variables to select, just continue
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
            }
            yield* runPatterns(0, s);
        };
        return fn;
    };
}


const makeSQLRelObj = await makeRelDB({
    client: 'better-sqlite3',
    connection: {
        filename: './test.db'
    },
    useNullAsDefault: true
});
const T = await makeSQLRelObj("people");

const x = and(
    // eq($.name, "daniel"),
    T({ id: $.id, name: $.name }),
    T.run,
    T({ id: $.id, color: $.color }),
    T.run,
    T({ id: $.id, friend: $.f_id }),
    // runGoal(T),
    T({ id: $.f_id, name: $.f_name, color: $.f_color }),
    T.run,
    // eq($.id, 1),

    // T($.id, "address_id", $.a_id),
    // T($.a_id, "type", "address"),
    // T($.a_id, "city", $.city),
    // T($.a_id, "state", $.state),
)
// const w = T($.d_id, "device_name", $.d_name);
// console.log({ w })

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







