import { find } from "lodash";
import { and, eq, lvar, makeFacts, createLogicVarProxy, mapInline, Subst, Term, walk, isVar, unify } from "./logic_lib.ts";
import knex from "knex";


const $ = createLogicVarProxy();

export const makeSQLRel = (table) => {
    const patterns = [];
    const fn = function T(...query: Term[]) {
        const columns = ["e", "a", "v"];
        return async function* (s: Subst) {
            const params: Record<string, any> = {};
            const select: [string, number][] = [];
            const where: string[] = [];
            const varMap: Record<string, string> = {};
            query.forEach((q, i) => {
                const v = walk(q, s);
                if (isVar(v)) {
                    select.push([columns[i], v.id]);
                    varMap[columns[i]] = `var_${v.id}`;
                } else {
                    where.push(`${columns[i]} = ${typeof v === 'string' ? `'${v}'` : v}`);
                }
                params[columns[i]] = v;
            });
            patterns.push({ select, where, varMap, params, table });
            yield s;
        };
    };
    fn.sql = () => patterns;
    return fn;
}

/**
 * makeSQLRelObj: Like makeSQLRel, but query is an object with keys as column names.
 * Smarter version: merges patterns with the same table and primary key variable.
 */
export const makeRelDB = async (knex_connect_options) => {
    const db = knex(knex_connect_options);
    return (table: string, primaryKey: string = "id") => {
        type Pattern = {
            select: [string, number][];
            where: string[];
            varMap: Record<string, string>;
            params: Record<string, any>;
            table: string;
        };
        const patterns: Pattern[] = [];
        const fn = function T(queryObj: Record<string, Term>) {
            return async function* (s: Subst) {
                const params: Record<string, any> = {};
                const select: [string, number][] = [];
                const where: string[] = [];
                const varMap: Record<string, string> = {};
                for (const col of Object.keys(queryObj)) {
                    const v = walk(queryObj[col], s);
                    if (isVar(v)) {
                        select.push([col, v.id]);
                        varMap[col] = `var_${v.id}`;
                    } else {
                        where.push(`${col} = ${typeof v === 'string' ? `'${v}'` : v}`);
                    }
                    params[col] = v;
                }
                patterns.push({ select, where, varMap, params, table });
                // Don't yield yet; wait for all patterns to be collected
                yield s;
            };
        };
        fn.sql = () => {
            // Group by table and primary key var id (if present)
            const grouped: Record<string, Pattern> = {};
            for (const pat of patterns) {
                const pkVar = pat.varMap[primaryKey];
                const groupKey = pkVar ? `${pat.table}|${pkVar}` : `${pat.table}|${Math.random()}`;
                if (!grouped[groupKey]) {
                    grouped[groupKey] = {
                        select: [...pat.select],
                        where: [...pat.where],
                        varMap: { ...pat.varMap },
                        params: { ...pat.params },
                        table: pat.table
                    };
                } else {
                    // Merge selects
                    for (const sel of pat.select) {
                        if (!grouped[groupKey].select.some(([c, id]) => c === sel[0] && id === sel[1])) {
                            grouped[groupKey].select.push(sel);
                        }
                    }
                    // Merge where clauses
                    for (const w of pat.where) {
                        if (!grouped[groupKey].where.includes(w)) {
                            grouped[groupKey].where.push(w);
                        }
                    }
                    // Merge varMaps and params
                    Object.assign(grouped[groupKey].varMap, pat.varMap);
                    Object.assign(grouped[groupKey].params, pat.params);
                }
            }
            patterns.splice(0, patterns.length);
            return Object.values(grouped);
        };
        let qcnt = 0;
        // The generator that actually runs the SQL and yields substitutions
        fn.run = async function* (s: Subst) {
            // console.dir(patterns, { depth: 100 });
            const queries = fn.sql();
            while (queries.length > 0) {
                const q = queries.shift();
                const myqid = ++qcnt;
                // For this subst, reconstruct select/where from params and varMap
                let selectCols: string[] = [];
                let whereClauses: string[] = [];
                for (const col of Object.keys(q.params)) {
                    const origVal = q.params[col];
                    const val = isVar(origVal) ? walk(origVal, s) : origVal;
                    if (isVar(val)) {
                        selectCols.push(col);
                    } else {
                        whereClauses.push(`${col} = ${typeof val === 'string' ? `'${val}'` : val}`);
                    }
                }
                // Build knex query
                if (selectCols.length === 0) {
                    console.log("Q", myqid, "NONE", whereClauses);
                    continue;
                }
                let k = db(q.table).select(selectCols);
                for (const w of [...q.where, ...whereClauses]) {
                    const [col, val] = w.split(/\s*=\s*/);
                    k = k.where(col, '=', val.replace(/^'|'$/g, ''));
                }
                console.log("Q", myqid, k.toString());
                const rows = await k;
                for (const row of rows) {
                    // console.log("R", myqid, JSON.stringify(row));
                    let s2 = new Map(s);
                    let ok = false;
                    for (const col of selectCols) {
                        // Find the varId for this col
                        const origVal = q.params[col];
                        if (isVar(origVal)) {
                            const walked = walk(origVal, s2);
                            // console.log("before unify", { walked, origVal, s2, col, row })
                            const unified = unify(walked, row[col], s2);
                            if (unified) {
                                s2 = unified;
                                ok = true;
                            }
                        }
                    }
                    if (ok) {
                        yield s2;
                    } else {
                        console.log("NY", myqid, JSON.stringify(row));
                    }
                }
                // s = lastmap;
            }
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
    T({ id: $.id, color: $.color }),
    T({ id: $.id, friend: $.f_id }),
    runGoal(T),
    T({ id: $.f_id, name: $.f_name, color: $.f_color }),
    runGoal(T),

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
export function runGoal(T) {
    return async function* (s) {
        for await (const s2 of T.run(s)) {
            yield s2;
        }
    };
}




