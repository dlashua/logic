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
                const v = await walk(q.params[col], subst);
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
                    const walked = await walk(origVal, s2);
                    const unified = await unify(walked, row[col], s2);
                    if (unified) {
                        s2 = unified;
                        ok = true;
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
            return async function* (s: Subst) {
                // If all values are grounded, do nothing
                let allGrounded = true;
                for (const col in queryObj) {
                    if (isVar(queryObj[col])) {
                        allGrounded = false;
                        break;
                    }
                }
                if (allGrounded) return;

                // Build where clause from grounded columns
                const where: Record<string, any> = {};
                const outputVars: string[] = [];
                for (const col in queryObj) {
                    const v = await walk(queryObj[col], s);
                    if (isVar(v)) {
                        outputVars.push(col);
                    } else {
                        where[col] = v;
                    }
                }
                // If there are no output variables, do nothing
                if (outputVars.length === 0) return;
                // Only support one output variable at a time for now
                if (outputVars.length > 1) {
                    throw new Error('Only one output variable supported per SQL relation call for now.');
                }
                const outCol = outputVars[0];
                // Run the SQL query for all possible values
                const rows = await db(table).select(outCol).where(where);
                for (const row of rows) {
                    const s2 = new Map(s);
                    s2.set(queryObj[outCol].id, row[outCol]);
                    yield s2;
                }
            };
        };
    };

    return { makeRel, run, db };
}

const taps = (msg: string) =>
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

const debugGoal = (label: string) => async function* (s: Subst) {
    console.log(`[DEBUG] ${label}:`, s);
    yield s;
};

// Stepwise debug: test only friends($.name, $.f_name) with eq($.name, "daniel")
const x = and(
    // eq($.name, "daniel"),
    // debugGoal('before friends'),
    friends($.name, $.f_name),
    // debugGoal('after friends'),
    // relDB.run,
    // debugGoal('after relDB.run'),
)

const m = new Map();
let outid = 0;
import { formatSubstitutions } from "./run.ts";

(async () => {
    let count = 0;
    for await (const out of formatSubstitutions(x(m), {
        name: $.name,
        // color: $.color,
        f_name: $.f_name,
        // f_color: $.f_color,
    }, 10)) {
        console.log("OUT", outid++, out);
        if (++count >= 5) break; // Hard stop after 5 results
    }
    console.log("DONE");
    await relDB.db.destroy(); // or whatever your knex instance is called
})();

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







