import { and, eq, lvar, makeFacts, createLogicVarProxy, mapInline, Subst, Term, walk, isVar, unify, run, or, ifte, disj, Rel, fresh, runEasy } from "./logic_lib.ts";
import knex, { Knex } from "knex";
import { makeRelDB } from "./facts-sql.ts";

const $$ = createLogicVarProxy();

const taps = (msg) =>
    async function* (s: Subst) {
        console.log("TAP", msg, s);
        yield s;
    };


const relDB = await makeRelDB({
  client: 'better-sqlite3',
  connection: {
    filename: './test.db',
  },
  useNullAsDefault: true,
});
const P = await relDB.makeRel("people");
const F = await relDB.makeRel("friends");


const id_name = Rel((i, p) =>
  P({ id: i, name: p})
)

const person_color = Rel((p, c) =>
    P({ name: p, color: c })
)

const person_car = Rel((p, c) =>
    P({ name: p, car: c })
)

const friends =
    Rel((f1, f2) => {
        // fresh((f1_id, f2_id) =>
        const f1_id = lvar();
        const f2_id = lvar();
        return    and(
                P({ id: f1_id, name: f1 }),
                F({ f1: f1_id, f2: f2_id }),
                P({ id: f2_id, name: f2 }),
            )
        // ),
      }
    )

const favnum = makeFacts();
favnum.set("daniel", 3);


const results = runEasy(($) => [
  {
    name: $.name,
    color: $.color,
    car: $.car,
    // favnum: $.favnum,
    f_name: $.f_name,
    f_color: $.f_color,
  },
  and(
    person_color($.name, $.color),
    person_car($.name, $.car),
    // favnum($.name, $.favnum),
    friends($.name, $.f_name),
    person_color($.f_name, $.f_color),

    // F({ f1: $.p_id, f2: $.f_id}),
    // id_name($.p_id, $.name),
    // id_name($.f_id, $.f_name),
    // person_color($.name, $.color),
    // person_color($.f_name, $.f_color),
    // person_car($.name, $.car),

    // taps("FINAL"),
  )
]
)

let outid = 0;
for await (const out of results) {
    // Now run the DB queries for this substitution
    // for await (const dbSubst of T.run(subst)) {
    // dbSubst is a substitution unified with DB results
    console.log("OUT", outid++, out);
    // }
}

await relDB.db.destroy();

console.log(relDB.queries);





