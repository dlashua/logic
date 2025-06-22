import {
  Rel,
  Subst,
  and,
  collecto,
  createLogicVarProxy,  
  fresh,  
  makeFacts,  
  runEasy, 
} from "./logic_lib.ts";
import { makeRelDB } from "./facts-sql.ts";


const $$ = createLogicVarProxy();


const taps = (msg: string) =>
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

const person_color = Rel((p, c) =>
  P({
    name: p,
    color: c, 
  }),
)

const friends =
    Rel((f1, f2) =>
      fresh((f1_id, f2_id) =>
        and(
          P({
            id: f1_id,
            name: f1, 
          }),
          F({
            f1: f1_id,
            f2: f2_id, 
          }),
          P({
            id: f2_id,
            name: f2, 
          }),
        ),
      ),
    )

const favnum = makeFacts();
favnum.set("aubrey", 1);
favnum.set("daniel", 2);
favnum.set("jen", 3);
favnum.set("corey", 4);

const debugGoal = (label: string) => async function* (s: Subst) {
  console.log(`[DEBUG] ${label}:`, s);
  yield s;
};

await runEasy(($) => [
  {
    name: $.name,
    color: $.color,
    favnum: $.favnum,
    // f_name: $.f_name,
    f_names: $.f_names,
    // f_color: $.f_color,
  },
  and(
    person_color($.name, $.color),
    collecto(
      {
        name: $.f_name,
        color: $.f_color, 
      },
      and(
        friends($.name, $.f_name),
        person_color($.f_name, $.f_color),
      ),
      $.f_names,
    ),
    favnum($.name, $.favnum),
  ),
]).forEach((x: any) => console.log(x))

await relDB.db.destroy();



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







