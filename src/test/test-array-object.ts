import { makeFacts, makeFactsObj } from "facts";
import {
  and,
  collect_distincto,
  collecto,
  eitherOr,
  eq,
  fresh,
  membero,
  or,
  query,
  uniqueo,
  type Term,
} from "logic";

const people = makeFactsObj(["name", "vzid", "managerVzid"]);

const start = Date.now();
const results = await query()
  //   .select(($) => ({ ary: $.ary, obj: $.obj }))
  .where(($) => [])
  .toArray();

console.dir(results, { depth: null });
console.log("count", results.length, "elapsed", Date.now() - start);
