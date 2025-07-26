import {
  collecto,
  eitherOr,
  eq,
  extract,
  ifte,
  membero,
  query,
  run,
} from "logic";
import { makeFacts } from "facts";

const parentOf = makeFacts();
const favColor = makeFacts();

parentOf.set("celeste", "daniel");
parentOf.set("james", "daniel");
parentOf.set("jackson", "daniel");

parentOf.set("celeste", "jen");
parentOf.set("liam", "david");
parentOf.set("daniel", "rick");
parentOf.set("david", "rick");
parentOf.set("jen", "gail");
favColor.set("daniel", "blue");
favColor.set("jen", "pink");
favColor.set("celeste", "yellow");
favColor.set("liam", "yellow");

const start = Date.now();
const results = await query()
  //   .select(($) => ({ ary: $.ary, obj: $.obj }))
  .where(($) => [
    membero($.person, ["jen", "daniel", "rick"]),
    eitherOr(favColor($.person, $.favColor), eq($.favColor, "none")),
    // favColor($.person, $.favColor),
    collecto($._kid, parentOf($._kid, $.person), $.kids),
  ])
  .toArray();

console.dir(results, { depth: null });
console.log("count", results.length, "elapsed", Date.now() - start);
