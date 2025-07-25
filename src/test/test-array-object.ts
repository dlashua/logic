import { and, eq, or } from "../core/combinators.ts";
import { lvar } from "../core/kernel.ts";
import { query } from "../query.ts";
import { neqo, thruCount } from "../relations/control.ts";
import { multo, pluso } from "../relations/numeric.ts";
import { make } from "./classic/logic-helpers.ts";

const piles = lvar("piles");
const keyVals = {
  a: [1,2,3,4,5],
  b: [11,22,33,44,55],
  c: [111,222,333,444,555],
  d: [1111,2222,3333,4444,5555],
  e: [11111,22222,33333,44444,55555],
}

const { constrainArrays, unlink, link, distinctValidateAll } = make(piles, keyVals, "a", 5);
const start = Date.now();
const results = await query()
  .select(piles)
  .where($ => [

    constrainArrays($),

    link($, { c: 444, d: 4444 }),
    link($, { a: 1, c: 111 }),
    link($, { b: 22, c: 222 }),
    link($, { b: 33, c: 333 }),

    or(
      link($, { a: 2, b: 22 }),
      link($, { a: 1, d: 3333 }),
    ),
    pluso($.a_1_b, 11, $.a_2_b),

    or(
      link($, { c:333, d: 3333 }),
      link($, { c:444, d: 5555 }),
    ),

    // or(
    link($, { b:22, d: 2222 }),
    link($, { c:555, d: 5555 }),
    // ),

    pluso($.a_1_d, 3333, $.a_4_d),
    pluso($.a_1_b, 11100, $.a_1_e),
    pluso($.a_2_b, 22200, $.a_2_e),
    pluso($.a_3_b, 33300, $.a_3_e),
    pluso($.a_4_b, 44400, $.a_4_e),
    pluso($.a_5_b, 55500, $.a_5_e),

    link($, { a: 5, b: 55 }),

    neqo($.a_4_c, 555),
    neqo($.a_1_d, 3333),

    // distinctValidateAll($),

  ]).toArray();

console.dir(results, { depth: null });
console.log("count", results.length, "elapsed", Date.now() - start);

