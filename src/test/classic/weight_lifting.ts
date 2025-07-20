import { getRandomValues } from "crypto";
import { exit } from "process";
import {
  and,
  conde,
  eq,
  fresh,
  ifte,
  lift,
  or
} from "../../core/combinators.ts"
import { query } from "../../query.ts";
import {
  fail,
  failo,
  neqo,
  not,
  substLog
} from "../../relations/control.ts"
import { lengtho, membero } from "../../relations/lists.ts";
import { Subst, Term } from "../../core/types.ts";
import { SimpleObservable } from "../../core/observable.ts";
import { queryUtils } from "../../shared/utils.ts";
import { arrayToLogicList, isVar, lvar, unify } from "../../core/kernel.ts";
import {
  gteo,
  gto,
  lto,
  minuso,
  pluso
} from "../../relations/numeric.ts"
import { group_by_collecto, group_by_counto } from "../../relations/aggregates-subqueries.ts";
import { suspendable, CHECK_LATER } from "../../core/suspend-helper.ts";
import { allDifferentConstraint, cspSolver } from "../../core/csp-solver.ts";
import { make } from "./logic-helpers.ts";

const lifters = lvar("lifters");

const keyVals = {
  name: ["brent", "jeremy", "nicola", "oliver"],
  from: ["dane", "korea", "s_africa", "swede"],
  weight: [880, 920, 960, 1000],
};

// Much more efficient: Use suspendable constraints that only validate when grounded


const { constrainArrays, enforceConsistency, link, unlink, distinctValidateAll, getVar } = make(lifters, keyVals, "name", 4);

const start = Date.now();
const results = await query()
  .select(lifters)
  .where($ => [

    constrainArrays($),

    // Add domain constraints since enforceConsistency doesn't actually do it
    // domainConstraints($),

    // 1. Nicola is either the man who will lift 920 lbs or the Korean.
    or(
      and(
        link($, { name: "nicola", weight: 920 }),
        unlink($, { name: "nicola" }, { from: "korea" }),
      ),
      and (
        link($, { name: "nicola", from: "korea" }),
        unlink($, { name: "nicola" }, { weight: 920 }),
      )
    ),

    // 2. Jeremy is the Korean.
    link($, { name: "jeremy", from: "korea" }),

    // // 3. Nicola will lift a weight that is 80 pounds lighter than the one selected by the Dane.
    unlink($, { name: "nicola" }, { weight: 1000 }),
    unlink($, { name: "nicola" }, { from: "dane" }),
    or(
      ...(["jeremy", "oliver", "brent"].map(person => 
        and(
          eq(getVar($, person, "from"), "dane"),
          pluso($.name_nicola_weight, 80, getVar($, person, "weight")),
        ),
      )),
    ),

    // // 4. Brent will lift 1,000 lbs.
    eq($.name_brent_weight, 1000),

    // // 5. The Korean will lift a weight that is 40 pounds lighter than the one selected by the Swede.
    or(
      ...(keyVals.name.flatMap(kperson => 
        keyVals.name.map(sperson => and(
          eq(getVar($, kperson, "from"), "korea"),
          eq(getVar($, sperson, "from"), "swede"),
          pluso(getVar($, kperson, "weight"), 40, getVar($, sperson, "weight")),
        )),
      )),
    ),

    ...keyVals.name.map(person => 
      or(
        ...keyVals.from.map(place => eq(getVar($, person, "from"), place))
      ),
    ),

    ...keyVals.name.map(person => 
      or(
        ...keyVals.weight.map(weight => eq(getVar($, person, "weight"), weight))
      ),
    ),

  ]).toArray();

console.dir(results, { depth: null });
console.log("count", results.length, "elapsed", Date.now() - start);