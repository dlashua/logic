import {
	and,
	conde,
	eq,
	fresh,
	ifte,
	lift,
	or,
} from "../../core/combinators.ts";
import { isVar, lvar, unify } from "../../core/kernel.ts";
import { SimpleObservable } from "../../core/observable.ts";
import { Subst, Term } from "../../core/types.ts";
import { query } from "../../query.ts";
import {
	group_by_collecto,
	group_by_counto,
} from "../../relations/aggregates-subqueries.ts";
import {
	fail,
	failo,
	neqo,
	not,
	substLog,
	thruCount,
} from "../../relations/control.ts";
import { lengtho, membero } from "../../relations/lists.ts";
import { gteo, gto, lto, minuso, pluso } from "../../relations/numeric.ts";
import { queryUtils } from "../../shared/utils.ts";
import { collectThenGo, make } from "./logic-helpers.ts";

const recipes = lvar("recipes");
const keyVals = {
	name: [
		"baked_ham",
		"bean_casserole",
		"candied_yams",
		"chicken_cutlet",
		"mac_and_cheese",
		"meatloaf",
		"roast_potatoes",
	],
	baking_time: [25, 30, 35, 40, 45, 50, 55],
	servings: [2, 3, 4, 6, 7, 8, 9],
	temp: [325, 340, 350, 375, 400, 410, 425],
};

const {
	constrainArrays,
	enforceConsistency,
	link,
	unlink,
	distinctValidateAll,
	getVar,
	smartMemberoAll,
} = make(recipes, keyVals, "name", 7);

const start = Date.now();

const results = await query()
	.select(recipes)
	.where(($) => [
		constrainArrays($),
		thruCount("constrainArrays"),
		collectThenGo(),

		// 2. The dish that bakes for 40 minutes doesn't cook at 410 degrees.
		unlink($, { baking_time: 40 }, { temp: 410 }),

		thruCount("constraint 2"),
		collectThenGo(),

		// 4. The candied yams recipe doesn't cook at 325 degrees.
		unlink($, { name: "candied_yams" }, { temp: 325 }),
		thruCount("constraint 4"),
		collectThenGo(),

		// 11. The chicken cutlet doesn't cook at 350 degrees.
		unlink($, { name: "chicken_cutlet" }, { temp: 350 }),
		thruCount("constraint 11"),
		collectThenGo(),

		// 15. The recipe that bakes for 45 minutes doesn't cook at 350 degrees.
		unlink($, { baking_time: 45 }, { temp: 350 }),
		thruCount("constraint 15"),
		collectThenGo(),

		// 5. The recipe that serves 4 people is either the dish that bakes for 25 minutes or the recipe that bakes for 35 minutes.
		or(
			link($, { servings: 4, baking_time: 25 }),
			link($, { servings: 4, baking_time: 35 }),
		),
		thruCount("constraint 5"),
		collectThenGo(),

		// 7. The recipe that cooks at 340 degrees is either the recipe that bakes for 30 minutes or the dish that bakes for 50 minutes.
		or(
			link($, { temp: 340, baking_time: 30 }),
			link($, { temp: 340, baking_time: 50 }),
		),
		thruCount("constraint 7"),
		collectThenGo(),

		// 6. The seven dishes are the meatloaf, the recipe that cooks at 325 degrees, the recipe that serves 2 people, the recipe that bakes for 30 minutes, the recipe that bakes for 50 minutes, the recipe that bakes for 55 minutes and the dish that serves 3 people.
		// Each of these cannot be any of the other listed attributes.
		// Unlink all pairs among the 7 items
		unlink($, { name: "meatloaf" }, { temp: 325 }),
		thruCount("constraint 6 1 1"),
		collectThenGo(),

		unlink($, { name: "meatloaf" }, { servings: 2 }),
		thruCount("constraint 6 1 2"),
		collectThenGo(),

		unlink($, { name: "meatloaf" }, { servings: 3 }),
		thruCount("constraint 6 1 3"),
		collectThenGo(),

		unlink($, { name: "meatloaf" }, { baking_time: 30 }),
		thruCount("constraint 6 1 4"),
		collectThenGo(),

		unlink($, { name: "meatloaf" }, { baking_time: 50 }),
		thruCount("constraint 6 1 5"),
		collectThenGo(),

		unlink($, { name: "meatloaf" }, { baking_time: 55 }),
		thruCount("constraint 6 1"),
		collectThenGo(),

		unlink($, { temp: 325 }, { servings: 2 }),
		thruCount("constraint 6 2 1"),
		collectThenGo(),

		unlink($, { temp: 325 }, { servings: 3 }),
		thruCount("constraint 6 2 2"),
		collectThenGo(),

		unlink($, { temp: 325 }, { baking_time: 30 }),
		thruCount("constraint 6 2 3"),
		collectThenGo(),

		unlink($, { temp: 325 }, { baking_time: 50 }),
		thruCount("constraint 6 2 4"),
		collectThenGo(),

		unlink($, { temp: 325 }, { baking_time: 55 }),
		thruCount("constraint 6 2"),
		collectThenGo(),

		unlink($, { servings: 2 }, { baking_time: 30 }),
		thruCount("constraint 6 3 1"),
		collectThenGo(),

		unlink($, { servings: 2 }, { baking_time: 50 }),
		thruCount("constraint 6 3 2"),
		collectThenGo(),
		unlink($, { servings: 2 }, { baking_time: 55 }),
		thruCount("constraint 6 3"),
		collectThenGo(),

		unlink($, { servings: 3 }, { baking_time: 30 }),
		thruCount("constraint 6 4 1"),
		collectThenGo(),

		unlink($, { servings: 3 }, { baking_time: 50 }),
		thruCount("constraint 6 4 2"),
		collectThenGo(),

		unlink($, { servings: 3 }, { baking_time: 55 }),
		thruCount("constraint 6"),
		collectThenGo(),

		// 14. Of the mac and cheese and the dish that bakes for 55 minutes, one cooks at 425 degrees and the other serves 9 people.
		unlink($, { name: "mac_and_cheese" }, { baking_time: 55 }),
		unlink($, { temp: 425 }, { servings: 9 }),
		or(
			and(
				link($, { name: "mac_and_cheese", temp: 425 }),
				link($, { baking_time: 55, servings: 9 }),
				// If mac_and_cheese cooks at 425, then it doesn't bake for 55 minutes
				// If the 55-minute dish serves 9 people, then it doesn't cook at 425 degrees
				unlink($, { baking_time: 55 }, { temp: 425 }),
				unlink($, { name: "mac_and_cheese" }, { servings: 9 }),
			),
			and(
				link($, { name: "mac_and_cheese", servings: 9 }),
				link($, { baking_time: 55, temp: 425 }),
				// If mac_and_cheese serves 9 people, then it doesn't bake for 55 minutes
				unlink($, { name: "mac_and_cheese" }, { temp: 425 }),
				// If the 55-minute dish cooks at 425 degrees, then it doesn't serve 9 people
				unlink($, { baking_time: 55 }, { servings: 9 }),
			),
		),
		thruCount("constraint 14"),

		// 8. The dish that serves 9 people bakes 10 minutes longer than the recipe that serves 8 people.
		link($, { servings: 8, baking_time: $.servings_8_baking_time }),
		link($, { servings: 9, baking_time: $.servings_9_baking_time }),
		pluso($.servings_8_baking_time, 10, $.servings_9_baking_time),
		unlink($, { servings: 8 }, { baking_time: 55 }),
		unlink($, { servings: 9 }, { baking_time: 25 }),
		// The dish that serves 8 people and the dish that serves 9 people are different
		// unlink($, { servings: 8 }, { servings: 9 }),
		thruCount("constraint 8"),

		// 1. The recipe that cooks at 340 degrees requires 10 minutes less baking time than the chicken cutlet.
		link($, { temp: 340, baking_time: $.temp_340_baking_time }),
		// link($, { name: "chicken_cutlet", baking_time: $.name_chicken_cutlet_baking_time }),
		pluso($.temp_340_baking_time, 10, $.name_chicken_cutlet_baking_time),
		unlink($, { temp: 340 }, { baking_time: 55 }),
		unlink($, { name: "chicken_cutlet" }, { baking_time: 25 }),
		// The chicken cutlet doesn't cook at 340 degrees (they are separate dishes)
		unlink($, { name: "chicken_cutlet" }, { temp: 340 }),
		thruCount("constraint 1"),

		// 3. The dish that serves 3 people bakes 10 minutes longer than the baked ham.
		// link($, { name: "baked_ham", baking_time: $.name_baked_ham_baking_time }),
		link($, { servings: 3, baking_time: $.servings_3_baking_time }),
		pluso($.name_baked_ham_baking_time, 10, $.servings_3_baking_time),
		unlink($, { name: "baked_ham" }, { baking_time: 55 }),
		unlink($, { servings: 3 }, { baking_time: 25 }),
		// The baked ham doesn't serve 3 people (they are separate dishes)
		unlink($, { name: "baked_ham" }, { servings: 3 }),
		thruCount("constraint 3"),

		// 12. The dish that cooks at 375 degrees bakes 20 minutes longer than the candied yams recipe.
		// link($, { name: "candied_yams", baking_time: $.name_candied_yams_baking_time }),
		link($, { temp: 375, baking_time: $.temp_375_baking_time }),
		pluso($.name_candied_yams_baking_time, 20, $.temp_375_baking_time),
		unlink($, { name: "candied_yams" }, { baking_time: 55 }),
		unlink($, { temp: 375 }, { baking_time: 25 }),
		// The candied yams doesn't cook at 375 degrees (they are separate dishes)
		unlink($, { name: "candied_yams" }, { temp: 375 }),
		thruCount("constraint 12"),

		// 9. The dish that cooks at 325 degrees requires somewhat less baking time than the roast potatoes recipe.
		link($, { temp: 325, baking_time: $.temp_325_baking_time }),
		// link($, { name: "roast_potatoes", baking_time: $.name_roast_potatoes_baking_time }),
		lto($.temp_325_baking_time, $.name_roast_potatoes_baking_time),
		unlink($, { temp: 325 }, { baking_time: 55 }),
		unlink($, { name: "roast_potatoes" }, { baking_time: 25 }),
		// The roast potatoes doesn't cook at 325 degrees (they are separate dishes)
		unlink($, { name: "roast_potatoes" }, { temp: 325 }),
		thruCount("constraint 9"),

		// 10. The meatloaf bakes somewhat longer than the roast potatoes recipe.
		// link($, { name: "meatloaf", baking_time: $.name_meatloaf_baking_time }),
		gto($.name_meatloaf_baking_time, $.name_roast_potatoes_baking_time),
		// The meatloaf and roast potatoes are different dishes
		// unlink($, { name: "meatloaf" }, { name: "roast_potatoes" }),
		thruCount("constraint 10"),

		// 13. The recipe that serves 7 people bakes 5 minutes longer than the recipe that cooks at 325 degrees.
		link($, { temp: 325, baking_time: $.temp_325_baking_time2 }),
		link($, { servings: 7, baking_time: $.servings_7_baking_time }),
		pluso($.temp_325_baking_time2, 5, $.servings_7_baking_time),
		// The dish that serves 7 people doesn't cook at 325 degrees (they are separate dishes)
		unlink($, { servings: 7 }, { temp: 325 }),
		thruCount("constraint 13"),

		// and(
		//   ...(Object.keys(keyVals).filter(x => x !== "name").map(field =>
		//     and(
		//       ...(keyVals[field].map(fieldVal =>
		//         or(
		//           ...(keyVals.name.map(name =>
		//             and(
		//               eq($[`name_${name}_${field}`], fieldVal),
		//               thruCount(`final ${name} ${field} ${fieldVal}`),
		//               // collectThenGo(),
		//             )
		//           ))
		//         )
		//       ))
		//     )
		//   ))
		// ),

		smartMemberoAll($),

		// enforceConsistency($),
		// distinctValidateAll($),
		thruCount("OUT"),
	])
	.toArray();

if (results.length < 20) {
	console.dir(results, { depth: null });
}
console.log("count", results.length, "elapsed", Date.now() - start);

// 1. The recipe that cooks at 340 degrees requires 10 minutes less baking time than the chicken cutlet.
// 2. The dish that bakes for 40 minutes doesn't cook at 410 degrees.
// 3. The dish that serves 3 people bakes 10 minutes longer than the baked ham.
// 4. The candied yams recipe doesn't cook at 325 degrees.
// 5. The recipe that serves 4 people is either the dish that bakes for 25 minutes or the recipe that bakes for 35 minutes.
// 6. The seven dishes are the meatloaf, the recipe that cooks at 325 degrees, the recipe that serves 2 people, the recipe that bakes for 30 minutes, the recipe that bakes for 50 minutes, the recipe that bakes for 55 minutes and the dish that serves 3 people.
// 7. The recipe that cooks at 340 degrees is either the recipe that bakes for 30 minutes or the dish that bakes for 50 minutes.
// 8. The dish that serves 9 people bakes 10 minutes longer than the recipe that serves 8 people.
// 9. The dish that cooks at 325 degrees requires somewhat less baking time than the roast potatoes recipe.
// 10. The meatloaf bakes somewhat longer than the roast potatoes recipe.
// 11. The chicken cutlet doesn't cook at 350 degrees.
// 12. The dish that cooks at 375 degrees bakes 20 minutes longer than the candied yams recipe.
// 13. The recipe that serves 7 people bakes 5 minutes longer than the recipe that cooks at 325 degrees.
// 14. Of the mac and cheese and the dish that bakes for 55 minutes, one cooks at 425 degrees and the other serves 9 people.
// 15. The recipe that bakes for 45 minutes doesn't cook at 350 degrees.
