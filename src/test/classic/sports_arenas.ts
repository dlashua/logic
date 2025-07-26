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
import { type Subst, Term } from "../../core/types.ts";
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
import { make } from "./logic-helpers.ts";

const arenas = lvar("arenas");

const pops = [110, 150, 190, 230, 270, 310, 350];
const sports = [
	"baseball",
	"basketball",
	"football",
	"hockey",
	"lacrosse",
	"rugby",
	"soccer",
];
// const names = ["beck", "dotson", "frederick", "ingram", "rowe", "underwood", "wilcox"];
const names = [
	"beck",
	"dotson",
	"frederick",
	"ingram",
	"rowe",
	"underwood",
	"wilcox",
];

const towns = [
	"brunswick",
	"leeds",
	"new_c",
	"ocotillo",
	"paramount",
	"schaller",
	"zwingle",
];

const keyVals = {
	name: names,
	pop: pops,
	town: towns,
	sport: sports,
};

function collectThenGo() {
	return (input$: SimpleObservable<Subst>) =>
		new SimpleObservable<Subst>((observer) => {
			const substs: Subst[] = [];
			const sub = input$.subscribe({
				next: (s) => substs.push(s),
				error: observer.error,
				complete: () => {
					for (const oneS of substs) {
						observer.next(oneS);
					}
					observer.complete?.();
				},
			});

			return () => sub.unsubscribe();
		});
}

const { constrainArrays, enforceConsistency, link, unlink, smartMemberoAll } =
	make(arenas, keyVals, "name", 7);

const start = Date.now();
const results = query()
	.select(arenas)
	.where(($) => [
		constrainArrays($),
		thruCount("constrainArrays"),
		collectThenGo(),

		// 16. The facility with seating for 350 people isn't in New Cuyama.
		// 16
		unlink($, { pop: 350 }, { town: "new_c" }),
		thruCount("constraint 16"),

		collectThenGo(),

		// 14. The football facility holds more people than Dotson Arena.
		// 14
		unlink($, { sport: "football" }, { pop: 110 }),
		unlink($, { name: "dotson" }, { pop: 350 }),
		unlink($, { name: "dotson" }, { sport: "football" }),
		link($, { sport: "football", pop: $.sport_football_pop }),
		gto($.sport_football_pop, $.name_dotson_pop),
		thruCount("constraint 14"),

		collectThenGo(),

		// 11. The facility in Ocotillo holds 120 more people than Wilcox Arena.
		// 11
		unlink($, { name: "wilcox" }, { town: "ocotillo" }),
		unlink($, { town: "ocotillo" }, { pop: 110 }),
		unlink($, { name: "wilcox" }, { pop: 350 }),
		link($, { town: "ocotillo", pop: $.town_ocotillo_pop }),
		pluso($.name_wilcox_pop, 120, $.town_ocotillo_pop),
		thruCount("constraint 11"),

		collectThenGo(),

		// 7. Wilcox Arena holds 80 fewer people than the baseball facility.
		// 7
		unlink($, { name: "wilcox" }, { pop: 350 }),
		unlink($, { sport: "baseball" }, { pop: 110 }),
		unlink($, { name: "wilcox" }, { sport: "baseball" }),
		link($, { sport: "baseball", pop: $.sport_baseball_pop }),
		pluso($.name_wilcox_pop, 80, $.sport_baseball_pop),
		thruCount("constraint 7"),

		collectThenGo(),

		// 9. The arena in Ocotillo is either the facility with seating for 310 people or the lacrosse facility.
		// 9
		// Note: No unlinks needed here - Ocotillo can be either 310-person or lacrosse arena
		or(
			link($, { pop: 310, town: "ocotillo" }),
			link($, { sport: "lacrosse", town: "ocotillo" }),
		),
		thruCount("constraint 9"),

		collectThenGo(),

		// 2. The arena in New Cuyama is either the football facility or the arena with seating for 150 people.
		// 2
		// Note: No unlinks needed here - New Cuyama can be either football or 150-person arena
		or(
			link($, { town: "new_c", sport: "football" }),
			link($, { town: "new_c", pop: 150 }),
		),
		thruCount("constraint 2"),

		collectThenGo(),

		// 4. The arena in New Cuyama holds more people than the facility in Ocotillo.
		// 4
		unlink($, { town: "new_c" }, { pop: 110 }),
		unlink($, { town: "ocotillo" }, { pop: 350 }),
		link($, { town: "new_c", pop: $.town_new_c_pop }),
		link($, { town: "ocotillo", pop: $.town_ocotillo_pop }),
		gto($.town_new_c_pop, $.town_ocotillo_pop),
		thruCount("constraint 4"),

		collectThenGo(),

		// 3. The basketball facility holds 40 more people than the hockey facility.
		// 3
		unlink($, { sport: "hockey" }, { pop: 350 }),
		unlink($, { sport: "basketball" }, { pop: 110 }),
		link($, { sport: "hockey", pop: $.sport_hockey_pop }),
		link($, { sport: "basketball", pop: $.sport_basketball_pop }),
		pluso($.sport_hockey_pop, 40, $.sport_basketball_pop),
		thruCount("constraint 3"),

		collectThenGo(),

		// 6. Neither the facility in Leeds nor the rugby facility is Dotson Arena.
		// 6
		unlink($, { name: "dotson" }, { town: "leeds" }),
		unlink($, { name: "dotson" }, { sport: "rugby" }),
		unlink($, { town: "leeds" }, { sport: "rugby" }),
		thruCount("constraint 6"),

		collectThenGo(),

		// 1. Frederick Arena isn't in Ocotillo.
		// 1
		unlink($, { name: "frederick" }, { town: "ocotillo" }),
		thruCount("constraint 1"),

		collectThenGo(),

		// 13. Rowe Arena is either the rugby facility or the arena in Leeds.
		// 13
		unlink($, { town: "leeds" }, { sport: "rugby" }),

		or(
			and(
				link($, { name: "rowe", sport: "rugby" }),
				unlink($, { name: "rowe" }, { town: "leeds" }),
			),
			and(
				link($, { name: "rowe", town: "leeds" }),
				unlink($, { name: "rowe" }, { sport: "rugby" }),
			),
		),
		thruCount("constraint 13"),

		collectThenGo(),

		// 17. Of the rugby facility and Ingram Arena, one is in Zwingle and the other holds 350 people.
		// 17
		unlink($, { name: "ingram" }, { sport: "rugby" }),
		unlink($, { pop: 350 }, { town: "zwingle" }),

		or(
			and(
				// rugby = 350, ingram = zwingle
				link($, { sport: "rugby", pop: 350 }),
				link($, { name: "ingram", town: "zwingle" }),
				unlink($, { sport: "rugby" }, { town: "zwingle" }),
				unlink($, { name: "ingram" }, { pop: 350 }),
			),
			and(
				// rugby = zwingle , ingram = 350
				link($, { sport: "rugby", town: "zwingle" }),
				link($, { name: "ingram", pop: 350 }),
				unlink($, { pop: 350 }, { sport: "rugby" }),
				unlink($, { name: "ingram" }, { town: "zwingle" }),
			),
		),
		thruCount("constraint 17"),

		collectThenGo(),

		// 12. Of the facility in Schaller and the arena with seating for 150 people, one is Beck Arena and the other is set up for basketball games.
		// 12
		unlink($, { name: "beck" }, { sport: "basketball" }),
		unlink($, { town: "schaller" }, { pop: 150 }),
		thruCount("constraint 12 top"),

		or(
			and(
				// beck = 150, basketball = schaller
				thruCount("constraint 12 or 2 start"),

				link($, { name: "beck", pop: 150 }),
				link($, { sport: "basketball", town: "schaller" }),
				unlink($, { name: "beck" }, { town: "schaller" }),
				unlink($, { sport: "basketball" }, { pop: 150 }),
				thruCount("constraint 12 or 2 end"),
			),
			and(
				// beck = schaller, basketball = 150
				thruCount("constraint 12 or 1 start"),

				link($, { name: "beck", town: "schaller" }),
				link($, { sport: "basketball", pop: 150 }),
				unlink($, { name: "beck" }, { pop: 150 }),
				unlink($, { town: "schaller" }, { sport: "basketball" }),
				thruCount("constraint 12 or 1 end"),
			),
		),
		thruCount("constraint 12"),

		collectThenGo(),

		// 10. Dotson Arena isn't set up for lacrosse games.
		// 10
		unlink($, { name: "dotson" }, { sport: "lacrosse" }),
		thruCount("constraint 10"),

		collectThenGo(),

		// 8. The arena in Paramount isn't set up for hockey games.
		// 8
		unlink($, { town: "paramount" }, { sport: "hockey" }),
		thruCount("constraint 8"),

		collectThenGo(),

		// 15. The basketball facility holds 80 fewer people than Beck Arena.
		// 15
		unlink($, { sport: "basketball" }, { pop: 350 }),
		unlink($, { name: "beck" }, { pop: 110 }),
		unlink($, { name: "beck" }, { sport: "basketball" }),
		link($, { sport: "basketball", pop: $.sport_basketball_pop }),
		pluso($.sport_basketball_pop, 80, $.name_beck_pop),
		thruCount("constraint 15"),

		collectThenGo(),

		// 5. The arena in Paramount holds fewer people than the arena in Zwingle.
		// 5
		unlink($, { town: "paramount" }, { pop: 350 }),
		unlink($, { town: "zwingle" }, { pop: 110 }),
		link($, { town: "paramount", pop: $.town_paramount_pop }),
		link($, { town: "zwingle", pop: $.town_zwingle_pop }),
		lto($.town_paramount_pop, $.town_zwingle_pop),
		thruCount("constraint 5"),

		collectThenGo(),

		// and(
		//   ...(Object.keys(keyVals).filter(x => x !== "name").map(field =>
		//     and(
		//       ...(keyVals.name.map(name =>
		//         or(
		//           ...(keyVals[field].map(fieldVal =>
		//             and(
		//               eq($[`name_${name}_${field}`], fieldVal),
		//               thruCount(`final ${name} ${field} ${fieldVal}`),
		//               collectThenGo(),
		//             )
		//           ))
		//         )
		//       ))
		//     )
		//   ))
		// ),

		// Assign all populations, towns, and sports using smartMemberoAll
		smartMemberoAll($),
		thruCount("smartMemberoAll"),
		collectThenGo(),

		// substLog("top", true),
		// enforceConsistency($),
		thruCount("OUT"),
	]);

let cnt = 0;
const res = [];
await new Promise((resolve) =>
	results.toObservable().subscribe({
		// next: (v) => console.dir(v, { depth: null }),
		next: (v) => {
			cnt++;
			if (cnt % 1000 === 0) {
				console.log(cnt);
			}
			res.push(v);
		},
		complete: () => resolve(null),
	}),
);

console.log(cnt);

// console.dir(results, { depth: null });
if (res.length <= 32) {
	console.dir(res, { depth: null });
}
console.log("count", cnt, "elapsed", Date.now() - start);

// [
//   [
//     [ 'beck', 230, 'schaller', 'baseball' ],
//     [ 'dotson', 110, 'brunswick', 'hockey' ],
//     [ 'frederick', 310, 'new_c', 'football' ],
//     [ 'ingram', 350, 'leeds', 'soccer' ],
//     [ 'rowe', 190, 'zwingle', 'rugby' ],
//     [ 'underwood', 270, 'ocotillo', 'lacrosse' ],
//     [ 'wilcox', 150, 'paramount', 'basketball' ]
//   ]
// ]
