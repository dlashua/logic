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
import { fail, not, substLog } from "../../relations/control.ts";
import { lengtho, membero } from "../../relations/lists.ts";
import { minuso, pluso } from "../../relations/numeric.ts";
import { queryUtils } from "../../shared/utils.ts";

const houses = lvar("houses");
function link(obj) {
	const things = ["house", "person", "pet", "drinks", "smokes", "color"];
	const vars = things.map((key) => obj[key] ?? lvar());
	return membero([...vars], houses);
}

function left(leftobj, rightobj) {
	const leftnum = lvar("leftnum");
	const rightnum = lvar("rightnum");
	return and(
		minuso(rightnum, 1, leftnum),
		// not(eq(rightnum, 1)),
		// not(eq(leftnum, 5)),
		link({ house: leftnum, ...leftobj }),
		link({ house: rightnum, ...rightobj }),
	);
}

function next(aobj, bobj) {
	const anum = lvar("anum");
	const bnum = lvar("bnum");
	return and(
		or(
			//   and(
			minuso(anum, 1, bnum),
			// not(eq(anum, 1)),
			// not(eq(bnum, 5)),
			//   ),
			//   and(
			minuso(bnum, 1, anum),
			// not(eq(bnum, 1)),
			// not(eq(anum, 5)),
			//   ),
		),
		link({ house: anum, ...aobj }),
		link({ house: bnum, ...bobj }),
	);
}

function exists(key, items) {
	return and(...items.map((item) => link({ [key]: item })));
}

const start = Date.now();
const results = await query()
	.select(($) => ({ x: houses }))
	.where(($) => [
		eq(houses, [$.house1, $.house2, $.house3, $.house4, $.house5]),

		eq($.house1, [1, $._, $._, $._, $._, $._]),
		eq($.house2, [2, $._, $._, $._, $._, $._]),
		eq($.house3, [3, $._, $._, $._, $._, $._]),
		eq($.house4, [4, $._, $._, $._, $._, $._]),
		eq($.house5, [5, $._, $._, $._, $._, $._]),

		link({ house: 1, person: "norwegian" }),
		next({ person: "norwegian" }, { color: "blue" }),
		link({ house: 3, drinks: "milk" }),

		link({ color: "green", drinks: "coffee" }),
		left({ color: "green" }, { color: "white" }),

		link({ color: "yellow", smokes: "dunhill" }),

		link({ person: "brit", color: "red" }),
		link({ person: "swede", pet: "dogs" }),
		link({ person: "dane", drinks: "tea" }),
		link({ smokes: "pall mall", pet: "birds" }),
		link({ person: "german", smokes: "prince" }),
		link({ smokes: "blue master", drinks: "beer" }),

		next({ smokes: "dunhill" }, { pet: "horses" }),
		next({ smokes: "blends" }, { pet: "cats" }),
		next({ smokes: "blends" }, { drinks: "water" }),

		exists("house", [1, 2, 3, 4, 5]),
		exists("person", ["brit", "swede", "dane", "norwegian", "german"]),
		exists("pet", ["dogs", "birds", "horses", "cats", "fish"]),
		exists("drinks", ["tea", "coffee", "milk", "beer", "water"]),
		exists("smokes", [
			"pall mall",
			"dunhill",
			"blends",
			"blue master",
			"prince",
		]),
		exists("color", ["red", "green", "yellow", "blue", "white"]),
	])
	.toArray();

console.dir(results, { depth: null });
console.log("count", results.length, "elapsed", Date.now() - start);
