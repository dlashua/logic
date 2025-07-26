import { and, or } from "../../core/combinators.ts";
import { enrichGroupInput, lvar } from "../../core/kernel.ts";
import { query } from "../../query.ts";
import { neqo, substLog, uniqueo } from "../../relations/control.ts";
import { constrainArrays, enforceConsistency, link } from "./logic-helpers.ts";

export const piles = lvar();

export const keyVals = {
	a: ["A", "B", "C", "D", "E"],
	n: [1, 2, 3, 4, 5],
	d: ["AA", "BB", "CC", "DD", "EE"],
	i: [11, 22, 33, 44, 55],
	f: ["AAA", "BBB", "CCC", "DDD", "EEE"],
};

function exists($: any, key: string, items: any[]) {
	return and(...items.map((item: any) => link($, { [key]: item })));
}

console.dir(
	await query()
		.select(piles)
		.where(($) => [
			constrainArrays($, 5),
			link($, {
				a: "A",
				n: 1,
				d: "AA",
				i: 11,
				f: "AAA",
			}),
			link($, {
				a: "B",
				n: 2,
				d: "BB",
				i: 22,
				f: "BBB",
			}),
			link($, {
				a: "C",
				n: 3,
				d: "CC",
				i: 33,
				f: "CCC",
			}),
			link($, {
				a: "D",
				n: 4,
				d: "DD",
				i: 44,
				f: "DDD",
			}),
			link($, {
				a: "E",
				n: 5,
				d: "EE",
				i: 55,
				f: "EEE",
			}),
			enforceConsistency($, 5),
		])
		.toArray(),
	{ depth: null },
);
