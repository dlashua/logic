import { and, branch, eq, fresh, lift, Subquery } from "../core/combinators.js";
import {
	arrayToLogicList,
	enrichGroupInput,
	isVar,
	logicListToArray,
	lvar,
	unify,
	walk,
} from "../core/kernel.js";
import type { Goal, Term, Var } from "../core/types.ts";
import {
	collect_streamo,
	group_by_collect_distinct_streamo,
	group_by_collect_streamo,
} from "./aggregates.js";
import { substLog } from "./control.js";

/**
 * aggregateRelFactory: generic helper for collecto, collect_distincto, counto.
 * - x: variable to collect
 * - goal: logic goal
 * - out: output variable
 * - aggFn: aggregation function (receives array of results)
 * - dedup: if true, deduplicate results
 */

export function aggregateRelFactory(
	aggFn: (results: Term[]) => any,
	dedup = false,
) {
	return (x: Term, goal: Goal, out: Term): Goal => {
		return enrichGroupInput(
			"aggregateRelFactory",
			[],
			[goal],
			Subquery(
				goal,
				x, // extract x from each subgoal result
				out, // bind the aggregated result to this variable
				(extractedValues, _) => {
					const values = dedup ? deduplicate(extractedValues) : extractedValues;
					return aggFn(values);
				},
			),
		);
	};
}

/**
 * collecto(x, goal, xs): xs is the list of all values x can take under goal (logic relation version)
 * Usage: collecto(x, membero(x, ...), xs)
 */

export const collecto = aggregateRelFactory(
	(arr) => arrayToLogicList(arr),
	false,
);

/**
 * collect_distincto(x, goal, xs): xs is the list of distinct values of x under goal.
 * Usage: collect_distincto(x, goal, xs)
 */

export const collect_distincto = aggregateRelFactory(
	(arr) => arrayToLogicList(arr),
	true,
);

/**
 * counto(x, goal, n): n is the number of (distinct) values of x under goal.
 * Usage: counto(x, goal, n)
 */

export const counto = aggregateRelFactory((arr) => arr.length, false);

export const count_distincto = aggregateRelFactory((arr) => arr.length, true);

/**
 * count_valueo(x, goal, value, count):
 *   count is the number of times x == value in the stream of substitutions from goal.
 *   (Canonical, goal-wrapping version: aggregates over all solutions to goal.)
 *
 *   This is implemented using Subquery with a custom aggregator that counts
 *   how many times the extracted value equals the target value (walked in context).
 */

export function count_valueo(
	x: Term,
	goal: Goal,
	value: Term,
	count: Term,
): Goal {
	return Subquery(
		goal,
		x, // extract x from each subgoal result
		count, // bind the count to this variable
		(extractedValues, originalSubst) => {
			// Walk the value in the original substitution context
			const targetValue = walk(value, originalSubst);
			// Count how many extracted values match the target value
			return extractedValues.filter(
				(val) => JSON.stringify(val) === JSON.stringify(targetValue),
			).length;
		},
	);
}
// export function aggregateRelFactory(
//   aggFn: (results: Term[]) => any,
//   dedup = false,
// ) {
//   return (x: Term, goal: Goal, out: Term): Goal => {
//     const ToutAgg = lvar("ToutAgg");
//     const collect_rel = dedup ? collect_distinct_streamo : collect_streamo
//     return and(
//       goal,
//       collect_rel(x, ToutAgg, false),
//       lift(aggFn)(ToutAgg, out),
//     )
//   };
// }
/**
 * groupAggregateRelFactory(aggFn): returns a group-by aggregation goal constructor.
 * The returned function has signature (keyVar, valueVar, goal, outKey, outAgg, dedup?) => Goal
 * Example: const group_by_collecto = groupAggregateRelFactory(arrayToLogicList)
 */

export function groupAggregateRelFactory(
	aggFn: (items: any[]) => any,
	dedup = false,
) {
	return (
		keyVar: Term,
		valueVar: Term,
		goal: Goal,
		outValueAgg: Term,
	): Goal => {
		const group_by_rel = dedup
			? group_by_collect_distinct_streamo
			: group_by_collect_streamo;
		// @ts-expect-error
		const aggFnName = aggFn?.displayName || aggFn.name || "unknown";
		return enrichGroupInput(
			`groupAggregateRelFactory ${aggFnName}`,
			[],
			[goal],
			fresh((in_outValueAgg) =>
				branch(
					and(goal, group_by_rel(keyVar, valueVar, in_outValueAgg, true)),
					(observer, substs, subst) => {
						for (const oneSubst of substs) {
							const keyVal = walk(keyVar as Term, oneSubst);
							if (isVar(keyVal)) {
								continue;
							}
							const valueAggVal = walk(in_outValueAgg, oneSubst);
							if (isVar(valueAggVal)) {
								continue;
							}
							const convertedAgg = aggFn(valueAggVal as any[]);
							const s2 = unify(keyVar, keyVal, subst);
							if (!s2) continue;
							const s3 = unify(outValueAgg, convertedAgg, s2);
							if (!s3) continue;
							observer.next(s3);
						}
					},
				),
			),
		);
	};
}

export const group_by_collecto = groupAggregateRelFactory(
	function group_by_collecto(x) {
		return x;
	},
);
export const group_by_counto = groupAggregateRelFactory(
	function group_by_counto(items: any[]) {
		return logicListToArray(items).length;
	},
);

/**
 * Helper: deduplicate an array of items using JSON.stringify for deep equality.
 */
function deduplicate<T>(items: T[]): T[] {
	const seen = new Set<string>();
	const result: T[] = [];
	for (const item of items) {
		const k = JSON.stringify(item);
		if (!seen.has(k)) {
			seen.add(k);
			result.push(item);
		}
	}
	return result;
}
