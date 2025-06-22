import {
  LogicList, 
  Subst, 
  Term, arrayToLogicList, cons, isNil, nil, unify, walk, 
} from './core.ts';
import {
  Goal, 
  eq, 
} from './relations.ts';

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

/**
 * Helper: group by key for logic goals, then apply a callback per group.
 * Calls cb(s, key, items) for each group, where key is the group key and items is the array of values.
 */
export async function* groupByGoal(
  keyVar: Term,
  valueVar: Term,
  goal: Goal,
  s: Subst,
  cb: (s: Subst, key: any, items: any[]) => AsyncGenerator<Subst>,
): AsyncGenerator<Subst> {
  // Collect all (key, value) pairs
  const pairs: { key: any, value: any }[] = [];
  for await (const s1 of goal(s)) {
    const key = await walk(keyVar, s1);
    const value = await walk(valueVar, s1);
    pairs.push({
      key,
      value, 
    });
  }
  // Group by key (map to array of values)
  const grouped = new Map<string, { key: any, items: any[] }>();
  for (const { key, value } of pairs) {
    const k = JSON.stringify(key); // Use JSON.stringify for deep equality
    if (!grouped.has(k)) grouped.set(k, {
      key,
      items: [], 
    });
    const group = grouped.get(k);
    if (group) group.items.push(value);
  }
  // For each group, yield using the callback
  for (const { key, items } of grouped.values()) {
    for await (const s2 of cb(s, key, items)) {
      yield s2;
    }
  }
}

/**
 * aggregateRelFactory: generic helper for collecto, distincto, counto.
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
  return (
    x: Term, 
    goal: Goal, 
    out: Term,
  ): Goal => {
    return async function* (s: Subst) {
      const results: Term[] = [];
      for await (const s1 of goal(s)) {
        const val = await walk(x, s1);
        results.push(val);
      }
      const agg = aggFn(dedup ? deduplicate(results) : results);
      yield* eq(out, agg)(s);
    };
  }
}

/**
 * groupAggregateRelFactory(aggFn): returns a group-by aggregation goal constructor.
 * The returned function has signature (keyVar, valueVar, goal, outKey, outAgg, dedup?) => Goal
 * Example: const groupCollecto = groupAggregateRelFactory(arrayToLogicList)
 */
export function groupAggregateRelFactory(
  aggFn: (items: any[]) => any,
) {
  return function (
    keyVar: Term, 
    valueVar: Term, 
    goal: Goal, 
    outKey: Term, 
    outAgg: Term, 
    dedup = false,
  ): Goal {
    return async function* (s: Subst) {
      yield* groupByGoal(keyVar, valueVar, goal, s, async function* (s, key, items) {
        const groupItems = dedup ? deduplicate(items) : items;
        const agg = aggFn(groupItems);
        for await (const s2 of eq(outKey, key)(s)) {
          for await (const s3 of eq(outAgg, agg)(s2)) {
            yield s3;
          }
        }
      });
    };
  };
}

export const groupCollecto = groupAggregateRelFactory(arrayToLogicList);
export const groupCounto = groupAggregateRelFactory((items: any[]) => items.length);

/**
 * collecto(x, goal, xs): xs is the list of all values x can take under goal (logic relation version)
 * Usage: collecto(x, membero(x, ...), xs)
 */
export const collecto = aggregateRelFactory(arr => arrayToLogicList(arr))

/**
 * distincto(x, goal, xs): xs is the list of distinct values of x under goal.
 * Usage: distincto(x, goal, xs)
 */
export const distincto = aggregateRelFactory(arr => arrayToLogicList(arr), true)

/**
 * counto(x, goal, n): n is the number of (distinct) values of x under goal.
 * Usage: counto(x, goal, n)
 */
export const counto = aggregateRelFactory(arr => arr.length);

