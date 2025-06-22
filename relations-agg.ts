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
  // Group by key
  const grouped = new Map<any, { key: any, items: any }>();
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
 * groupAggregateo(keyVar, valueVar, goal, outKey, outAgg, aggFn):
 * For each group, yields a substitution with outKey = group key and outAgg = aggFn(items).
 * aggFn receives the array of values for the group and returns the aggregate result.
 */
export function groupAggregateo(
  keyVar: Term,
  valueVar: Term,
  goal: Goal,
  outKey: Term,
  outAgg: Term,
  aggFn: (items: any[]) => any,
): Goal {
  return async function* (s: Subst) {
    yield* groupByGoal(keyVar, valueVar, goal, s, async function* (s, key, items) {
      const agg = aggFn(items);
      for await (const s2 of eq(outKey, key)(s)) {
        for await (const s3 of eq(outAgg, agg)(s2)) {
          yield s3;
        }
      }
    });
  };
}

/**
 * aggregateRel(aggFn): returns a group-by aggregation goal constructor.
 * The returned function has signature (keyVar, valueVar, goal, outKey, outAgg) => Goal
 * Example: const groupCollecto = aggregateRel(arrayToLogicList)
 */
export function aggregateRel(aggFn: (items: any[]) => any) {
  return function (keyVar: Term, valueVar: Term, goal: Goal, outKey: Term, outAgg: Term): Goal {
    return groupAggregateo(keyVar, valueVar, goal, outKey, outAgg, aggFn);
  };
}

export const groupCollecto = aggregateRel(arrayToLogicList);
export const groupCounto = aggregateRel((items: any[]) => items.length);

/**
 * collecto(x, goal, xs): xs is the list of all values x can take under goal (logic relation version)
 * Usage: collecto(x, membero(x, ...), xs)
 */
export function collecto(x: Term, goal: Goal, xs: Term): Goal {
  return async function* (s: Subst) {
    // Collect all values of x under goal
    const results: Term[] = [];
    for await (const s1 of goal(s)) {
      results.push(await walk(x, s1));
    }
    // Convert results to a logic list
    let logicList: LogicList = nil;
    for (let i = results.length - 1; i >= 0; --i) {
      logicList = cons(results[i], logicList);
    }
    // Unify xs with the collected list
    yield* eq(xs, logicList)(s);
  };
}

/**
 * distincto(x, goal, xs): xs is the list of distinct values of x under goal.
 * Usage: distincto(x, goal, xs)
 */
export function distincto(x: Term, goal: Goal, xs: Term): Goal {
  return async function* (s: Subst) {
    const seen = new Set<any>();
    const results: Term[] = [];
    for await (const s1 of goal(s)) {
      const val = await walk(x, s1);
      const key = JSON.stringify(val);
      if (!seen.has(key)) {
        seen.add(key);
        results.push(val);
      }
    }
    let logicList: LogicList = nil;
    for (let i = results.length - 1; i >= 0; --i) {
      logicList = cons(results[i], logicList);
    }
    yield* eq(xs, logicList)(s);
  };
}

/**
 * counto(x, goal, n): n is the number of (distinct) values of x under goal.
 * Usage: counto(x, goal, n)
 */
export function counto(x: Term, goal: Goal, n: Term): Goal {
  return async function* (s: Subst) {
    let count = 0;
    for await (const s1 of goal(s)) {
      count++;
    }
    yield* eq(n, count)(s);
  };
}
