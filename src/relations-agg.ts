import type { Subst, Term, Goal, Var } from "./core.ts"
import { arrayToLogicList, walk , eq } from "./core.ts";

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
  const pairs: { key: any; value: any }[] = [];
  for await (const s1 of goal(s)) {
    const key = await walk(keyVar, s1);
    const value = await walk(valueVar, s1);
    pairs.push({
      key,
      value,
    });
  }
  // Group by key (map to array of values)
  const grouped = new Map<string, { key: any; items: any[] }>();
  for (const { key, value } of pairs) {
    const k = JSON.stringify(key); // Use JSON.stringify for deep equality
    if (!grouped.has(k))
      grouped.set(k, {
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

// Helper to profile goals if profiling is enabled
function maybeProfile(goal: Goal): Goal {
  // Use the global maybeProfile from relations.ts if available
  // (imported above), otherwise fallback to identity
  return ((globalThis as any).LOGIC_PROFILING_ENABLED !== undefined)
    ? (globalThis as any).LOGIC_PROFILING_ENABLED ? (globalThis as any).wrapGoalForProfiling(goal) : goal
    : goal;
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
  return (x: Term, goal: Goal, out: Term): Goal => {
    return maybeProfile(async function* aggregateRelFactory (s: Subst) {
      const results: Term[] = [];
      for await (const s1 of goal(s)) {
        const val = await walk(x, s1);
        results.push(val);
      }
      const agg = aggFn(dedup ? deduplicate(results) : results);
      yield* eq(out, agg)(s);
    });
  };
}

/**
 * groupAggregateRelFactory(aggFn): returns a group-by aggregation goal constructor.
 * The returned function has signature (keyVar, valueVar, goal, outKey, outAgg, dedup?) => Goal
 * Example: const groupCollecto = groupAggregateRelFactory(arrayToLogicList)
 */
export function groupAggregateRelFactory(aggFn: (items: any[]) => any) {
  return (
    keyVar: Term,
    valueVar: Term,
    goal: Goal,
    outKey: Term,
    outAgg: Term,
    dedup = false,
  ): Goal =>
    async function* (s: Subst) {
      yield* groupByGoal(
        keyVar,
        valueVar,
        goal,
        s,
        async function* (s, key, items) {
          const groupItems = dedup ? deduplicate(items) : items;
          const agg = aggFn(groupItems);
          for await (const s2 of eq(outKey, key)(s)) {
            for await (const s3 of eq(outAgg, agg)(s2)) {
              yield s3;
            }
          }
        },
      );
    };
}

export const groupCollecto = groupAggregateRelFactory(arrayToLogicList);
export const groupCounto = groupAggregateRelFactory(
  (items: any[]) => items.length,
);

/**
 * collecto(x, goal, xs): xs is the list of all values x can take under goal (logic relation version)
 * Usage: collecto(x, membero(x, ...), xs)
 */
export const collecto = aggregateRelFactory((arr) => arrayToLogicList(arr));

/**
 * distincto(x, goal, xs): xs is the list of distinct values of x under goal.
 * Usage: distincto(x, goal, xs)
 */
export const distincto = aggregateRelFactory(
  (arr) => arrayToLogicList(arr),
  true,
);

/**
 * counto(x, goal, n): n is the number of (distinct) values of x under goal.
 * Usage: counto(x, goal, n)
 */
export const counto = aggregateRelFactory((arr) => arr.length);

/**
 * Aggregates all possible values of a logic variable into an array and binds to sourceVar in a single solution.
 */
export function aggregateVar(sourceVar: Var, subgoal: Goal): Goal {
  return async function* (s: Subst) {
    const results: Term[] = [];
    for await (const subst of subgoal(s)) {
      results.push(await walk(sourceVar, subst));
    }
    const s2 = new Map(s);
    s2.set(sourceVar.id, results);
    yield s2;
  };
}

/**
 * For each unique combination of groupVars, aggregate all values of each aggVar in aggVars, and yield a substitution with arrays bound to each aggVar.
 */
export function aggregateVarMulti(groupVars: Var[], aggVars: Var[], subgoal: Goal): Goal {
  return async function* (s: Subst) {
    const groupMap = new Map<string, Term[][]>();
    for await (const subst of subgoal(s)) {
      const groupKey = JSON.stringify(await Promise.all(groupVars.map(v => walk(v, subst))));
      let aggArrays = groupMap.get(groupKey);
      if (!aggArrays) {
        aggArrays = aggVars.map(() => []);
        groupMap.set(groupKey, aggArrays);
      }
      aggVars.forEach((v, i) => {
        aggArrays![i].push(walk(v, subst));
      });
    }
    if (groupMap.size === 0) {
      const s2 = new Map(s);
      aggVars.forEach((v, i) => s2.set(v.id, []));
      yield s2;
      return;
    }
    for (const [groupKey, aggArrays] of groupMap.entries()) {
      const groupValues = JSON.parse(groupKey);
      const s2 = new Map(s);
      groupVars.forEach((v, index) => s2.set(v.id, groupValues[index]));
      aggVars.forEach((v, index) => s2.set(v.id, aggArrays[index].map(async x => await x)));
      yield s2;
    }
  };
}
