import type {
  Subst,
  Term,
  Goal,
  Var,
  Observable
} from "../core/types.ts"
import { walk, arrayToLogicList } from "../core/kernel.ts";
import { eq } from "../core/combinators.ts";
import { SimpleObservable } from "../core/observable.ts";

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
export function groupByGoal(
  keyVar: Term,
  valueVar: Term,
  goal: Goal,
  s: Subst,
  cb: (s: Subst, key: any, items: any[]) => SimpleObservable<Subst>,
): SimpleObservable<Subst> {
  return new SimpleObservable<Subst>((observer) => {
    // Collect all (key, value) pairs
    const pairs: { key: any; value: any }[] = [];
    
    goal(s).subscribe({
      next: (s1) => {
        const key = walk(keyVar, s1);
        const value = walk(valueVar, s1);
        pairs.push({
          key,
          value 
        });
      },
      complete: () => {
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
        let completedGroups = 0;
        const totalGroups = grouped.size;
        
        if (totalGroups === 0) {
          observer.complete?.();
          return;
        }
        
        for (const { key, items } of grouped.values()) {
          cb(s, key, items).subscribe({
            next: observer.next,
            error: observer.error,
            complete: () => {
              completedGroups++;
              if (completedGroups === totalGroups) {
                observer.complete?.();
              }
            }
          });
        }
      },
      error: observer.error
    });
  });
}

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
    return (s: Subst) => new SimpleObservable<Subst>((observer) => {
      const results: Term[] = [];
      
      goal(s).subscribe({
        next: (s1) => {
          const val = walk(x, s1);
          results.push(val);
        },
        complete: () => {
          const agg = aggFn(dedup ? deduplicate(results) : results);
          eq(out, agg)(s).subscribe({
            next: observer.next,
            error: observer.error,
            complete: observer.complete
          });
        },
        error: observer.error
      });
    });
  };
}

/**
 * groupAggregateRelFactory(aggFn): returns a group-by aggregation goal constructor.
 * The returned function has signature (keyVar, valueVar, goal, outKey, outAgg, dedup?) => Goal
 * Example: const group_by_collecto = groupAggregateRelFactory(arrayToLogicList)
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
    (s: Subst) => {
      return groupByGoal(
        keyVar,
        valueVar,
        goal,
        s,
        (s, key, items) => {
          const groupItems = dedup ? deduplicate(items) : items;
          const agg = aggFn(groupItems);
          return new SimpleObservable<Subst>((observer) => {
            eq(outKey, key)(s).subscribe({
              next: (s2) => {
                eq(outAgg, agg)(s2).subscribe({
                  next: observer.next,
                  error: observer.error,
                  complete: observer.complete
                });
              },
              error: observer.error,
              complete: () => {
                // If eq(outKey, key) fails, still complete
                observer.complete?.();
              }
            });
          });
        },
      );
    };
}

export const group_by_collecto = groupAggregateRelFactory(arrayToLogicList);
export const group_by_counto = groupAggregateRelFactory(
  (items: any[]) => items.length,
);

/**
 * collecto(x, goal, xs): xs is the list of all values x can take under goal (logic relation version)
 * Usage: collecto(x, membero(x, ...), xs)
 */
export const collecto = aggregateRelFactory((arr) => arrayToLogicList(arr));

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
export const counto = aggregateRelFactory((arr) => arr.length);