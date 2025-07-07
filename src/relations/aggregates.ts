import type {
  Subst,
  Term,
  Goal,
  Var,
  Observable
} from "../core/types.ts"
import { walk, arrayToLogicList, enrichGroupInput, unify } from "../core/kernel.ts";
import { eq } from "../core/combinators.ts";
import { SimpleObservable } from "../core/observable.ts";

/**
 * @deprecated Avoid using toSimple. Use native Observable/subscribe patterns instead.
 */
function toSimple<T>(input$: Observable<T>): SimpleObservable<T> {
  return (input$ instanceof SimpleObservable)
    ? input$
    : new SimpleObservable<T>(observer => {
      const sub = input$.subscribe(observer);
      return () => sub.unsubscribe();
    });
}

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
  input$: Observable<Subst>,
  cb: (s: Subst, key: any, items: any[]) => SimpleObservable<Subst>,
): SimpleObservable<Subst> {
  return toSimple(input$).flatMap((s: Subst) =>
    new SimpleObservable<Subst>((observer) => {
      const pairs: { key: any; value: any }[] = [];
      toSimple(goal(SimpleObservable.of(s))).subscribe({
        next: (s1) => {
          const key = walk(keyVar, s1);
          const value = walk(valueVar, s1);
          pairs.push({
            key,
            value 
          });
        },
        complete: () => {
          const grouped = new Map<string, { key: any; items: any }>();
          for (const { key, value } of pairs) {
            const k = JSON.stringify(key);
            if (!grouped.has(k))
              grouped.set(k, {
                key,
                items: []
              });
            const group = grouped.get(k);
            if (group) group.items.push(value);
          }
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
    })
  );
}

let aggregateIdCounter = 0;

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
  // @ts-expect-error
  const name = aggFn.name || aggFn.displayName || "aggregateRelFactory";
  return (x: Term, goal: Goal, out: Term): Goal => {
    return enrichGroupInput(name, ++aggregateIdCounter, [], [], (input$) =>
      toSimple(input$).flatMap((s: Subst) =>
        new SimpleObservable<Subst>((observer) => {
          const results: Term[] = [];
          toSimple(goal(SimpleObservable.of(s))).subscribe({
            next: (s1) => {
              const val = walk(x, s1);
              results.push(val);
            },
            complete: () => {
              const agg = aggFn(dedup ? deduplicate(results) : results);
              toSimple(eq(out, agg)(SimpleObservable.of(s))).subscribe({
                next: observer.next,
                error: observer.error,
                complete: observer.complete
              });
            },
            error: observer.error
          });
        })
      )
    );
  }
  //   return (input$: Observable<Subst>) => toSimple(input$).flatMap((s: Subst) =>
  //     new SimpleObservable<Subst>((observer) => {
  //       const results: Term[] = [];
  //       toSimple(goal(SimpleObservable.of(s))).subscribe({
  //         next: (s1) => {
  //           const val = walk(x, s1);
  //           results.push(val);
  //         },
  //         complete: () => {
  //           const agg = aggFn(dedup ? deduplicate(results) : results);
  //           toSimple(eq(out, agg)(SimpleObservable.of(s))).subscribe({
  //             next: observer.next,
  //             error: observer.error,
  //             complete: observer.complete
  //           });
  //         },
  //         error: observer.error
  //       });
  //     })
  //   );
  // };
}

let groupAggregateIdCounter = 0;

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
    enrichGroupInput(
      "groupAggregateRelFactory",
      ++groupAggregateIdCounter,
      [],
      [goal],
      (input$: Observable<Subst>) =>
        groupByGoal(
          keyVar,
          valueVar,
          goal,
          input$,
          (s, key, items) => {
            const groupItems = dedup ? deduplicate(items) : items;
            const agg = aggFn(groupItems);
            return new SimpleObservable<Subst>((observer) => {
              toSimple(eq(outKey, key)(SimpleObservable.of(s))).subscribe({
                next: (s2) => {
                  toSimple(eq(outAgg, agg)(SimpleObservable.of(s2))).subscribe({
                    next: observer.next,
                    error: observer.error,
                    complete: observer.complete
                  });
                },
                error: observer.error,
                complete: () => {
                  observer.complete?.();
                }
              });
            });
          },
        )
    );
}

export const group_by_collecto = groupAggregateRelFactory(arrayToLogicList);
export const group_by_counto = groupAggregateRelFactory(
  (items: any[]) => items.length,
);

/**
 * collecto(x, goal, xs): xs is the list of all values x can take under goal (logic relation version)
 * Usage: collecto(x, membero(x, ...), xs)
 */
export const collecto = aggregateRelFactory(function collecto (arr) { return arrayToLogicList(arr)});

/**
 * collect_distincto(x, goal, xs): xs is the list of distinct values of x under goal.
 * Usage: collect_distincto(x, goal, xs)
 */
export const collect_distincto = aggregateRelFactory(
  function collecto_distincto (arr) { return arrayToLogicList(arr)},
  true,
);

/**
 * counto(x, goal, n): n is the number of (distinct) values of x under goal.
 * Usage: counto(x, goal, n)
 */
export const counto = aggregateRelFactory((arr) => arr.length);

/**
 * count_valueo(x, goal, value, count):
 *   count is the number of times x == value in the stream of substitutions from goal.
 *   (Canonical, goal-wrapping version: aggregates over all solutions to goal.)
 */
export function count_valueo(
  x: Term,
  goal: Goal,
  value: Term,
  count: Term
): Goal {
  return (input$: Observable<Subst>) =>
    new SimpleObservable<Subst>((observer) => {
      const inputSub = input$.subscribe({
        next: (s: Subst) => {
          // For each input substitution, run the goal, then count in that stream
          count_value_streamo(x, value, count)(goal(SimpleObservable.of(s))).subscribe({
            next: (sCount) => {
              // Merge the count result with the original substitution
              // sCount may only bind 'count', so unify with s
              const merged = new Map(s);
              for (const [k, v] of sCount) merged.set(k, v);
              observer.next(merged);
            },
            error: observer.error,
            complete: observer.complete,
          });
        },
        error: observer.error,
        complete: observer.complete,
      });
      return () => inputSub.unsubscribe();
    });
}

/**
 * count_value_streamo(x, value, count):
 *   count is the number of times x == value in the current stream of substitutions.
 *   (Stream-based version: aggregates over the current stream, like maxo/mino.)
 *
 * Usage: count_value_streamo(x, value, count)
 */
export function count_value_streamo(
  x: Term,
  value: Term,
  count: Term
): Goal {
  return (input$: Observable<Subst>) =>
    new SimpleObservable<Subst>((observer) => {
      const substitutions: Subst[] = [];
      input$.subscribe({
        next: (s) => substitutions.push(s),
        error: observer.error,
        complete: () => {
          let n = 0;
          for (const s of substitutions) {
            const val = walk(x, s);
            const target = walk(value, s);
            if (JSON.stringify(val) === JSON.stringify(target)) n++;
          }
          eq(count, n)(SimpleObservable.of(new Map())).subscribe({
            next: observer.next,
            error: observer.error,
            complete: observer.complete,
          });
        },
      });
    });
}

/**
 * group_by_count_streamo(x, count):
 *   For each unique value of x in the current stream, emit a substitution with x and its count.
 *   Example: if stream is x=A,x=A,x=B,x=A,x=B,x=C, emits x=A,count=3; x=B,count=2; x=C,count=1
 *   (Stream-based group-by-count, canonical in Datalog/logic aggregation.)
 */
export function group_by_count_streamo(
  x: Term,
  count: Term
): Goal {
  return (input$: Observable<Subst>) =>
    new SimpleObservable<Subst>((observer) => {
      // Map from value key to array of substitutions
      const valueMap = new Map<string, { value: any, substitutions: Subst[] }>();
      input$.subscribe({
        next: (s) => {
          const val = walk(x, s);
          const k = JSON.stringify(val);
          if (!valueMap.has(k)) valueMap.set(k, {
            value: val,
            substitutions: []
          });
          valueMap.get(k)!.substitutions.push(s);
        },
        error: observer.error,
        complete: () => {
          for (const { value, substitutions } of valueMap.values()) {
            const subst = new Map(substitutions[0]);
            const subst1 = unify(x, value, subst);
            if (subst1 === null) continue;
            const subst2 = unify(count, substitutions.length, subst1);
            if (subst2 === null) continue;
            observer.next(subst2 as Subst);
          }
          observer.complete?.();
        },
      });
    });
}

/**
 * sort_by_streamo(x, orderOrFn?):
 *   Sorts the stream of substitutions by the value of x.
 *   - If orderOrFn is 'asc' (default), sorts ascending.
 *   - If orderOrFn is 'desc', sorts descending.
 *   - If orderOrFn is a function (a, b) => number, uses it as the comparator on walked x values.
 *   Emits the same substitutions, but in sorted order by x.
 *   Example: if stream is x=3, x=1, x=2, emits x=1, x=2, x=3 (asc)
 */
export function sort_by_streamo(
  x: Term,
  orderOrFn?: 'asc' | 'desc' | ((a: any, b: any) => number)
): Goal {
  return (input$: Observable<Subst>) =>
    new SimpleObservable<Subst>((observer) => {
      const buffer: { value: any, subst: Subst }[] = [];
      input$.subscribe({
        next: (s) => {
          const val = walk(x, s);
          buffer.push({
            value: val,
            subst: s
          });
        },
        error: observer.error,
        complete: () => {
          let comparator: (a: { value: any }, b: { value: any }) => number;
          if (typeof orderOrFn === 'function') {
            comparator = (a, b) => orderOrFn(a.value, b.value);
          } else if (orderOrFn === 'desc') {
            comparator = (a, b) => {
              if (a.value < b.value) return 1;
              if (a.value > b.value) return -1;
              return 0;
            };
          } else { // 'asc' or undefined
            comparator = (a, b) => {
              if (a.value < b.value) return -1;
              if (a.value > b.value) return 1;
              return 0;
            };
          }
          buffer.sort(comparator);
          for (const { subst } of buffer) {
            observer.next(subst);
          }
          observer.complete?.();
        },
      });
    });
}

/**
 * take_streamo(n):
 *   Allows only the first n substitutions to pass through the stream.
 *   Example: take_streamo(3) will emit only the first 3 substitutions.
 */
export function take_streamo(n: number): Goal {
  return (input$: Observable<Subst>) =>
    new SimpleObservable<Subst>((observer) => {
      let count = 0;
      const sub = input$.subscribe({
        next: (s) => {
          if (count < n) {
            observer.next(s);
            count++;
            if (count === n) {
              observer.complete?.();
              sub.unsubscribe();
            }
          }
        },
        error: observer.error,
        complete: observer.complete,
      });
      return () => sub.unsubscribe();
    });
}