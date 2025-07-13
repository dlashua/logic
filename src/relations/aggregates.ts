import type {
  Subst,
  Term,
  Goal,
  Var,
  Observable
} from "../core/types.ts"
import { walk, arrayToLogicList, enrichGroupInput, unify } from "../core/kernel.ts";
import { eq, Subquery } from "../core/combinators.ts";
import { SimpleObservable } from "../core/observable.ts";
import { collect_and_process_base, group_by_streamo_base } from "./aggregates-base.ts";

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
      
      const subscription = toSimple(goal(SimpleObservable.of(s))).subscribe({
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
            // Clean up on early completion
            pairs.length = 0;
            grouped.clear();
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
                  // Clean up after all groups completed
                  pairs.length = 0;
                  grouped.clear();
                  observer.complete?.();
                }
              }
            });
          }
        },
        error: (error) => {
          // Clean up on error
          pairs.length = 0;
          observer.error?.(error);
        }
      });
      
      // Return cleanup function to handle early unsubscription
      return () => {
        subscription.unsubscribe?.();
        pairs.length = 0; // Clean up pairs on unsubscribe
      };
    })
  );
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
  // @ts-expect-error
  const name = aggFn.name || aggFn.displayName || "aggregateRelFactory";
  return (x: Term, goal: Goal, out: Term): Goal => {
    return enrichGroupInput(name, [], [goal], (input$) =>
      toSimple(input$).flatMap((s: Subst) =>
        new SimpleObservable<Subst>((observer) => {
          const results: Term[] = [];
          
          const subscription = toSimple(goal(SimpleObservable.of(s))).subscribe({
            next: (s1) => {
              const val = walk(x, s1);
              results.push(val);
            },
            complete: () => {
              const agg = aggFn(dedup ? deduplicate(results) : results);
              toSimple(eq(out, agg)(SimpleObservable.of(s))).subscribe({
                next: observer.next,
                error: observer.error,
                complete: () => {
                  // Clean up results after processing
                  results.length = 0;
                  observer.complete?.();
                }
              });
            },
            error: (error) => {
              // Clean up results on error
              results.length = 0;
              observer.error?.(error);
            }
          });
          
          // Return cleanup function to handle early unsubscription
          return () => {
            subscription.unsubscribe?.();
            results.length = 0; // Clean up results on unsubscribe
          };
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
 *   
 *   This is implemented using Subquery with a custom aggregator that counts
 *   how many times the extracted value equals the target value (walked in context).
 */
export function count_valueo(
  x: Term,
  goal: Goal,
  value: Term,
  count: Term
): Goal {
  return Subquery(
    goal,
    x, // extract x from each subgoal result
    count, // bind the count to this variable
    (extractedValues, originalSubst) => {
      // Walk the value in the original substitution context
      const targetValue = walk(value, originalSubst);
      // Count how many extracted values match the target value
      return extractedValues.filter(val => 
        JSON.stringify(val) === JSON.stringify(targetValue)
      ).length;
    }
  );
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
      
      const subscription = input$.subscribe({
        next: (s) => substitutions.push(s),
        error: (error) => {
          // Clean up substitutions on error
          substitutions.length = 0;
          observer.error?.(error);
        },
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
            complete: () => {
              // Clean up substitutions after processing
              substitutions.length = 0;
              observer.complete?.();
            },
          });
        },
      });
      
      // Return cleanup function to handle early unsubscription
      return () => {
        subscription.unsubscribe?.();
        substitutions.length = 0; // Clean up substitutions on unsubscribe
      };
    });
}

/**
 * group_by_count_streamo(x, count, drop?):
 *   Groups the input stream by values of x and counts each group.
 *   - If drop=false (default): Preserves all variables from original substitutions,
 *     emitting one result for EACH substitution in each group with the count added.
 *   - If drop=true: Creates fresh substitutions with ONLY x and count variables.
 *   Example: if stream is x=A,y=1; x=A,y=2; x=B,y=3
 *   - drop=false: emits x=A,y=1,count=2; x=A,y=2,count=2; x=B,y=3,count=1
 *   - drop=true: emits x=A,count=2; x=B,count=1
 */
export function group_by_count_streamo(
  x: Term,
  count: Term,
  drop = false
): Goal {
  return group_by_streamo_base(
    x, // keyVar
    null, // valueVar (not needed for counting)
    count, // outVar
    drop, // drop
    (_, substitutions) => substitutions.length // aggregator: count substitutions
  );
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
  return collect_and_process_base(
    (buffer: Subst[], observer: { next: (s: Subst) => void }) => {
      // Extract values and create sortable pairs
      const pairs = buffer.map(subst => ({
        value: walk(x, subst),
        subst
      }));
      
      // Create comparator
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
      
      // Sort and emit
      pairs.sort(comparator);
      for (const { subst } of pairs) {
        observer.next(subst);
      }
    }
  );
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
      
      const subscription = input$.subscribe({
        next: (item) => {
          if (count < n) {
            observer.next(item);
            count++;
            if (count === n) {
              observer.complete?.();
              subscription.unsubscribe?.();
            }
          }
        },
        error: observer.error,
        complete: observer.complete,
      });
      
      return () => subscription.unsubscribe?.();
    });
}

/**
 * group_by_collect_streamo(keyVar, valueVar, outList, drop?):
 *   Groups the input stream by keyVar and collects valueVar values into lists.
 *   The keyVar is preserved in the output (no need for separate outKey parameter).
 *   - If drop=false (default): Preserves all variables from original substitutions,
 *     emitting one result for EACH substitution in each group with the collected list added.
 *   - If drop=true: Creates fresh substitutions with ONLY keyVar and outList variables.
 *   Example: if stream is x=A,y=1; x=A,y=2; x=B,y=3
 *   - drop=false: emits x=A,y=1,list=[1,2]; x=A,y=2,list=[1,2]; x=B,y=3,list=[3]
 *   - drop=true: emits x=A,list=[1,2]; x=B,list=[3]
 */
export function group_by_collect_streamo(
  keyVar: Term,
  valueVar: Term,
  outList: Term,
  drop = false
): Goal {
  return group_by_streamo_base(
    keyVar, // keyVar
    valueVar, // valueVar
    outList, // outVar
    drop, // drop
    (values, _) => arrayToLogicList(values) // aggregator: collect into list
  );
}





