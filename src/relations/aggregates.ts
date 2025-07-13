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
    return Subquery(
      goal,
      x, // extract x from each subgoal result
      out, // bind the aggregated result to this variable
      (extractedValues, _) => {
        const values = dedup ? deduplicate(extractedValues) : extractedValues;
        return aggFn(values);
      }
    );
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
    enrichGroupInput(
      "groupAggregateRelFactory",
      [],
      [goal],
      (input$: Observable<Subst>) => {
        return (input$ as SimpleObservable<Subst>).flatMap((s: Subst) => {
          // For each input substitution, run the goal and collect grouped results
          const groups = new Map<string, { key: any, values: any[] }>();
          
          return new SimpleObservable<Subst>((observer) => {
            const subscription = goal(SimpleObservable.of(s)).subscribe({
              next: (s1) => {
                const key = walk(keyVar, s1);
                const value = walk(valueVar, s1);
                const keyStr = JSON.stringify(key);
                
                if (!groups.has(keyStr)) {
                  groups.set(keyStr, {
                    key,
                    values: []
                  });
                }
                groups.get(keyStr)!.values.push(value);
              },
              error: (error) => {
                groups.clear();
                observer.error?.(error);
              },
              complete: () => {
                // For each group, emit one result with outKey and outAgg bound
                for (const { key, values } of groups.values()) {
                  const groupItems = dedup ? deduplicate(values) : values;
                  const agg = aggFn(groupItems);
                  
                  // Create a fresh substitution with just outKey and outAgg
                  const subst = new Map();
                  const subst1 = unify(outKey, key, subst);
                  if (subst1 === null) continue;
                  const subst2 = unify(outAgg, agg, subst1);
                  if (subst2 === null) continue;
                  observer.next(subst2 as Subst);
                }
                groups.clear();
                observer.complete?.();
              },
            });
            
            return () => {
              subscription.unsubscribe?.();
              groups.clear();
            };
          });
        });
      }
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
      const orderFn = (() => {
        if(typeof orderOrFn === "function") {
          return orderOrFn;
        }
        if(typeof orderOrFn === "string") {
          if (orderOrFn === "desc") {
            return descComparator;
          } 
        } 
        return ascComparator;
      })();

      const comparator = (a: {value: any}, b: {value: any}) => orderFn(a.value, b.value);
      
      // Sort and emit
      pairs.sort(comparator);
      for (const { subst } of pairs) {
        observer.next(subst);
      }
    }
  );
}

const descComparator = <T>(a: T, b: T) => {
  if (a < b) return 1;
  if (a > b) return -1;
  return 0;
}

const ascComparator = <T>(a: T, b: T) => {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
};

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





