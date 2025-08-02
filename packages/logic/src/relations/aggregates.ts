import type { Observable } from "@codespiral/observable";
import { SimpleObservable } from "@codespiral/observable";
import { eq } from "../core/combinators.js";
import { arrayToLogicList, unify, walk } from "../core/kernel.js";
import type { Goal, Subst, Term } from "../core/types.ts";
import {
  collect_and_process_base,
  group_by_streamo_base,
} from "./aggregates-base.js";

/**
 * count_value_streamo(x, value, count):
 *   count is the number of times x == value in the current stream of substitutions.
 *   (Stream-based version: aggregates over the current stream, like maxo/mino.)
 *
 * Usage: count_value_streamo(x, value, count)
 */
export function count_value_streamo(x: Term, value: Term, count: Term): Goal {
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

          eq(
            count,
            n,
          )(SimpleObservable.of(new Map())).subscribe({
            next: (v) => observer.next(v),
            error: (e: Error) => observer.error(e),
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
  drop = false,
): Goal {
  return group_by_streamo_base(
    x, // keyVar
    null, // valueVar (not needed for counting)
    count, // outVar
    drop, // drop
    (_, substitutions) => substitutions.length, // aggregator: count substitutions
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
  orderOrFn?: "asc" | "desc" | ((a: any, b: any) => number),
): Goal {
  return collect_and_process_base(
    (buffer: Subst[], observer: { next: (s: Subst) => void }) => {
      // Extract values and create sortable pairs
      const pairs = buffer.map((subst) => ({
        value: walk(x, subst),
        subst,
      }));

      // Create comparator
      const orderFn = (() => {
        if (typeof orderOrFn === "function") {
          return orderOrFn;
        }
        if (typeof orderOrFn === "string") {
          if (orderOrFn === "desc") {
            return descComparator;
          }
        }
        return ascComparator;
      })();

      const comparator = (a: { value: any }, b: { value: any }) =>
        orderFn(a.value, b.value);

      // Sort and emit
      pairs.sort(comparator);
      for (const { subst } of pairs) {
        observer.next(subst);
      }
    },
  );
}

const descComparator = <T>(a: T, b: T) => {
  if (a < b) return 1;
  if (a > b) return -1;
  return 0;
};

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
        error: (e: Error) => observer.error(e),
        complete: () => observer.complete(),
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
export function group_by_collect_streamo<T>(
  keyVar: Term,
  valueVar: Term<T>,
  outList: Term<T[]>,
  drop = false,
): Goal {
  return group_by_streamo_base(
    keyVar, // keyVar
    valueVar, // valueVar
    outList, // outVar
    drop, // drop
    (values, _) => arrayToLogicList(values), // aggregator: collect into list
  );
}

export function group_by_collect_distinct_streamo<T>(
  keyVar: Term,
  valueVar: Term<T>,
  outList: Term<T[]>,
  drop = false,
): Goal {
  return group_by_streamo_base(
    keyVar, // keyVar
    valueVar, // valueVar
    outList, // outVar
    drop, // drop
    (values, _) => arrayToLogicList([...new Set(values)]), // aggregator: collect into list
  );
}

export function collect_streamo(
  valueVar: Term,
  outList: Term,
  drop = false,
): Goal {
  return collect_and_process_base((buffer, observer) => {
    const results = buffer.map((x) => walk(valueVar, x));
    let s;
    if (drop) {
      s = new Map() as Subst;
    } else {
      s = buffer[0] ?? new Map();
    }
    const newSubst = unify(results, outList, s);
    if (newSubst) {
      observer.next(newSubst);
    }
  });
}

// export function collect_distinct_streamo(
//   valueVar: Term,
//   outList: Term,
//   drop = false,
// ): Goal {
//   return collect_and_process_base(
//     (buffer, observer) => {
//       const resultsRaw = buffer.map(x => walk(valueVar, x));
//       const results = [...new Set(resultsRaw)];
//       let s;
//       if(drop) {
//         s = new Map() as Subst;
//       } else {
//         s = buffer[0];
//       }
//       const newSubst = unify(results, outList, s);
//       if(newSubst) {
//         observer.next(newSubst);
//       }
//     }
//   )
// }
