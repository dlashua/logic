/**
 * Base functions for building aggregation operations.
 * 
 * These are foundational building blocks intended for creating new aggregation
 * relations, not for direct use by end users. They handle the low-level
 * subscription, buffering, grouping, and cleanup patterns that most
 * aggregation functions need.
 * 
 * Functions ending in _base are infrastructure - use the public aggregation
 * functions in aggregates.ts instead.
 */

import type { Subst, Term, Goal, Observable } from "../core/types.ts"
import { walk, unify } from "../core/kernel.ts";
import { SimpleObservable } from "../core/observable.ts";

/**
 * Helper: collect all substitutions from a stream, then process them all at once.
 * Handles subscription, buffering, cleanup, and error management.
 * This is a foundational building block for aggregation functions that need
 * to see all data before processing (like sorting).
 * 
 * @param processor - Function that receives all buffered substitutions and observer to emit results
 */
export function collect_and_process_base(
  processor: (buffer: Subst[], observer: { next: (s: Subst) => void }) => void
): Goal {
  return (input$: Observable<Subst>) =>
    new SimpleObservable<Subst>((observer) => {
      const buffer: Subst[] = [];
      
      const subscription = input$.subscribe({
        next: (item) => buffer.push(item),
        error: (error) => {
          buffer.length = 0;
          observer.error?.(error);
        },
        complete: () => {
          processor(buffer, observer);
          buffer.length = 0;
          observer.complete?.();
        },
      });
      
      return () => {
        subscription.unsubscribe?.();
        buffer.length = 0;
      };
    });
}

/**
 * Generic stream-based grouping function - the foundation for all group_by_*_streamo functions.
 * Groups substitutions by keyVar and applies an aggregator function to each group.
 * This is a foundational building block for all grouping operations.
 * 
 * @param keyVar - Variable to group by
 * @param valueVar - Variable to extract values from (null for count-only operations)
 * @param outVar - Variable to bind the aggregated result to
 * @param drop - If true, create fresh substitutions; if false, preserve original variables
 * @param aggregator - Function that takes (values, substitutions) and returns aggregated result
 */
export function group_by_streamo_base(
  keyVar: Term,
  valueVar: Term | null,
  outVar: Term,
  drop: boolean,
  aggregator: (values: any[], substitutions: Subst[]) => any
): Goal {
  return (input$: Observable<Subst>) =>
    new SimpleObservable<Subst>((observer) => {
      // Shared grouping logic - collect all substitutions by key
      const groups = new Map<string, { key: any, values: any[], substitutions: Subst[] }>();
      
      const subscription = input$.subscribe({
        next: (s) => {
          const key = walk(keyVar, s);
          const keyStr = JSON.stringify(key);
          
          if (!groups.has(keyStr)) {
            groups.set(keyStr, {
              key,
              values: [],
              substitutions: []
            });
          }
          const group = groups.get(keyStr)!;
          if (valueVar !== null) {
            const value = walk(valueVar, s);
            group.values.push(value);
          }
          group.substitutions.push(s);
        },
        error: (error) => {
          groups.clear();
          observer.error?.(error);
        },
        complete: () => {
          // Different output generation based on drop parameter
          if (drop) {
            // Drop mode: emit one fresh substitution per group
            for (const { key, values, substitutions } of groups.values()) {
              const aggregated = aggregator(values, substitutions);
              const subst = new Map();
              const subst1 = unify(keyVar, key, subst);
              if (subst1 === null) continue;
              const subst2 = unify(outVar, aggregated, subst1);
              if (subst2 === null) continue;
              observer.next(subst2 as Subst);
            }
          } else {
            // Preserve mode: emit all substitutions with aggregated result added
            for (const { key, values, substitutions } of groups.values()) {
              const aggregated = aggregator(values, substitutions);
              for (const subst of substitutions) {
                const subst1 = unify(keyVar, key, subst);
                if (subst1 === null) continue;
                const subst2 = unify(outVar, aggregated, subst1);
                if (subst2 === null) continue;
                observer.next(subst2 as Subst);
              }
            }
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
}
