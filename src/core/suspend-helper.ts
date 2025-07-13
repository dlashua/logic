import { Term, Subst, Goal } from "./types.ts";
import { SimpleObservable } from "./observable.ts";
import { isVar, walk } from "./kernel.ts";
import { addSuspendToSubst } from "./subst-suspends.ts";

export const CHECK_LATER = Symbol.for("constraint-check-later");

/**
 * Generic constraint helper that handles suspension automatically
 */
export function suspendable<T extends Term[]>(
  vars: T,
  evaluator: (values: any[], subst: Subst) => Subst | null | typeof CHECK_LATER,
  minGrounded = vars.length - 1
): Goal {
  function handleSuspend (subst: Subst): Subst | null {
    const values = vars.map(v => walk(v, subst));
    const groundedCount = values.filter(v => !isVar(v)).length;
    
    if (groundedCount >= minGrounded) {
      const result = evaluator(values, subst);
      if(result === null) return null;
      if(result !== CHECK_LATER) return result;
    }
    
    const watchedVars: string[] = [];
    for (const value of values) {
      if (isVar(value)) {
        watchedVars.push((value as any).id);
      }
    }
    return addSuspendToSubst(subst, handleSuspend, watchedVars);
  };

  return (input$: SimpleObservable<Subst>) => new SimpleObservable<Subst>((observer) => {
    const sub = input$.subscribe({
      next: (subst) => {
        const result = handleSuspend(subst);
        if (result !== null) {
          observer.next(result);
        }
      },
      error: observer.error,
      complete: observer.complete,
    });

    return () => sub.unsubscribe();
  });
}

