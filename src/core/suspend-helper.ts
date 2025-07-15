import { Term, Subst, Goal , Var } from "./types.ts";
import { SimpleObservable } from "./observable.ts";
import { isVar, walk } from "./kernel.ts";
import { addSuspendToSubst } from "./subst-suspends.ts";

export const CHECK_LATER = Symbol.for("constraint-check-later");

/**
 * Generic constraint helper that handles suspension automatically
 */



export function suspendable<T extends Term[]>(
  vars: T,
  evaluator: (values: Term[], subst: Subst) => Subst | null | typeof CHECK_LATER,
  minGrounded = vars.length - 1
): Goal {
  function handleSuspend(subst: Subst): Subst | null {
    const values = vars.map(v => walk(v, subst));
    const groundedCount = values.filter(v => !isVar(v)).length;

    if (groundedCount >= minGrounded) {
      const result = evaluator(values, subst);
      if (result === null) {
        return null;
      }
      if (result !== CHECK_LATER) {
        return result;
      }
    }

    // Only suspend if there are variables to watch
    const watchedVars: string[] = values
      .filter(v => isVar(v))
      .map(v => (v as Var).id); // Type-safe access to Var.id
    if (watchedVars.length > 0) {
      return addSuspendToSubst(subst, handleSuspend, watchedVars);
    }
    return null; // No variables to watch and CHECK_LATER returned, fail
  }

  return (input$: SimpleObservable<Subst>) => new SimpleObservable<Subst>((observer) => {
    const sub = input$.subscribe({
      next: (subst) => {
        try {
          const result = handleSuspend(subst);
          if (result !== null) {
            observer.next(result);
          }
        } catch (error) {
          observer.error?.(error);
        }
      },
      error: observer.error,
      complete: observer.complete,
    });

    return () => sub.unsubscribe();
  });
}

export function old_suspendable<T extends Term[]>(
  vars: T,
  evaluator: (values: any[], subst: Subst) => Subst | null | typeof CHECK_LATER,
  minGrounded = vars.length - 1
): Goal {
  function handleSuspend (subst: Subst): Subst | null {
    const values = vars.map(v => walk(v, subst));
    const groundedCount = values.filter(v => !isVar(v)).length;
    
    if (groundedCount >= minGrounded) {
      const result = evaluator(values, subst);
      if(result === null) {
        return null
      };
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

