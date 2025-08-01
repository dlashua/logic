import { SimpleObservable } from "@swiftfall/observable";
import { isVar, walk } from "./kernel.js";
import { addSuspendToSubst } from "./subst-suspends.js";
import type { Goal, Subst, Term, Var } from "./types.js";

export const CHECK_LATER = Symbol.for("constraint-check-later");

/**
 * Generic constraint helper that handles suspension automatically
 */

export function makeSuspendHandler(
  vars: Term[],
  evaluator: (
    values: Term[],
    subst: Subst,
  ) => Subst | null | typeof CHECK_LATER,
  minGrounded: number,
) {
  return function handleSuspend(subst: Subst): Subst | null {
    const values = vars.map((v) => walk(v, subst));
    const groundedCount = values.filter((v) => !isVar(v)).length;

    if (groundedCount >= minGrounded) {
      const result = evaluator(values, subst);
      if (result === null) {
        return null;
      }
      if (result !== CHECK_LATER) {
        return result;
      }
      // If we get here, result === CHECK_LATER, so fall through to suspension logic
    }

    // Only suspend if there are variables to watch
    const watchedVars: string[] = vars
      .filter((v) => isVar(v))
      .map((v) => (v as Var).id); // Type-safe access to Var.id
    if (watchedVars.length > 0) {
      return addSuspendToSubst(subst, handleSuspend, watchedVars);
    }
    return null; // No variables to watch and CHECK_LATER returned, fail
  };
}

export function suspendable<T extends Term[]>(
  vars: T,
  evaluator: (
    values: Term[],
    subst: Subst,
  ) => Subst | null | typeof CHECK_LATER,
  minGrounded = vars.length - 1,
): Goal {
  const handleSuspend = makeSuspendHandler(vars, evaluator, minGrounded);
  // console.log("VARS", vars);
  return (input$: SimpleObservable<Subst>) =>
    new SimpleObservable<Subst>((observer) => {
      const sub = input$.subscribe({
        next: (subst) => {
          try {
            const result = handleSuspend(subst);
            if (result !== null) {
              observer.next(result);
              return;
            }
            // console.log("SUSPEND DIED");
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
