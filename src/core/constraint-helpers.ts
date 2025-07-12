import { queryUtils } from "../shared/utils.ts";
import { Term, Subst, Goal } from "./types.ts";
import { SimpleObservable } from "./observable.ts";
import { isVar , unify, walk } from "./kernel.ts";
import { addConstraintToSubst } from "./subst-constraints.ts";

/**
 * Generic constraint helper that handles suspension automatically
 */
export function constraint<T extends Term[]>(
  vars: T,
  evaluator: (values: any[], subst: Subst) => Subst | null,
  minGrounded = vars.length - 1
): Goal {
  function handleConstraint (subst: Subst): Subst | null {
    const values = vars.map(v => walk(v, subst));
    const groundedCount = values.filter(v => !isVar(v)).length;
    
    if (groundedCount >= minGrounded) {
      const result = evaluator(values, subst);
      if (result !== null) {
        return result;
      }
    }
    
    const watchedVars: string[] = [];
    for (const value of values) {
      if (isVar(value)) {
        watchedVars.push((value as any).id);
      }
    }
    return addConstraintToSubst(subst, handleConstraint, watchedVars);
  };

  return (input$: SimpleObservable<Subst>) => new SimpleObservable<Subst>((observer) => {
    const sub = input$.subscribe({
      next: (subst) => {
        const result = handleConstraint(subst);
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

/**
 * Arithmetic constraint: x + y = z
 */
export function addConstraint(x: Term<number>, y: Term<number>, z: Term<number>): Goal {
  return constraint([x, y, z], (values, subst) => {
    const [xVal, yVal, zVal] = values;
    const xGrounded = !isVar(xVal);
    const yGrounded = !isVar(yVal);
    const zGrounded = !isVar(zVal);
    
    // All grounded - check constraint
    if (xGrounded && yGrounded && zGrounded) {
      return (xVal + yVal === zVal) ? subst : null;
    }
    // Two grounded - compute third
    else if (xGrounded && yGrounded) {
      return unify(z, xVal + yVal, subst);
    }
    else if (xGrounded && zGrounded) {
      return unify(y, zVal - xVal, subst);
    }
    else if (yGrounded && zGrounded) {
      return unify(x, zVal - yVal, subst);
    }
    
    return null; // Still not enough variables bound
  });
}

/**
 * Multiplication constraint: x * y = z
 */
export function mulConstraint(x: Term<number>, y: Term<number>, z: Term<number>): Goal {
  return constraint([x, y, z], (values, subst) => {
    const [xVal, yVal, zVal] = values;
    const xGrounded = !isVar(xVal);
    const yGrounded = !isVar(yVal);
    const zGrounded = !isVar(zVal);
    
    if (xGrounded && yGrounded && zGrounded) {
      return (xVal * yVal === zVal) ? subst : null;
    }
    else if (xGrounded && yGrounded) {
      return unify(z, xVal * yVal, subst);
    }
    else if (xGrounded && zGrounded && xVal !== 0) {
      return unify(y, zVal / xVal, subst);
    }
    else if (yGrounded && zGrounded && yVal !== 0) {
      return unify(x, zVal / yVal, subst);
    }
    
    return null;
  });
}
