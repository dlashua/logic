import { Goal, Subst, Term } from "../core/types.ts";
import { walk, unify, isVar } from "../core/kernel.ts";
import { SimpleObservable } from "../core/observable.ts";
import { suspendable, CHECK_LATER } from "../core/suspend-helper.ts";

/**
 * A goal that succeeds if the numeric value in the first term is greater than
 * the numeric value in the second term.
 */
export function gto(x: Term, y: Term): Goal {
  return (input$: SimpleObservable<Subst>) => 
    input$.flatMap((s: Subst) => new SimpleObservable<Subst>((observer) => {
      const xWalked = walk(x, s);
      const yWalked = walk(y, s);
      
      // Both must be grounded to numeric values
      if (typeof xWalked === 'number' && typeof yWalked === 'number') {
        if (xWalked > yWalked) {
          observer.next(s);
        }
      }
      // If either is ungrounded, this constraint cannot be satisfied
      observer.complete?.();
    }));
}

/**
 * A goal that succeeds if the numeric value in the first term is less than
 * the numeric value in the second term.
 */
export function lto(x: Term, y: Term): Goal {
  return (input$: SimpleObservable<Subst>) => 
    input$.flatMap((s: Subst) => new SimpleObservable<Subst>((observer) => {
      const xWalked = walk(x, s);
      const yWalked = walk(y, s);
      
      // Both must be grounded to numeric values
      if (typeof xWalked === 'number' && typeof yWalked === 'number') {
        if (xWalked < yWalked) {
          observer.next(s);
        }
      }
      // If either is ungrounded, this constraint cannot be satisfied
      observer.complete?.();
    }));
}

/**
 * A goal that succeeds if the numeric value in the first term is greater than or equal to
 * the numeric value in the second term.
 */
export function gteo(x: Term, y: Term): Goal {
  return (input$: SimpleObservable<Subst>) => 
    input$.flatMap((s: Subst) => new SimpleObservable<Subst>((observer) => {
      const xWalked = walk(x, s);
      const yWalked = walk(y, s);
      
      // Both must be grounded to numeric values
      if (typeof xWalked === 'number' && typeof yWalked === 'number') {
        if (xWalked >= yWalked) {
          observer.next(s);
        }
      }
      // If either is ungrounded, this constraint cannot be satisfied
      observer.complete?.();
    }));
}

/**
 * A goal that succeeds if the numeric value in the first term is less than or equal to
 * the numeric value in the second term.
 */
export function lteo(x: Term, y: Term): Goal {
  return (input$: SimpleObservable<Subst>) => 
    input$.flatMap((s: Subst) => new SimpleObservable<Subst>((observer) => {
      const xWalked = walk(x, s);
      const yWalked = walk(y, s);
      
      // Both must be grounded to numeric values
      if (typeof xWalked === 'number' && typeof yWalked === 'number') {
        if (xWalked <= yWalked) {
          observer.next(s);
        }
      }
      // If either is ungrounded, this constraint cannot be satisfied
      observer.complete?.();
    }));
}

/**
 * A goal that succeeds if z is the sum of x and y.
 * Can work in multiple directions if some variables are grounded.
 */
export function pluso(x: Term<number>, y: Term<number>, z: Term<number>): Goal {
  return suspendable([x, y, z], (values, subst) => {
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
    
    return CHECK_LATER; // Still not enough variables bound
  });
}
export const minuso = (x: Term<number>, y: Term<number>, z: Term<number>): Goal => pluso(z, y, x);

/**
 * A goal that succeeds if z is the product of x and y.
 * Can work in multiple directions if some variables are grounded.
 */
export function multo(x: Term<number>, y: Term<number>, z: Term<number>): Goal {
  return suspendable([x, y, z], (values, subst) => {
    const [xVal, yVal, zVal] = values;
    const xGrounded = !isVar(xVal);
    const yGrounded = !isVar(yVal);
    const zGrounded = !isVar(zVal);
    
    if (xGrounded && yGrounded && zGrounded) {
      return (xVal * yVal === zVal) ? subst : null;
    }
    if (xGrounded && yGrounded) {
      return unify(z, xVal * yVal, subst);
    }
    if(zGrounded && zVal !== 0) {
      if (xGrounded && xVal === 0) return null;
      if (yGrounded && yVal === 0) return null;
    }
    if (xGrounded && zGrounded) {
      return unify(y, zVal / xVal, subst);
    }
    else if (yGrounded && zGrounded) {
      return unify(x, zVal / yVal, subst);
    }
    
    return CHECK_LATER; // Still not enough variables bound
  });
}
export const dividebyo = (x: Term<number>, y: Term<number>, z: Term<number>): Goal => multo(z, y, x);


/**
 * A goal that succeeds only for the substitution(s) that have the maximum value
 * for the given variable across all input substitutions.
 * 
 * Usage: maxo($.movie_popularity) - selects the substitution with highest movie_popularity
 */
export function maxo(variable: Term): Goal {
  return (input$: SimpleObservable<Subst>) => 
    new SimpleObservable<Subst>((observer) => {
      const substitutions: Subst[] = [];
      
      // First, collect all substitutions
      const subscription = input$.subscribe({
        next: (s) => {
          substitutions.push(s);
        },
        error: observer.error,
        complete: () => {
          if (substitutions.length === 0) {
            observer.complete?.();
            return;
          }
          
          // Find the maximum value and corresponding substitutions
          let maxValue: number | undefined = undefined;
          const maxSubstitutions: Subst[] = [];
          
          for (const s of substitutions) {
            const value = walk(variable, s);
            if (typeof value === 'number') {
              if (maxValue === undefined || value > maxValue) {
                maxValue = value;
                maxSubstitutions.length = 0; // Clear array
                maxSubstitutions.push(s);
              } else if (value === maxValue) {
                maxSubstitutions.push(s);
              }
            }
          }
          
          // Emit all substitutions that have the maximum value
          for (const s of maxSubstitutions) {
            observer.next(s);
          }
          
          observer.complete?.();
        }
      });
      
      return () => subscription.unsubscribe?.();
    });
}

/**
 * A goal that succeeds only for the substitution(s) that have the minimum value
 * for the given variable across all input substitutions.
 * 
 * Usage: mino($.movie_popularity) - selects the substitution with lowest movie_popularity
 */
export function mino(variable: Term): Goal {
  return (input$: SimpleObservable<Subst>) => 
    new SimpleObservable<Subst>((observer) => {
      const substitutions: Subst[] = [];
      
      // First, collect all substitutions
      const subscription = input$.subscribe({
        next: (s) => {
          substitutions.push(s);
        },
        error: observer.error,
        complete: () => {
          if (substitutions.length === 0) {
            observer.complete?.();
            return;
          }
          
          // Find the minimum value and corresponding substitutions
          let minValue: number | undefined = undefined;
          const minSubstitutions: Subst[] = [];
          
          for (const s of substitutions) {
            const value = walk(variable, s);
            if (typeof value === 'number') {
              if (minValue === undefined || value < minValue) {
                minValue = value;
                minSubstitutions.length = 0; // Clear array
                minSubstitutions.push(s);
              } else if (value === minValue) {
                minSubstitutions.push(s);
              }
            }
          }
          
          // Emit all substitutions that have the minimum value
          for (const s of minSubstitutions) {
            observer.next(s);
          }
          
          observer.complete?.();
        }
      });
      
      return () => subscription.unsubscribe?.();
    });
}