import { Goal, Subst, Term } from "../core/types.ts";
import { walk, unify } from "../core/kernel.ts";
import { SimpleObservable } from "../core/observable.ts";

/**
 * A goal that succeeds if the numeric value in the first term is greater than
 * the numeric value in the second term.
 */
export function gto(x: Term, y: Term): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
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
  });
}

/**
 * A goal that succeeds if the numeric value in the first term is less than
 * the numeric value in the second term.
 */
export function lto(x: Term, y: Term): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
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
  });
}

/**
 * A goal that succeeds if the numeric value in the first term is greater than or equal to
 * the numeric value in the second term.
 */
export function gteo(x: Term, y: Term): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
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
  });
}

/**
 * A goal that succeeds if the numeric value in the first term is less than or equal to
 * the numeric value in the second term.
 */
export function lteo(x: Term, y: Term): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
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
  });
}

/**
 * A goal that succeeds if z is the sum of x and y.
 * Can work in multiple directions if some variables are grounded.
 */
export function pluso(x: Term, y: Term, z: Term): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
    const xWalked = walk(x, s);
    const yWalked = walk(y, s);
    const zWalked = walk(z, s);
    
    const xNum = typeof xWalked === 'number';
    const yNum = typeof yWalked === 'number';
    const zNum = typeof zWalked === 'number';
    
    // All three grounded - check constraint
    if (xNum && yNum && zNum) {
      if ((xWalked as number) + (yWalked as number) === (zWalked as number)) {
        observer.next(s);
      }
    }
    // Two grounded - compute the third
    else if (xNum && yNum) {
      const result = (xWalked as number) + (yWalked as number);
      const unified = unify(z, result, s);
      if (unified !== null) {
        observer.next(unified);
      }
    }
    else if (xNum && zNum) {
      const result = (zWalked as number) - (xWalked as number);
      const unified = unify(y, result, s);
      if (unified !== null) {
        observer.next(unified);
      }
    }
    else if (yNum && zNum) {
      const result = (zWalked as number) - (yWalked as number);
      const unified = unify(x, result, s);
      if (unified !== null) {
        observer.next(unified);
      }
    }
    // Less than two grounded - cannot proceed
    
    observer.complete?.();
  });
}

/**
 * A goal that succeeds if z is the product of x and y.
 * Can work in multiple directions if some variables are grounded.
 */
export function multo(x: Term, y: Term, z: Term): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
    const xWalked = walk(x, s);
    const yWalked = walk(y, s);
    const zWalked = walk(z, s);
    
    const xNum = typeof xWalked === 'number';
    const yNum = typeof yWalked === 'number';
    const zNum = typeof zWalked === 'number';
    
    // All three grounded - check constraint
    if (xNum && yNum && zNum) {
      if ((xWalked as number) * (yWalked as number) === (zWalked as number)) {
        observer.next(s);
      }
    }
    // Two grounded - compute the third
    else if (xNum && yNum) {
      const result = (xWalked as number) * (yWalked as number);
      const unified = unify(z, result, s);
      if (unified !== null) {
        observer.next(unified);
      }
    }
    else if (xNum && zNum && (xWalked as number) !== 0) {
      const result = (zWalked as number) / (xWalked as number);
      if (Number.isInteger(result)) {
        const unified = unify(y, result, s);
        if (unified !== null) {
          observer.next(unified);
        }
      }
    }
    else if (yNum && zNum && (yWalked as number) !== 0) {
      const result = (zWalked as number) / (yWalked as number);
      if (Number.isInteger(result)) {
        const unified = unify(x, result, s);
        if (unified !== null) {
          observer.next(unified);
        }
      }
    }
    // Less than two grounded - cannot proceed
    
    observer.complete?.();
  });
}