import { ConsNode, Goal, Subst, Term } from "../core/types.ts";
import { walk, isVar } from "../core/kernel.ts";
import { eq } from "../core/combinators.ts";
import { SimpleObservable } from "../core/observable.ts";

export const uniqueo = (t: Term, g: Goal) => 
  (s: Subst) => new SimpleObservable<Subst>((observer) => {
    const seen = new Set();
    
    g(s).subscribe({
      next: (s2) => {
        const w_t = walk(t, s2);
        if (isVar(w_t)) {
          observer.next(s2);
          return;
        }
        const key = JSON.stringify(w_t);
        if (seen.has(key)) return;
        seen.add(key);
        observer.next(s2);
      },
      error: observer.error,
      complete: observer.complete
    });
  });

export function not(goal: Goal): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
    let found = false;
    
    goal(s).subscribe({
      next: (subst) => {
        // Check if this result only added bindings that were already in the original substitution
        // If it added new variable bindings, we don't consider this a "safe" success
        let addedNewBindings = false;
        for (const [key, value] of subst) {
          if (!s.has(key)) {
            addedNewBindings = true;
            break;
          }
        }
        
        // If the goal succeeded without adding new bindings, it's a genuine success
        if (!addedNewBindings) {
          found = true;
        }
      },
      error: observer.error,
      complete: () => {
        if (!found) observer.next(s);
        observer.complete?.();
      }
    });
  });
}

export const neqo = (x: Term, y: Term) => not(eq(x, y));

/**
 * A goal that succeeds if the given goal succeeds exactly once.
 * Useful for cut-like behavior.
 */
export function onceo(goal: Goal): Goal {
  return (s: Subst) => goal(s).take(1);
}

/**
 * A goal that always succeeds with the given substitution.
 * Useful as a base case or for testing.
 */
export function succeedo(): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
    observer.next(s);
    observer.complete?.();
  });
}

/**
 * A goal that always fails.
 * Useful for testing or as a base case.
 */
export function failo(): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
    observer.complete?.();
  });
}

/**
 * A goal that succeeds if the term is ground (contains no unbound variables).
 */
export function groundo(term: Term): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
    const walked = walk(term, s);
    
    function isGround(t: Term): boolean {
      if (isVar(t)) return false;
      if (Array.isArray(t)) {
        return t.every(isGround);
      }
      if (t && typeof t === "object" && "tag" in t) {
        if (t.tag === "cons") {
          const l = t as ConsNode;
          return isGround(l.head) && isGround(l.tail);
        }
        if (t.tag === "nil") {
          return true;
        }
      }
      if (t && typeof t === "object" && !("tag" in t)) {
        return Object.values(t).every(isGround);
      }
      return true; // primitives are ground
    }
    
    if (isGround(walked)) {
      observer.next(s);
    }
    observer.complete?.();
  });
}

/**
 * A goal that succeeds if the term is not ground (contains unbound variables).
 */
export function nonGroundo(term: Term): Goal {
  return not(groundo(term));
}