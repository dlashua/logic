import util from "node:util";
import { K } from "vitest/dist/chunks/reporters.d.BFLkQcL6.js";
import { ConsNode, Goal, Subst, Term } from "../core/types.ts"
import { walk, isVar, enrichGroupInput } from "../core/kernel.ts";
import { eq } from "../core/combinators.ts";
import { SimpleObservable } from "../core/observable.ts";

export const uniqueo = (t: Term, g: Goal): Goal =>
  enrichGroupInput("uniqueo", [g], [],
    (input$: SimpleObservable<Subst>) => input$.flatMap((s: Subst) => {
      const seen = new Set();
      return g(SimpleObservable.of(s)).flatMap((s2: Subst) => {
        const w_t = walk(t, s2);
        if (isVar(w_t)) {
          return SimpleObservable.of(s2);
        }
        const key = JSON.stringify(w_t);
        if (seen.has(key)) return SimpleObservable.empty();
        seen.add(key);
        return SimpleObservable.of(s2);
      });
    }));

export function not(goal: Goal): Goal {
  return enrichGroupInput("not", [], [goal], (input$: SimpleObservable<Subst>) =>
    input$.flatMap((s: Subst) => {
      let found = false;
      return new SimpleObservable<Subst>((observer) => {
        goal(SimpleObservable.of(s)).subscribe({
          next: (subst) => {
            let addedNewBindings = false;
            for (const [key, value] of subst) {
              if (!s.has(key)) {
                addedNewBindings = true;
                break;
              }
            }
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
    })
  );
}

export const neqo = (x: Term, y: Term): Goal => not(eq(x, y));

/**
 * A goal that succeeds if the given goal succeeds exactly once.
 * Useful for cut-like behavior.
 */
export function onceo(goal: Goal): Goal {
  return (input$: SimpleObservable<Subst>) => goal(input$).take(1);
}

/**
 * A goal that always succeeds with the given substitution.
 * Useful as a base case or for testing.
 */
export function succeedo(): Goal {
  return (input$: SimpleObservable<Subst>) => input$.flatMap((s: Subst) =>
    new SimpleObservable<Subst>((observer) => {
      observer.next(s);
      observer.complete?.();
    })
  );
}

/**
 * A goal that always fails.
 * Useful for testing or as a base case.
 */
export function failo(): Goal {
  return (_input$: SimpleObservable<Subst>) => SimpleObservable.empty<Subst>();
}

/**
 * A goal that succeeds if the term is ground (contains no unbound variables).
 */
export function groundo(term: Term): Goal {
  return (input$: SimpleObservable<Subst>) => input$.flatMap((s: Subst) =>
    new SimpleObservable<Subst>((observer) => {
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
    })
  );
}

/**
 * A goal that succeeds if the term is not ground (contains unbound variables).
 */
export function nonGroundo(term: Term): Goal {
  return not(groundo(term));
}

/**
 * A goal that logs each substitution it sees along with a message.
 */
export function substLog(msg: string, onlyVars = false): Goal {
  return enrichGroupInput("substLog", [], [], (input$: SimpleObservable<Subst>) =>
    new SimpleObservable<Subst>((observer) => {
      const sub = input$.subscribe({
        next: (s) => {
          const ns = onlyVars 
            ? Object.fromEntries([...s.entries()].filter(([k,v]) => typeof k === "string"))
            : s;
          console.log(`[substLog] ${msg}:`, util.inspect(ns, {
            depth: null,
            colors: true
          }));
          observer.next(s);
        },
        error: observer.error,
        complete: observer.complete,
      });
      return () => sub.unsubscribe();
    }));
}

export function fail(): Goal {
  return (input$: SimpleObservable<Subst>) =>
    new SimpleObservable<Subst>((observer) => {
      const sub = input$.subscribe({
        next: (s) => {

          /* pass */
        },
        error: observer.error,
        complete: observer.complete,
      });
      return () => sub.unsubscribe();
    });
}