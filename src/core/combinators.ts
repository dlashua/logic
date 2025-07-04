import {
  Goal,
  Subst,
  Term,
  Var,
  LiftedArgs,
  Observable
} from "./types.ts";
import { unify, lvar, walk, isVar } from "./kernel.ts";
import { SimpleObservable } from "./observable.ts";

/**
 * A goal that succeeds if two terms can be unified.
 */
export function eq(u: Term, v: Term): Goal {
  return (s: Subst) => {
    return new SimpleObservable<Subst>((observer) => {
      try {
        const result = unify(u, v, s);
        if (result !== null) {
          observer.next(result);
        }
        observer.complete?.();
      } catch (error) {
        observer.error?.(error);
      }
    });
  };
}

/**
 * Introduces new (fresh) logic variables into a sub-goal.
 */
export function fresh(f: (...vars: Var[]) => Goal): Goal {
  return (s: Subst) => {
    const freshVars = Array.from({
      length: f.length 
    }, () => lvar());
    const subGoal = f(...freshVars);
    return subGoal(s);
  };
}

/**
 * Logical disjunction (OR).
 */
export function disj(g1: Goal, g2: Goal): Goal {
  return (s: Subst) => {
    return g1(s).merge(g2(s));
  };
}

/**
 * Logical conjunction (AND).
 */
export function conj(g1: Goal, g2: Goal): Goal {
  return (s: Subst) => {
    return g1(s).flatMap(s1 => g2(s1));
  };
}

/**
 * Helper for combining multiple goals with logical AND.
 */
export const and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (s) => SimpleObservable.of(s);
  }
  return goals.reduce(conj);
};

/**
 * Helper for combining multiple goals with logical OR.
 */
export const or = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (s) => SimpleObservable.empty<Subst>();
  }
  return goals.reduce(disj);
};

/**
 * Multi-clause disjunction (OR).
 */
export function conde(...clauses: Goal[][]): Goal {
  const clauseGoals = clauses.map(clause => and(...clause));
  return or(...clauseGoals);
}

/**
 * Lifts a pure JavaScript function into a Goal function.
 */
export function lift<T extends (...args: any) => any>(fn: T): LiftedArgs<T> {
  return ((...args: any[]) => {
    // Extract the 'out' parameter (last argument)
    const out = args[args.length - 1];
    const inputArgs = args.slice(0, -1);
    
    return (s: Subst) => {
      return new SimpleObservable<Subst>((observer) => {
        try {
          // Walk all input arguments to resolve any variables
          const resolvedArgs = inputArgs.map(arg => walk(arg, s));
          
          // Check if all arguments are ground (no variables)
          const hasVariables = resolvedArgs.some(arg => isVar(arg));
          
          if (!hasVariables) {
            // All arguments are ground, we can call the function
            const result = fn(...resolvedArgs);
            
            // Unify the result with the output parameter
            const unified = unify(out, result, s);
            if (unified !== null) {
              observer.next(unified);
            }
          }
          // If there are variables in the input, the goal fails
          observer.complete?.();
        } catch (error) {
          // If the function throws, the goal fails silently
          observer.complete?.();
        }
      });
    };
  }) as LiftedArgs<T>;
}

/**
 * Soft-cut if-then-else combinator.
 */
export function ifte(ifGoal: Goal, thenGoal: Goal, elseGoal: Goal): Goal {
  return (s: Subst) => {
    return new SimpleObservable<Subst>((observer) => {
      const results: Subst[] = [];
      let succeeded = false;
      
      ifGoal(s).subscribe({
        next: (s1) => {
          succeeded = true;
          results.push(s1);
        },
        complete: () => {
          if (succeeded) {
            // Execute then goal for each result
            let completed = 0;
            for (const s1 of results) {
              thenGoal(s1).subscribe({
                next: observer.next,
                error: observer.error,
                complete: () => {
                  completed++;
                  if (completed === results.length) {
                    observer.complete?.();
                  }
                }
              });
            }
            if (results.length === 0) {
              observer.complete?.();
            }
          } else {
            // Execute else goal
            elseGoal(s).subscribe({
              next: observer.next,
              complete: observer.complete,
              error: observer.error
            });
          }
        },
        error: observer.error
      });
    });
  };
}

/**
 * Negation as failure - succeeds only if the goal fails
 */
export function not(goal: Goal): Goal {
  return (s: Subst) => {
    return new SimpleObservable<Subst>((observer) => {
      let succeeded = false;
      
      goal(s).subscribe({
        next: () => {
          succeeded = true;
        },
        complete: () => {
          if (!succeeded) {
            observer.next(s); // Original substitution unchanged
          }
          observer.complete?.();
        },
        error: observer.error
      });
    });
  };
}

/**
 * Succeeds exactly once with the given substitution (useful for cut-like behavior)
 */
export function once(goal: Goal): Goal {
  return (s: Subst) => {
    return goal(s).take(1);
  };
}

/**
 * Apply a goal with a timeout
 */
export function timeout(goal: Goal, timeoutMs: number): Goal {
  return (s: Subst) => {
    return new SimpleObservable<Subst>((observer) => {
      let completed = false;
      
      const timer = setTimeout(() => {
        if (!completed) {
          completed = true;
          observer.error?.(new Error(`Goal timed out after ${timeoutMs}ms`));
        }
      }, timeoutMs);
      
      const subscription = goal(s).subscribe({
        next: (result) => {
          if (!completed) {
            observer.next(result);
          }
        },
        complete: () => {
          if (!completed) {
            completed = true;
            clearTimeout(timer);
            observer.complete?.();
          }
        },
        error: (error) => {
          if (!completed) {
            completed = true;
            clearTimeout(timer);
            observer.error?.(error);
          }
        }
      });
      
      return () => {
        clearTimeout(timer);
        subscription.unsubscribe();
      };
    });
  };
}

/**
 * Run a goal and collect results with optional limits
 */
export function run<T>(
  goal: Goal, 
  maxResults?: number,
  timeoutMs?: number
): Promise<{ results: Subst[], completed: boolean, error?: any }> {
  return new Promise((resolve) => {
    const results: Subst[] = [];
    let completed = false;
    let error: any = undefined;
    
    const effectiveGoal = timeoutMs ? timeout(goal, timeoutMs) : goal;
    const limitedGoal = maxResults ? (s: Subst) => effectiveGoal(s).take(maxResults) : effectiveGoal;
    
    limitedGoal(new Map()).subscribe({
      next: (result) => {
        results.push(result);
      },
      complete: () => {
        completed = true;
        resolve({
          results,
          completed,
          error 
        });
      },
      error: (err) => {
        error = err;
        resolve({
          results,
          completed,
          error 
        });
      }
    });
  });
}