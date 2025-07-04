import {
  Goal,
  Subst,
  Term,
  Var,
  LiftedArgs,
  Observable
} from "./types.ts";
import {
  unify,
  lvar,
  walk,
  isVar,
  liftGoal
} from "./kernel.ts"
import { SimpleObservable } from "./observable.ts";

// Well-known symbols for SQL query coordination
export const SQL_GROUP_ID = Symbol('sql-group-id');
export const SQL_GROUP_PATH = Symbol('sql-group-path');
export const SQL_GROUP_GOALS = Symbol('sql-group-goals');

// Counter for generating unique group IDs
let groupIdCounter = 0;

/**
 * A goal that succeeds if two terms can be unified.
 */
export function eq(u: Term, v: Term): Goal {
  return liftGoal((s: Subst) => {
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
  });
}

/**
 * Introduces new (fresh) logic variables into a sub-goal.
 */
export function fresh(f: (...vars: Var[]) => Goal): Goal {
  return (input$) => new SimpleObservable<Subst>((observer) => {
    let active = 0;
    let completed = false;
    const subscription = input$.subscribe({
      next: (s) => {
        active++;
        const freshVars = Array.from({
          length: f.length 
        }, () => lvar());
        const subGoal = f(...freshVars);
        subGoal(SimpleObservable.of(s)).subscribe({
          next: observer.next,
          error: observer.error,
          complete: () => {
            active--;
            if (completed && active === 0) observer.complete?.();
          }
        });
      },
      error: observer.error,
      complete: () => {
        completed = true;
        if (active === 0) observer.complete?.();
      }
    });
    return () => subscription.unsubscribe?.();
  });
}

/**
 * Logical disjunction (OR).
 */
export function disj(g1: Goal, g2: Goal): Goal {
  return (input$) => {
    const disjId = ++groupIdCounter;
    
    const branch1Input$ = input$.map(s => {
      const parentPath = s.get(SQL_GROUP_PATH) || [];
      const newPath = [...parentPath, { type: Symbol('disj'), id: disjId, branch: 0 }];
      
      return new Map([
        ...s,
        [SQL_GROUP_ID, disjId],
        [SQL_GROUP_PATH, newPath],
        [SQL_GROUP_GOALS, []] // Each branch gets its own goals list
      ]);
    });
    
    const branch2Input$ = input$.map(s => {
      const parentPath = s.get(SQL_GROUP_PATH) || [];
      const newPath = [...parentPath, { type: Symbol('disj'), id: disjId, branch: 1 }];
      
      return new Map([
        ...s,
        [SQL_GROUP_ID, disjId],
        [SQL_GROUP_PATH, newPath],
        [SQL_GROUP_GOALS, []] // Each branch gets its own goals list
      ]);
    });
    
    return g1(branch1Input$).merge(g2(branch2Input$));
  };
}

/**
 * Logical conjunction (AND).
 */
export function conj(g1: Goal, g2: Goal): Goal {
  return (input$) => {
    const conjId = ++groupIdCounter;
    
    const enrichedInput$ = input$.map(s => {
      const parentPath = s.get(SQL_GROUP_PATH) || [];
      const newPath = [...parentPath, { type: Symbol('conj'), id: conjId }];
      
      return new Map([
        ...s,
        [SQL_GROUP_ID, conjId],
        [SQL_GROUP_PATH, newPath],
        [SQL_GROUP_GOALS, [g1, g2]] // Include both goals in the group
      ]);
    });
    
    // Stream conjunction: g2 processes results from g1
    return g2(g1(enrichedInput$));
  };
}

/**
 * Helper for combining multiple goals with logical AND.
 */
export const and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (input$) => input$;
  }
  return goals.reduce(conj);
};

/**
 * Helper for combining multiple goals with logical OR.
 */
export const or = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return () => SimpleObservable.empty<Subst>();
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
    return (input$: SimpleObservable<Subst>) => new SimpleObservable<Subst>((observer) => {
      const subscription = input$.subscribe({
        next: (s) => {
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
          } catch (error) {
            observer.error?.(error);
          }
        },
        error: observer.error,
        complete: observer.complete
      });
      return () => subscription.unsubscribe?.();
    });
  }) as LiftedArgs<T>;
}

/**
 * Soft-cut if-then-else combinator.
 */
export function ifte(ifGoal: Goal, thenGoal: Goal, elseGoal: Goal): Goal {
  return (input$) => new SimpleObservable<Subst>((observer) => {
    input$.subscribe({
      next: (s) => {
        let succeeded = false;
        const results: Subst[] = [];
        ifGoal(SimpleObservable.of(s)).subscribe({
          next: (s1) => {
            succeeded = true;
            results.push(s1);
          },
          complete: () => {
            if (succeeded) {
              let completed = 0;
              for (const s1 of results) {
                thenGoal(SimpleObservable.of(s1)).subscribe({
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
              elseGoal(SimpleObservable.of(s)).subscribe({
                next: observer.next,
                complete: observer.complete,
                error: observer.error
              });
            }
          },
          error: observer.error
        });
      },
      error: observer.error,
      complete: observer.complete
    });
  });
}

/**
 * Negation as failure - succeeds only if the goal fails
 */
export function not(goal: Goal): Goal {
  return (input$) => new SimpleObservable<Subst>((observer) => {
    input$.subscribe({
      next: (s) => {
        let succeeded = false;
        goal(SimpleObservable.of(s)).subscribe({
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
      },
      error: observer.error,
      complete: observer.complete
    });
  });
}

/**
 * Succeeds exactly once with the given substitution (useful for cut-like behavior)
 */
export function once(goal: Goal): Goal {
  return (input$) => goal(input$).take(1);
}

/**
 * Apply a goal with a timeout
 */
export function timeout(goal: Goal, timeoutMs: number): Goal {
  return (input$) => new SimpleObservable<Subst>((observer) => {
    let completed = false;
    const timer = setTimeout(() => {
      if (!completed) {
        completed = true;
        observer.error?.(new Error(`Goal timed out after ${timeoutMs}ms`));
      }
    }, timeoutMs);
    goal(input$).subscribe({
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
    };
  });
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
    const limitedGoal = maxResults ? (input$: SimpleObservable<Subst>) => effectiveGoal(input$).take(maxResults) : effectiveGoal;
    limitedGoal(SimpleObservable.of(new Map())).subscribe({
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