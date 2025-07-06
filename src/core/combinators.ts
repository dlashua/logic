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
export const GOAL_GROUP_ID = Symbol('goal-group-id');
export const GOAL_GROUP_PATH = Symbol('goal-group-path');
export const GOAL_GROUP_INNER_GOALS = Symbol('goal-group-inner-goals'); // Goals in immediate group
export const GOAL_GROUP_OUTER_GOALS = Symbol('goal-group-outer-goals'); // Goals across all related groups

// Counter for generating unique group IDs
let groupIdCounter = 0;

/**
 * A goal that succeeds if two terms can be unified.
 */
export function eq(u: Term, v: Term): Goal {
  return liftGoal(function eq (s: Subst){
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
  return or(g1, g2);
}

/**
 * Logical conjunction (AND).
 */
export function conj(g1: Goal, g2: Goal): Goal {
  return and(g1, g2);
}

/**
 * Helper for combining multiple goals with logical AND.
 * Creates a single group containing all goals for optimal SQL merging.
 */
export const and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (input$) => input$;
  }
  if (goals.length === 1) {
    return goals[0];
  }
  return enrichGroupInput("and", ++groupIdCounter, goals, (enrichedInput$) =>
    goals.reduce((acc, goal) => goal(acc), enrichedInput$)
  );
};

/**
 * Helper for combining multiple goals with logical OR.
 * Creates a single group containing all goals for optimal SQL merging.
 */
export const or = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return () => SimpleObservable.empty<Subst>();
  }
  if (goals.length === 1) {
    return goals[0];
  }
  
  return enrichGroupInput("or", ++groupIdCounter, [], (input$: SimpleObservable<Subst>) => {
    const groupId = ++groupIdCounter;
    
    // Collect all input substitutions first
    const collectedInputs: Subst[] = [];
    let inputCompleted = false;
    
    return new SimpleObservable<Subst>(observer => {
      const subscriptions: (() => void)[] = [];
      
      // First, collect all input substitutions
      const inputSub = input$.subscribe({
        next: (s) => {
          collectedInputs.push(s);
        },
        error: observer.error,
        complete: () => {
          inputCompleted = true;
          
          // Now execute each goal with a stream of enriched substitutions
          let completedGoals = 0;
          
          goals.forEach((goal, index) => {
            const enrichedInputs = collectedInputs.map(s => 
              createEnrichedSubst(s, "or_in", groupId, [goal], [goal], index)
            );
            
            const enrichedStream = new SimpleObservable<Subst>(goalObserver => {
              enrichedInputs.forEach(enrichedSubst => goalObserver.next(enrichedSubst));
              goalObserver.complete?.();
            });
            
            const goalSub = goal(enrichedStream).subscribe({
              next: observer.next,
              error: observer.error,
              complete: () => {
                completedGoals++;
                if (completedGoals === goals.length) {
                  observer.complete?.();
                }
              }
            });
            
            subscriptions.push(() => goalSub.unsubscribe?.());
          });
        }
      });
      
      subscriptions.push(() => inputSub.unsubscribe?.());
      
      return () => {
        subscriptions.forEach(unsub => unsub());
      };
    });
  });
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
  return enrichGroupInput("not", ++groupIdCounter, [goal], (enrichedInput$) => {
    return new SimpleObservable<Subst>((observer) => {
      enrichedInput$.subscribe({
        next: (s) => {
          let succeeded = false;
          goal(SimpleObservable.of(s)).subscribe({
            next: () => {
              succeeded = true;
            },
            complete: () => {
              if (!succeeded) {
                observer.next(s); // Return enriched substitution unchanged
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

/**
 * Creates an enriched substitution with group metadata
 */
function createEnrichedSubst(
  s: Subst,
  type: string,
  groupId: number,
  goals: Goal[],
  innerGoals: Goal[],
  branch?: number
): Subst {
  const parentPath = (s.get(GOAL_GROUP_PATH) as any[]) || [];
  const newPath = [...parentPath, {
    type: Symbol(type),
    id: groupId,
    ...(branch !== undefined ? {
      branch 
    } : {})
  }];
  const parentOuterGoals = (s.get(GOAL_GROUP_OUTER_GOALS) as Goal[]) || [];
  // Recursively collect all innerGoals from the goals array
  function collectAllInnerGoals(goals: Goal[]): Goal[] {
    return goals.flatMap(goal => {
      const inner = (goal as any).innerGoals as Goal[] | undefined;
      if (inner && inner.length > 0) {
        return [goal, ...collectAllInnerGoals(inner)];
      } else {
        return [goal];
      }
    });
  }
  const goalsInnerGoals = collectAllInnerGoals(goals);
  const combinedOuterGoals = [...new Set([...parentOuterGoals, ...goalsInnerGoals])];
  
  const newSubst = new Map(s);
  newSubst.set(GOAL_GROUP_ID, groupId);
  newSubst.set(GOAL_GROUP_PATH, newPath);
  newSubst.set(GOAL_GROUP_INNER_GOALS, innerGoals);
  newSubst.set(GOAL_GROUP_OUTER_GOALS, combinedOuterGoals);
  return newSubst;
}

/**
 * Unified helper for enriching input with group metadata
 */
export function enrichGroupInput(
  type: string,
  groupId: number,
  goals: Goal[],
  fn: (enrichedInput$: SimpleObservable<Subst>) => any
) {
  const newInput$ = (input$: SimpleObservable<Subst>) => {
    const enrichedInput$ = input$.map(s => 
      createEnrichedSubst(s, type, groupId, goals, goals)
    );
    return fn(enrichedInput$);
  };
  (newInput$ as any).innerGoals = goals;
  newInput$.displayName = `${type}_${groupId}`;
  return newInput$;
}