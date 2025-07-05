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
export const SQL_INNER_GROUP_GOALS = Symbol('sql-inner-group-goals'); // Goals in immediate group
export const SQL_OUTER_GROUP_GOALS = Symbol('sql-outer-group-goals'); // Goals across all related groups

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
  return enrichGroupInput("disj", ++groupIdCounter, [g1, g2], (enrichedInput$) => {
    const branch1Input$ = enrichGroupInput("disj", groupIdCounter, [g1, g2], x => x, 0)(enrichedInput$);
    const branch2Input$ = enrichGroupInput("disj", groupIdCounter, [g1, g2], x => x, 1)(enrichedInput$);
    return g1(branch1Input$).merge(g2(branch2Input$));
  });
}

/**
 * Logical conjunction (AND).
 */
export function conj(g1: Goal, g2: Goal): Goal {
  return enrichGroupInput("conj", ++groupIdCounter, [g1, g2], (enrichedInput$) => g1(g2(enrichedInput$)));
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
  return enrichGroupInput("or", ++groupIdCounter, goals, (enrichedInput$) => {
    const orId = groupIdCounter;
    const branches = goals.map((goal, index) => {
      const branchInput$ = enrichGroupInput("or", orId, goals, x => x, index)(enrichedInput$);
      return goal(branchInput$);
    });
    return branches.reduce((acc, branch) => acc.merge(branch));
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

/**
 * Helper to enrich input$ with group metadata for combinators, and apply a function to the enriched input$.
 * @param type - The group type ('and', 'or', 'conj', 'disj')
 * @param groupId - The unique group id
 * @param goals - The goals in this group (array)
 * @param fn - Function to apply to the enriched input$
 * @param branch - (optional) branch index for or/disj
 * @returns A function that takes input$ and returns the result of fn(enrichedInput$)
 */
function enrichGroupInput(
  type: string,
  groupId: number,
  goals: Goal[],
  fn: (enrichedInput$: SimpleObservable<Subst>) => any,
  branch?: number
) {
  const newInput$ = (input$: SimpleObservable<Subst>) => {
    const enrichedInput$ = input$.map(s => {
      const parentPath = (s.get(SQL_GROUP_PATH) as any[]) || [];
      const newPath = [...parentPath, {
        type: Symbol(type),
        id: groupId,
        ...(branch !== undefined ? {
          branch
        } : {})
      }];
      const parentOuterGoals = (s.get(SQL_OUTER_GROUP_GOALS) as Goal[]) || [];
      const goalsInnerGoals = goals.flatMap(goal => goal.innerGoals ? [goal, ...goal.innerGoals] : [goal]);
      const combinedOuterGoals = [...new Set([...parentOuterGoals, ...goalsInnerGoals])];
      // const combinedOuterGoals = [...parentOuterGoals, ...goalsInnerGoals];
      const newSubst = new Map(s);
      newSubst.set(SQL_GROUP_ID, groupId);
      newSubst.set(SQL_GROUP_PATH, newPath);
      newSubst.set(SQL_INNER_GROUP_GOALS, goals);
      newSubst.set(SQL_OUTER_GROUP_GOALS, combinedOuterGoals);
      return newSubst;
    });
    return fn(enrichedInput$);
  };
  newInput$.innerGoals = goals;
  newInput$.displayName = `${type}_${groupId}`; // For better debugging
  return newInput$;
}