import { subscribe } from "diagnostics_channel";
import jsonata from "jsonata";
import { extract } from "../relations/objects.ts";
import {
  Goal,
  Subst,
  Term,
  Var,
  LiftedArgs,
  Observable,
  Subscription,
  Observer,
} from "./types.ts";
import {
  unify,
  lvar,
  walk,
  isVar,
  liftGoal,
  enrichGroupInput,
  arrayToLogicList
} from "./kernel.ts"
import { SimpleObservable } from "./observable.ts";
import { suspendable } from "./suspend-helper.ts";

/**
 * A goal that succeeds if two terms can be unified.
 */
// export function eq(u: Term, v: Term): Goal {
//   return liftGoal(function eq (s: Subst){
//     return new SimpleObservable<Subst>((observer) => {
//       try {
//         const result = unify(u, v, s);
//         if (result !== null) {
//           observer.next(result);
//         }
//         observer.complete?.();
//       } catch (error) {
//         observer.error?.(error);
//       }
//     });
//   });
// }
export function eq (x: Term, y: Term): Goal {
  return enrichGroupInput("eq", [], [], (input$) => 
    new SimpleObservable((observer) => {
      const sub = input$.subscribe({
        complete: observer.complete,
        error: observer.error,
        next: (subst) => {
          const s2 = unify(x,y,subst);
          if (s2) {
            observer.next(s2);
          }
        }
      })

      return () => sub.unsubscribe();
    })
  )
}
// export function eq(x: Term<any>, y: Term<any>): Goal {
//   return suspendable([x, y], (values, subst) => {
//     const [xVal, yVal] = values;
//     const xGrounded = !isVar(xVal);
//     const yGrounded = !isVar(yVal);

//     // All grounded - check constraint
//     if (xGrounded && yGrounded) {
//       return (xVal === yVal) ? subst : null;
//     }

//     if(xGrounded) {
//       const s2 = unify(xVal, yVal, subst);
//       if(s2) return s2;
//       return null;
//     }

//     if(yGrounded) {
//       const s2 = unify(yVal, xVal, subst);
//       if(s2) return s2;
//       return null;
//     }
    
//     return CHECK_LATER; // Still not enough variables bound
//   });
// }

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
        const freshVars = Array.from({ length: f.length }, () => lvar());
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
  return enrichGroupInput("and", goals, [], (enrichedInput$) =>
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
  
  return enrichGroupInput("or", [], goals, (input$: SimpleObservable<Subst>) => {
    return new SimpleObservable<Subst>((observer) => {
      // Use the improved share() method that replays values for logic programming
      const sharedInput$ = input$.share();
      
      let completedGoals = 0;
      const subscriptions: any[] = [];
      
      for (const goal of goals) {
        const goalSubscription = goal(sharedInput$).subscribe({
          next: observer.next,
          error: observer.error,
          complete: () => {
            completedGoals++;
            if (completedGoals === goals.length) {
              observer.complete?.();
            }
          }
        });
        subscriptions.push(goalSubscription);
      }
      
      return () => {
        subscriptions.forEach(sub => sub.unsubscribe?.());
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
 * project: Declarative data transformation relation for logic engine.
 * Allows extracting or mapping fields from an object using a path string or mapping object.
 * Example:
 *   project($.input, "species.name", $.species_name)
 *   project($.input, { genus: "genera[0].genus" }, $.output)
 */
function getByPath(obj: any, path: string): any {
  if (!path) return obj;
  const parts = path.split(".");
  let current = obj;
  for (const part of parts) {
    if (!current) return undefined;
    const match = part.match(/^(\w+)\[\?\(@\.(.+?)==['"](.+?)['"]\)\]$/);
    if (match) {
      const [_, arrKey, filterKey, filterVal] = match;
      current = (current[arrKey] || []).find((x: any) => x?.[filterKey] === filterVal);
    } else if (part.endsWith("]")) {
      const arrMatch = part.match(/^(\w+)\[(\d+)\]$/);
      if (arrMatch) {
        const [_, arrKey, idx] = arrMatch;
        current = (current[arrKey] || [])[parseInt(idx)];
      } else {
        current = current[part];
      }
    } else {
      current = current[part];
    }
  }
  return current;
}

// project(input, "field.path", outputVar)
// project(input, { out1: "path1", out2: "path2" }, outputObjVar)
export function project(
  inputVar: any,
  pathOrMap: string | Record<string, string>,
  outputVar: any
): Goal {
  return (input$: SimpleObservable<Subst>) => new SimpleObservable<Subst>((observer) => {
    const subscription = input$.subscribe({
      next: (s) => {
        const input = walk(inputVar, s);
        if (isVar(input)) return;
        if (input === undefined) return;
        if (typeof pathOrMap === "string") {
          const value = getByPath(input, pathOrMap);
          const unified = unify(outputVar, value, s);
          if (unified !== null) observer.next(unified);
        } else {
          const outObj: Record<string, any> = {};
          for (const key in pathOrMap) {
            outObj[key] = getByPath(input, pathOrMap[key]);
          }
          const unified = unify(outputVar, outObj, s);
          if (unified !== null) observer.next(unified);
        }
      },
      error: observer.error,
      complete: observer.complete
    });
    return () => subscription.unsubscribe?.();
  });
}

/**
 * projectJsonata: Declarative data transformation using JSONata expressions.
 *
 * @param inputVars - An object mapping keys to logic vars, or a single logic var.
 * @param jsonataExpr - The JSONata template string.
 * @param outputVars - An object mapping output keys to logic vars, or a single logic var.
 *
 * Example:
 *   projectJsonata({ x: $.some_var, y: $.some_other_var }, "{ thing: x, thang: y }", { thing: $.thing_here, thang: $.thang_here })
 *   projectJsonata($.input, "$value + 1", $.output)
 */
export function projectJsonata(
  inputVars: any,
  jsonataExpr: string,
  outputVars: any
): Goal {
  const expr = jsonata(jsonataExpr);
  return (input$: SimpleObservable<Subst>) => new SimpleObservable<Subst>((observer) => {
    let active = 0;
    let completed = false;
    const subscription = input$.subscribe({
      next: async (s) => {
        active++;
        // Prepare input for JSONata
        let inputObj: any;
        if (typeof inputVars === "object" && inputVars !== null && !isVar(inputVars)) {
          inputObj = {};
          for (const key in inputVars) {
            inputObj[key] = walk(inputVars[key], s);
          }
        } else {
          inputObj = walk(inputVars, s);
        }
        // Evaluate JSONata
        let result;
        try {
          result = await expr.evaluate(inputObj);
        } catch (e) {
          observer.error?.(e);
          active--;
          if (completed && active === 0) observer.complete?.();
          return;
        }
        // Unify result to output logic vars
        if (typeof outputVars === "object" && outputVars !== null && !isVar(outputVars)) {
          if (result && typeof result.then === "function") {
            result.then((resolved: any) => {
              let currentSubst = s;
              for (const key in outputVars) {
                const value = resolved && typeof resolved === 'object' ? resolved[key] : undefined;
                const unified = unify(outputVars[key], value, currentSubst);
                if (unified !== null) {
                  currentSubst = unified;
                } else {
                  // If any unification fails, skip this result
                  active--;
                  if (completed && active === 0) observer.complete?.();
                  return;
                }
              }
              observer.next(currentSubst);
              active--;
              if (completed && active === 0) observer.complete?.();
            }).catch((e: any) => {
              observer.error?.(e);
              active--;
              if (completed && active === 0) observer.complete?.();
            });
          } else {
            const resolved = result as any;
            let currentSubst = s;
            for (const key in outputVars) {
              const value = resolved && typeof resolved === 'object' ? resolved[key] : undefined;
              const unified = unify(outputVars[key], value, currentSubst);
              if (unified !== null) {
                currentSubst = unified;
              } else {
                // If any unification fails, skip this result
                active--;
                if (completed && active === 0) observer.complete?.();
                return;
              }
            }
            observer.next(currentSubst);
            active--;
            if (completed && active === 0) observer.complete?.();
          }
        } else {
          if (result && typeof result.then === "function") {
            result.then((resolved: any) => {
              const unified = unify(outputVars, resolved, s);
              if (unified !== null) observer.next(unified);
              active--;
              if (completed && active === 0) observer.complete?.();
            }).catch((e: any) => {
              observer.error?.(e);
              active--;
              if (completed && active === 0) observer.complete?.();
            });
          } else {
            const unified = unify(outputVars, result, s);
            if (unified !== null) observer.next(unified);
            active--;
            if (completed && active === 0) observer.complete?.();
          }
        }
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
 * Subquery: Run a subgoal and bind its results to a variable in the main stream.
 * This is the universal bridge between goal-based and stream-based operations.
 * 
 * @param goal - The subgoal to run
 * @param extractVar - Variable to extract from subgoal results  
 * @param bindVar - Variable to bind the extracted results to in main stream
 * @param aggregator - How to combine multiple results (receives results and original substitution)
 * 
 * Examples:
 * - Subquery(membero(x, [1,2,3]), x, $.items) // binds $.items to [1,2,3]
 * - Subquery(goal, x, $.count, (results, _) => results.length) // binds $.count to result count
 * - Subquery(goal, x, $.count, (results, s) => results.filter(r => r === walk(target, s)).length) // count matches
 */
export function Subquery(
  goal: Goal,
  extractVar: Term,
  bindVar: Term,
  aggregator: (results: any[], originalSubst: Subst) => any = (results, _) => arrayToLogicList(results)
): Goal {
  return enrichGroupInput("Subquery", [], [goal], (input$) =>
    input$.flatMap((s: Subst) => {
      const extracted: any[] = [];
      
      return new SimpleObservable<Subst>((observer) => {
        const subgoalSubscription = goal(SimpleObservable.of(s)).subscribe({
          next: (subResult) => {
            // Extract the value from each subgoal result
            const value = walk(extractVar, subResult);
            extracted.push(value);
          },
          error: (error) => {
            extracted.length = 0;
            observer.error?.(error);
          },
          complete: () => {
            // Aggregate all extracted values and bind to the target variable
            // Pass the original substitution so aggregator can walk variables
            const aggregated = aggregator(extracted, s);
            const unified = unify(bindVar, aggregated, s);
            if (unified !== null) {
              observer.next(unified);
            }
            extracted.length = 0;
            observer.complete?.();
          }
        });
        
        return () => {
          subgoalSubscription.unsubscribe?.();
          extracted.length = 0;
        };
      });
    })
  );
}

export function branch(
  goal: Goal,
  aggregator: (observer: Observer<Subst>, substs: Subst[], originalSubst: Subst) => void
): Goal {
  return enrichGroupInput("branch", [], [goal], (input$) =>
    new SimpleObservable<Subst>((observer) => {
      const goalSubs: Subscription[] = [];
      const inputSub = input$.subscribe({
        error: observer.error,
        complete: observer.complete,
        next: (inputSubst) => {
          const collectedSubsts: Subst[] = [];
          const goalSub = goal(SimpleObservable.of(inputSubst)).subscribe({
            error: observer.error,
            complete: () => {
              aggregator(observer, collectedSubsts, inputSubst);
              // observer.complete?.();
              collectedSubsts.length = 0;
            },
            next: (goalSubst) => {
              collectedSubsts.push(goalSubst);
            }
          });
          goalSubs.push(goalSub);
        }
      });

      return () => {
        goalSubs.forEach(goalSub => goalSub.unsubscribe());
        inputSub.unsubscribe();
      }
    })
  );
}