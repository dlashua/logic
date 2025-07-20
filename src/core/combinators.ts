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

// g4_and
export const g4_and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (input$) => input$;
  }
  if (goals.length === 1) {
    return goals[0];
  }
  return enrichGroupInput("and", goals, [], (enrichedInput$) => {
    // console.log("and: Starting with", goals.length, "goals");
    const prioritizedGoals = [...goals].sort((a, b) => {
      const aName = a.name || 'unnamed';
      const bName = b.name || 'unnamed';
      const bindingGoals = ['eq', 'membero'];
      const aIsBinding = bindingGoals.includes(aName);
      const bIsBinding = bindingGoals.includes(bName);
      return aIsBinding && !bIsBinding ? -1 : bIsBinding && !aIsBinding ? 1 : 0;
    });
    return prioritizedGoals.reduce((acc, goal, index) => 
      acc.flatMap(s => {
        // console.log(`and: Processing goal ${index + 1} (${goal.name || 'unnamed'})`);
        const result$ = goal(SimpleObservable.of(s));
        return result$.map(s2 => {
          // console.log(`and: Emitted from goal ${index + 1} (${goal.name || 'unnamed'})`);
          return s2;
        });
      }), enrichedInput$);
  });
};


export const g3_and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (input$) => input$;
  }
  if (goals.length === 1) {
    return goals[0];
  }
  return enrichGroupInput("and", goals, [], (enrichedInput$) => {
    // console.log("and: Starting with", goals.length, "goals");
    const prioritizedGoals = [...goals].sort((a, b) => {
      const aName = a.name || 'unnamed';
      const bName = b.name || 'unnamed';
      const bindingGoals = ['eq', 'membero'];
      const aIsBinding = bindingGoals.includes(aName);
      const bIsBinding = bindingGoals.includes(bName);
      return aIsBinding && !bIsBinding ? -1 : bIsBinding && !aIsBinding ? 1 : 0;
    });
    return prioritizedGoals.reduce((acc, goal, index) => 
      acc.flatMap(s => {
        // console.log(`and: Processing goal ${index + 1} (${goal.name || 'unnamed'}) with subst`, s);
        const result$ = goal(SimpleObservable.of(s));
        return new SimpleObservable<Subst>((observer) => {
          let hasEmitted = false;
          console.log(goal);
          const sub = result$.subscribe({
            next: (s2) => {
              // console.log(`and: Emitting subst from goal ${index + 1} (${goal.name || 'unnamed'})`, s2);
              hasEmitted = true;
              observer.next(s2);
            },
            error: observer.error,
            complete: () => {
              console.log(`and: Goal ${index + 1} (${goal.name || 'unnamed'}) completed, emitted:`, hasEmitted);
              if (!hasEmitted) {
                observer.next(s); // Pass through input substitution if no output
              }
              observer.complete?.();
            }
          });
          return () => sub.unsubscribe?.();
        });
      }), enrichedInput$);
  });
};

export const g2_and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (input$) => input$;
  }
  if (goals.length === 1) {
    return goals[0];
  }
  return enrichGroupInput("and", goals, [], (enrichedInput$) => {
    console.log("and: Starting with", goals.length, "goals");
    const prioritizedGoals = goals.sort((a, b) => {
      const aName = a.name || '';
      const bName = b.name || '';
      const bindingGoals = ['eq', 'membero'];
      const aIsBinding = bindingGoals.includes(aName);
      const bIsBinding = bindingGoals.includes(bName);
      return aIsBinding && !bIsBinding ? -1 : bIsBinding && !aIsBinding ? 1 : 0;
    });
    return prioritizedGoals.reduce((acc, goal, index) => 
      acc.flatMap(s => {
        console.log(`and: Processing goal ${index + 1} (${goal.name || 'unnamed'}) with subst`, s);
        const result$ = goal(SimpleObservable.of(s));
        return result$.map(s2 => {
          console.log(`and: Emitting subst from goal ${index + 1}`, s2);
          return s2;
        });
      }), enrichedInput$);
  });
};

export const g1_and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (input$) => input$;
  }
  if (goals.length === 1) {
    return goals[0];
  }
  return enrichGroupInput("and", goals, [], (enrichedInput$) =>
    goals.reduce((acc, goal) => 
      acc.flatMap(s => {
        const result$ = goal(SimpleObservable.of(s));
        return result$.map(s2 => s2); // Ensure all substitutions, including suspended ones, are propagated
      }), enrichedInput$)
  );
};

// old_and
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
 * Sequential AND combinator that runs each goal entirely before sending results to the next goal.
 * Unlike the standard and() which uses flatMap to interleave results, this version collects
 * all results from each goal before proceeding to the next one.
 */
// sequential and
export const sequential_and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (input$) => input$;
  }
  if (goals.length === 1) {
    return goals[0];
  }
  
  return enrichGroupInput("sequential_and", goals, [], (enrichedInput$) => {
    return new SimpleObservable<Subst>((observer) => {
      const processGoalsSequentially = (goalIndex: number, currentResults: Subst[]) => {
        if (goalIndex >= goals.length) {
          // All goals processed, emit all final results
          currentResults.forEach(result => observer.next(result));
          observer.complete?.();
          return;
        }

        const currentGoal = goals[goalIndex];
        const nextResults: Subst[] = [];
        let completedInputs = 0;
        const totalInputs = currentResults.length;

        if (totalInputs === 0) {
          // No inputs to process, move to completion
          observer.complete?.();
          return;
        }

        // Process each current result through the current goal
        currentResults.forEach((inputSubst) => {
          currentGoal(SimpleObservable.of(inputSubst)).subscribe({
            next: (resultSubst) => {
              nextResults.push(resultSubst);
            },
            error: (error) => {
              observer.error?.(error);
            },
            complete: () => {
              completedInputs++;
              if (completedInputs === totalInputs) {
                // This goal has processed all inputs, move to next goal
                processGoalsSequentially(goalIndex + 1, nextResults);
              }
            }
          });
        });
      };

      // Start by collecting all results from the input stream
      const initialResults: Subst[] = [];
      const inputSubscription = enrichedInput$.subscribe({
        next: (subst) => {
          initialResults.push(subst);
        },
        error: observer.error,
        complete: () => {
          // Input stream is complete, start processing goals sequentially
          processGoalsSequentially(0, initialResults);
        }
      });

      return () => {
        inputSubscription.unsubscribe?.();
      };
    });
  });
};

/**
 * Batch AND combinator that collects all results from each goal before proceeding.
 * This is a more functional version that processes goals one at a time.
 */
// batch and
export const batch_and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (input$) => input$;
  }
  if (goals.length === 1) {
    return goals[0];
  }

  return enrichGroupInput("batch_and", goals, [], (enrichedInput$) => {
    // Helper function to collect all results from an observable
    const collectResults = (obs$: SimpleObservable<Subst>): Promise<Subst[]> => {
      return new Promise((resolve, reject) => {
        const results: Subst[] = [];
        obs$.subscribe({
          next: (subst) => results.push(subst),
          error: reject,
          complete: () => resolve(results)
        });
      });
    };

    // Helper function to run a goal on multiple inputs and collect all results
    const runGoalOnInputs = async (goal: Goal, inputs: Subst[]): Promise<Subst[]> => {
      const allResults: Subst[] = [];
      
      for (const input of inputs) {
        const goalResults = await collectResults(goal(SimpleObservable.of(input)));
        allResults.push(...goalResults);
      }
      
      return allResults;
    };

    return new SimpleObservable<Subst>((observer) => {
      // First collect all initial inputs
      collectResults(enrichedInput$)
        .then(async (initialInputs) => {
          try {
            let currentResults = initialInputs;
            
            // Process each goal sequentially
            for (const goal of goals) {
              currentResults = await runGoalOnInputs(goal, currentResults);
            }
            
            // Emit all final results
            currentResults.forEach(result => observer.next(result));
            observer.complete?.();
            
          } catch (error) {
            observer.error?.(error);
          }
        })
        .catch(error => observer.error?.(error));
    });
  });
};

/**
 * Buffered AND combinator that processes goals in sequence with full buffering.
 * Each goal receives all results from the previous goal at once.
 */
// buffered_and
export const buffered_and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return (input$) => input$;
  }
  if (goals.length === 1) {
    return goals[0];
  }

  return enrichGroupInput("buffered_and", goals, [], (enrichedInput$) => {
    return new SimpleObservable<Subst>((observer) => {
      let currentBuffer: Subst[] = [];
      let goalIndex = 0;

      const processNextGoal = () => {
        if (goalIndex >= goals.length) {
          // All goals processed, emit buffered results
          currentBuffer.forEach(subst => observer.next(subst));
          observer.complete?.();
          return;
        }

        const goal = goals[goalIndex];
        const inputBuffer = [...currentBuffer]; // Copy current buffer
        currentBuffer = []; // Reset for next goal's results
        goalIndex++;

        if (inputBuffer.length === 0) {
          // No inputs to process
          observer.complete?.();
          return;
        }

        let completedCount = 0;
        
        // Run goal on each input in the buffer
        inputBuffer.forEach(inputSubst => {
          goal(SimpleObservable.of(inputSubst)).subscribe({
            next: (resultSubst) => {
              currentBuffer.push(resultSubst);
            },
            error: observer.error,
            complete: () => {
              completedCount++;
              if (completedCount === inputBuffer.length) {
                // All inputs for this goal are complete
                processNextGoal();
              }
            }
          });
        });
      };

      // Collect initial inputs first
      enrichedInput$.subscribe({
        next: (subst) => {
          currentBuffer.push(subst);
        },
        error: observer.error,
        complete: () => {
          // Initial input complete, start processing goals
          processNextGoal();
        }
      });
    });
  });
};



/**
 * Helper for combining multiple goals with logical OR.
 * Creates a single group containing all goals for optimal SQL merging.
 */

// g1_or
export const g1_or = (...goals: Goal[]): Goal => {
  if (goals.length === 0) {
    return () => SimpleObservable.empty<Subst>();
  }
  if (goals.length === 1) {
    return goals[0];
  }
  return enrichGroupInput("or", [], goals, (input$: SimpleObservable<Subst>) => {
    return new SimpleObservable<Subst>((observer) => {
      const sharedInput$ = input$.share();
      let completedGoals = 0;
      const subscriptions: any[] = [];
      for (const goal of goals) {
        const goalSubscription = goal(sharedInput$).subscribe({
          next: (s2) => {
            // console.log(`or: Emitting subst from goal (${goal.name || 'unnamed'})`, s2);
            observer.next(s2);
          },
          error: observer.error,
          complete: () => {
            // console.log(`or: Goal (${goal.name || 'unnamed'}) completed`);
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
