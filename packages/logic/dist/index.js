// src/core/combinators.ts
import jsonata from "jsonata";
import { SimpleObservable as SimpleObservable3 } from "@swiftfall/observable";

// src/core/kernel.ts
import { SimpleObservable as SimpleObservable2 } from "@swiftfall/observable";

// src/core/suspend-helper.ts
import { SimpleObservable } from "@swiftfall/observable";
var CHECK_LATER = Symbol.for("constraint-check-later");
function makeSuspendHandler(vars, evaluator, minGrounded) {
  return function handleSuspend(subst) {
    const values = vars.map((v) => walk(v, subst));
    const groundedCount = values.filter((v) => !isVar(v)).length;
    if (groundedCount >= minGrounded) {
      const result = evaluator(values, subst);
      if (result === null) {
        return null;
      }
      if (result !== CHECK_LATER) {
        return result;
      }
    }
    const watchedVars = vars.filter((v) => isVar(v)).map((v) => v.id);
    if (watchedVars.length > 0) {
      return addSuspendToSubst(subst, handleSuspend, watchedVars);
    }
    return null;
  };
}
function suspendable(vars, evaluator, minGrounded = vars.length - 1) {
  const handleSuspend = makeSuspendHandler(vars, evaluator, minGrounded);
  return (input$) => new SimpleObservable((observer) => {
    const sub = input$.subscribe({
      next: (subst) => {
        try {
          const result = handleSuspend(subst);
          if (result !== null) {
            observer.next(result);
            return;
          }
        } catch (error) {
          observer.error?.(error);
        }
      },
      error: observer.error,
      complete: observer.complete
    });
    return () => sub.unsubscribe();
  });
}

// src/core/subst-suspends.ts
var SUSPENDED_CONSTRAINTS = Symbol("suspended-constraints");
var constraintCounter = 0;
var MAX_COUNTER = 1e6;
function addSuspendToSubst(subst, resumeFn, watchedVars) {
  const suspends = subst.get(SUSPENDED_CONSTRAINTS) || [];
  const stillRelevantVars = watchedVars.filter((varId) => {
    const value = walk({ tag: "var", id: varId }, subst);
    return isVar(value);
  });
  if (stillRelevantVars.length === 0) {
    return subst;
  }
  if (constraintCounter >= MAX_COUNTER) {
    constraintCounter = 0;
  }
  const newSuspend = {
    id: `constraint_${constraintCounter++}`,
    resumeFn,
    // watchedVars: stillRelevantVars // Use pruned list
    watchedVars
  };
  const newSubst = new Map(subst);
  newSubst.set(SUSPENDED_CONSTRAINTS, [...suspends, newSuspend]);
  return newSubst;
}
function getSuspendsFromSubst(subst) {
  return subst.get(SUSPENDED_CONSTRAINTS) || [];
}
function removeSuspendFromSubst(subst, suspendIds) {
  const suspends = getSuspendsFromSubst(subst);
  const filteredSuspends = suspends.filter((c) => !suspendIds.includes(c.id));
  const newSubst = new Map(subst);
  if (filteredSuspends.length === 0) {
    newSubst.delete(SUSPENDED_CONSTRAINTS);
  } else {
    newSubst.set(SUSPENDED_CONSTRAINTS, filteredSuspends);
  }
  return newSubst;
}
function wakeUpSuspends(subst, newlyBoundVars) {
  const suspends = getSuspendsFromSubst(subst);
  if (suspends.length === 0) {
    return subst;
  }
  const [toWake, toKeep] = suspends.reduce(
    ([wake, keep], s) => s.watchedVars.some((v) => newlyBoundVars.includes(v)) ? [[...wake, s], keep] : [wake, [...keep, s]],
    [[], []]
  );
  let currentSubst = subst;
  for (const suspend of toWake) {
    const result = suspend.resumeFn(currentSubst);
    if (result === null) {
      return null;
    } else if (result === CHECK_LATER) {
      toKeep.push(suspend);
    } else {
      toKeep.push(suspend);
      currentSubst = result;
    }
  }
  return currentSubst;
}

// src/core/kernel.ts
var GOAL_GROUP_ID = Symbol("goal-group-id");
var GOAL_GROUP_PATH = Symbol("goal-group-path");
var GOAL_GROUP_CONJ_GOALS = Symbol("goal-group-conj-goals");
var GOAL_GROUP_ALL_GOALS = Symbol("goal-group-all-goals");
var varCounter = 0;
var groupCounter = 0;
function nextGroupId() {
  return groupCounter++;
}
function lvar(name = "") {
  return {
    tag: "var",
    id: `${name}_${varCounter++}`
  };
}
function resetVarCounter() {
  varCounter = 0;
}
function walk(u, s) {
  let current = u;
  if (!isVar(current) && !isCons(current) && !Array.isArray(current) && typeof current !== "object") {
    return current;
  }
  while (isVar(current) && s.has(current.id)) {
    current = s.get(current.id);
  }
  if (isCons(current)) {
    return cons(walk(current.head, s), walk(current.tail, s));
  }
  if (Array.isArray(current)) {
    return current.map((x) => walk(x, s));
  }
  if (current && typeof current === "object" && !isVar(current) && !isLogicList(current)) {
    const out = {};
    for (const k in current) {
      if (Object.hasOwn(current, k)) {
        out[k] = walk(current[k], s);
      }
    }
    return out;
  }
  return current;
}
function extendSubst(v, val, s) {
  if (occursCheck(v, val, s)) {
    return null;
  }
  const s2 = new Map(s);
  s2.set(v.id, val);
  return s2;
}
function occursCheck(v, x, s) {
  const resolvedX = walk(x, s);
  if (isVar(resolvedX)) {
    return v.id === resolvedX.id;
  }
  if (isCons(resolvedX)) {
    return occursCheck(v, resolvedX.head, s) || occursCheck(v, resolvedX.tail, s);
  }
  if (Array.isArray(resolvedX)) {
    for (const item of resolvedX) {
      if (occursCheck(v, item, s)) {
        return true;
      }
    }
  }
  return false;
}
function baseUnify(u, v, s) {
  if (s === null) {
    return null;
  }
  if (u === v) {
    return s;
  }
  const uWalked = walk(u, s);
  const vWalked = walk(v, s);
  if (uWalked === vWalked) {
    return s;
  }
  if (isVar(uWalked)) return extendSubst(uWalked, vWalked, s);
  if (isVar(vWalked)) return extendSubst(vWalked, uWalked, s);
  if (typeof uWalked === "number" && typeof vWalked === "number") {
    return uWalked === vWalked ? s : null;
  }
  if (typeof uWalked === "string" && typeof vWalked === "string") {
    return uWalked === vWalked ? s : null;
  }
  if (isNil(uWalked) && isNil(vWalked)) return s;
  if (isCons(uWalked) && isCons(vWalked)) {
    const s1 = unify(uWalked.head, vWalked.head, s);
    if (s1 === null) return null;
    return unify(uWalked.tail, vWalked.tail, s1);
  }
  if (Array.isArray(uWalked) && Array.isArray(vWalked) && uWalked.length === vWalked.length) {
    let currentSubst = s;
    for (let i = 0; i < uWalked.length; i++) {
      currentSubst = unify(uWalked[i], vWalked[i], currentSubst);
      if (currentSubst === null) return null;
    }
    return currentSubst;
  }
  if (JSON.stringify(uWalked) === JSON.stringify(vWalked)) {
    return s;
  }
  return null;
}
function unifyWithConstraints(u, v, s) {
  const result = baseUnify(u, v, s);
  if (result !== null && s !== null) {
    if (!result.has(SUSPENDED_CONSTRAINTS)) {
      return result;
    }
    const newlyBoundVars = [];
    for (const [key] of result) {
      if (!s.has(key) && typeof key === "string") {
        newlyBoundVars.push(key);
      }
    }
    if (newlyBoundVars.length > 0) {
      return wakeUpSuspends(result, newlyBoundVars);
    }
  }
  return result;
}
var unify = unifyWithConstraints;
function isVar(x) {
  return typeof x === "object" && x !== null && x.tag === "var";
}
var nil = { tag: "nil" };
function cons(head, tail) {
  return {
    tag: "cons",
    head,
    tail
  };
}
function arrayToLogicList(arr) {
  return arr.reduceRight((tail, head) => cons(head, tail), nil);
}
function logicList(...items) {
  return arrayToLogicList(items);
}
function isCons(x) {
  return typeof x === "object" && x !== null && x.tag === "cons";
}
function isNil(x) {
  return typeof x === "object" && x !== null && x.tag === "nil";
}
function isLogicList(x) {
  return isCons(x) || isNil(x);
}
function logicListToArray(list) {
  const out = [];
  let cur = list;
  while (cur && typeof cur === "object" && "tag" in cur && cur.tag === "cons") {
    out.push(cur.head);
    cur = cur.tail;
  }
  return out;
}
function liftGoal(singleGoal) {
  const groupType = singleGoal.name || "liftGoal";
  return enrichGroupInput(
    groupType,
    [],
    [],
    (input$) => new SimpleObservable2((observer) => {
      const subs = input$.subscribe({
        next: (s) => {
          const out$ = singleGoal(s);
          out$.subscribe({
            next: (s2) => observer.next(s2),
            error: (e) => observer.error?.(e),
            complete: () => {
            }
            // wait for all
          });
        },
        error: (e) => observer.error?.(e),
        complete: () => observer.complete?.()
      });
      return () => subs.unsubscribe?.();
    })
  );
}
function chainGoals(goals, initial$) {
  return goals.reduce((input$, goal) => goal(input$), initial$);
}
function createEnrichedSubst(s, type, conjGoals, disjGoals, branch2) {
  const groupId = nextGroupId();
  const parentPath = s.get(GOAL_GROUP_PATH) || [];
  const newPath = [
    ...parentPath,
    {
      type: Symbol(type),
      id: groupId,
      ...branch2 !== void 0 ? { branch: branch2 } : {}
    }
  ];
  const parentOuterGoals = s.get(GOAL_GROUP_ALL_GOALS) || [];
  function collectAllInnerConjGoals(goals) {
    return goals.flatMap((goal) => {
      const innerGoals = goal?.conjGoals ?? [];
      if (innerGoals && innerGoals.length > 0) {
        return [...collectAllInnerConjGoals(innerGoals)];
      } else {
        return [goal];
      }
    });
  }
  function collectAllInnerGoals(goals) {
    return goals.flatMap((goal) => {
      const innerConjGoals = goal?.conjGoals ?? [];
      const innerDisjGoals = goal?.disjGoals ?? [];
      const innerGoals = [.../* @__PURE__ */ new Set([...innerConjGoals, ...innerDisjGoals])];
      if (innerGoals && innerGoals.length > 0) {
        return [...collectAllInnerGoals(innerGoals)];
      } else {
        return [goal];
      }
    });
  }
  const allGoals = [...conjGoals, ...disjGoals];
  const conjInnerConj = collectAllInnerConjGoals(conjGoals);
  const disjInnerAll = collectAllInnerGoals(allGoals);
  const newSubst = new Map(s);
  newSubst.set(GOAL_GROUP_ID, groupId);
  newSubst.set(GOAL_GROUP_PATH, newPath);
  newSubst.set(GOAL_GROUP_CONJ_GOALS, [
    .../* @__PURE__ */ new Set([...conjGoals, ...conjInnerConj])
  ]);
  newSubst.set(GOAL_GROUP_ALL_GOALS, [
    .../* @__PURE__ */ new Set([
      ...parentOuterGoals,
      ...conjGoals,
      ...disjGoals,
      ...disjInnerAll
    ])
  ]);
  return newSubst;
}
function enrichGroupInput(type, conjGoals, disjGoals, fn) {
  function newInput$(input$) {
    const enrichedInput$ = input$.map(
      (s) => createEnrichedSubst(s, type, conjGoals, disjGoals)
    );
    return fn(enrichedInput$);
  }
  newInput$.conjGoals = conjGoals;
  newInput$.disjGoals = disjGoals;
  Object.defineProperty(newInput$, "name", { value: type });
  return newInput$;
}

// src/core/combinators.ts
function eq(x, y) {
  return enrichGroupInput(
    "eq",
    [],
    [],
    (input$) => new SimpleObservable3((observer) => {
      const sub = input$.subscribe({
        complete: observer.complete,
        error: observer.error,
        next: (subst) => {
          const s2 = unify(x, y, subst);
          if (s2) {
            observer.next(s2);
          }
        }
      });
      return () => sub.unsubscribe();
    })
  );
}
function fresh(f) {
  return (input$) => new SimpleObservable3((observer) => {
    let active = 0;
    let completed = false;
    const subscription = input$.subscribe({
      next: (s) => {
        active++;
        const freshVars = Array.from({ length: f.length }, () => lvar());
        const subGoal = f(...freshVars);
        subGoal(SimpleObservable3.of(s)).subscribe({
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
function disj(g1, g2) {
  return or(g1, g2);
}
function conj(g1, g2) {
  return and(g1, g2);
}
var and = (...goals) => {
  if (goals.length === 0) {
    return (input$) => input$;
  }
  if (goals.length === 1) {
    return goals[0];
  }
  return enrichGroupInput(
    "and",
    goals,
    [],
    (enrichedInput$) => goals.reduce((acc, goal) => goal(acc), enrichedInput$)
  );
};
var or = (...goals) => {
  if (goals.length === 0) {
    return () => SimpleObservable3.empty();
  }
  if (goals.length === 1) {
    return goals[0];
  }
  return enrichGroupInput(
    "or",
    [],
    goals,
    (input$) => {
      return new SimpleObservable3((observer) => {
        const sharedInput$ = input$.share();
        let completedGoals = 0;
        const subscriptions = [];
        const sharedObserver = {
          next: observer.next,
          error: observer.error,
          complete: () => {
            completedGoals++;
            if (completedGoals === goals.length) {
              observer.complete?.();
            }
          }
        };
        for (const goal of goals) {
          const goalSubscription = goal(sharedInput$).subscribe(sharedObserver);
          subscriptions.push(goalSubscription);
        }
        return () => {
          subscriptions.forEach((sub) => sub.unsubscribe?.());
        };
      });
    }
  );
};
function conde(...clauses) {
  const clauseGoals = clauses.map((clause) => and(...clause));
  return or(...clauseGoals);
}
function lift(fn) {
  return (...args) => {
    const out = args[args.length - 1];
    const inputArgs = args.slice(0, -1);
    return (input$) => new SimpleObservable3((observer) => {
      const subscription = input$.subscribe({
        next: (s) => {
          try {
            const resolvedArgs = inputArgs.map((arg) => walk(arg, s));
            const hasVariables = resolvedArgs.some((arg) => isVar(arg));
            if (!hasVariables) {
              const result = fn(...resolvedArgs);
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
  };
}
function eitherOr(firstGoal, secondGoal) {
  return (input$) => new SimpleObservable3((observer) => {
    let active = 0;
    let completed = false;
    const subscription = input$.subscribe({
      next: (s) => {
        active++;
        let hasResults = false;
        const results = [];
        firstGoal(SimpleObservable3.of(s)).subscribe({
          next: (s1) => {
            hasResults = true;
            results.push(s1);
          },
          complete: () => {
            if (hasResults) {
              for (const result of results) {
                observer.next(result);
              }
            } else {
              secondGoal(SimpleObservable3.of(s)).subscribe({
                next: observer.next,
                error: observer.error,
                complete: () => {
                  active--;
                  if (completed && active === 0) observer.complete?.();
                }
              });
              return;
            }
            active--;
            if (completed && active === 0) observer.complete?.();
          },
          error: observer.error
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
function ifte(ifGoal, thenGoal, elseGoal) {
  return (input$) => new SimpleObservable3((observer) => {
    input$.subscribe({
      next: (s) => {
        let succeeded = false;
        const results = [];
        ifGoal(SimpleObservable3.of(s)).subscribe({
          next: (s1) => {
            succeeded = true;
            results.push(s1);
          },
          complete: () => {
            if (succeeded) {
              let completed = 0;
              for (const s1 of results) {
                thenGoal(SimpleObservable3.of(s1)).subscribe({
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
              elseGoal(SimpleObservable3.of(s)).subscribe({
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
function once(goal) {
  return (input$) => goal(input$).take(1);
}
function timeout(goal, timeoutMs) {
  return (input$) => new SimpleObservable3((observer) => {
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
function run(goal, maxResults, timeoutMs) {
  return new Promise((resolve) => {
    const results = [];
    let completed = false;
    let error;
    const effectiveGoal = timeoutMs ? timeout(goal, timeoutMs) : goal;
    const limitedGoal = maxResults ? (input$) => effectiveGoal(input$).take(maxResults) : effectiveGoal;
    limitedGoal(SimpleObservable3.of(/* @__PURE__ */ new Map())).subscribe({
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
function getByPath(obj, path) {
  if (!path) return obj;
  const parts = path.split(".");
  let current = obj;
  for (const part of parts) {
    if (!current) return void 0;
    const match = part.match(/^(\w+)\[\?\(@\.(.+?)==['"](.+?)['"]\)\]$/);
    if (match) {
      const [_, arrKey, filterKey, filterVal] = match;
      const currentObj = current;
      const array = currentObj[arrKey];
      current = (array || []).find((x) => {
        const item = x;
        return item?.[filterKey] === filterVal;
      });
    } else if (part.endsWith("]")) {
      const arrMatch = part.match(/^(\w+)\[(\d+)\]$/);
      if (arrMatch) {
        const [_, arrKey, idx] = arrMatch;
        const currentObj = current;
        const array = currentObj[arrKey];
        current = (array || [])[parseInt(idx)];
      } else {
        const currentObj = current;
        current = currentObj[part];
      }
    } else {
      const currentObj = current;
      current = currentObj[part];
    }
  }
  return current;
}
function project(inputVar, pathOrMap, outputVar) {
  return (input$) => new SimpleObservable3((observer) => {
    const subscription = input$.subscribe({
      next: (s) => {
        const input = walk(inputVar, s);
        if (isVar(input)) return;
        if (input === void 0) return;
        if (typeof pathOrMap === "string") {
          const value = getByPath(input, pathOrMap);
          const unified = unify(outputVar, value, s);
          if (unified !== null) observer.next(unified);
        } else {
          const outObj = {};
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
function projectJsonata(inputVars, jsonataExpr, outputVars) {
  const expr = jsonata(jsonataExpr);
  return (input$) => new SimpleObservable3((observer) => {
    let active = 0;
    let completed = false;
    const subscription = input$.subscribe({
      next: async (s) => {
        active++;
        let inputObj;
        if (typeof inputVars === "object" && inputVars !== null && !isVar(inputVars)) {
          const objInput = {};
          const inputVarsRecord = inputVars;
          for (const key in inputVarsRecord) {
            objInput[key] = walk(inputVarsRecord[key], s);
          }
          inputObj = objInput;
        } else {
          inputObj = walk(inputVars, s);
        }
        let result;
        try {
          result = await expr.evaluate(inputObj);
        } catch (e) {
          observer.error?.(e);
          active--;
          if (completed && active === 0) observer.complete?.();
          return;
        }
        if (typeof outputVars === "object" && outputVars !== null && !isVar(outputVars)) {
          const outputVarsRecord = outputVars;
          if (result && typeof result === "object" && "then" in result && typeof result.then === "function") {
            result.then((resolved) => {
              let currentSubst = s;
              for (const key in outputVarsRecord) {
                const value = resolved && typeof resolved === "object" && resolved !== null ? resolved[key] : void 0;
                const unified = unify(
                  outputVarsRecord[key],
                  value,
                  currentSubst
                );
                if (unified !== null) {
                  currentSubst = unified;
                } else {
                  active--;
                  if (completed && active === 0) observer.complete?.();
                  return;
                }
              }
              observer.next(currentSubst);
              active--;
              if (completed && active === 0) observer.complete?.();
            }).catch((e) => {
              observer.error?.(e);
              active--;
              if (completed && active === 0) observer.complete?.();
            });
          } else {
            const resolved = result;
            let currentSubst = s;
            for (const key in outputVarsRecord) {
              const value = resolved && typeof resolved === "object" && resolved !== null ? resolved[key] : void 0;
              const unified = unify(
                outputVarsRecord[key],
                value,
                currentSubst
              );
              if (unified !== null) {
                currentSubst = unified;
              } else {
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
          if (result && typeof result === "object" && "then" in result && typeof result.then === "function") {
            result.then((resolved) => {
              const unified = unify(outputVars, resolved, s);
              if (unified !== null) observer.next(unified);
              active--;
              if (completed && active === 0) observer.complete?.();
            }).catch((e) => {
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
function Subquery(goal, extractVar, bindVar, aggregator = (results, _) => arrayToLogicList(results)) {
  return enrichGroupInput(
    "Subquery",
    [],
    [goal],
    (input$) => input$.flatMap((s) => {
      const extracted = [];
      return new SimpleObservable3((observer) => {
        const subgoalSubscription = goal(SimpleObservable3.of(s)).subscribe({
          next: (subResult) => {
            const value = walk(extractVar, subResult);
            extracted.push(value);
          },
          error: (error) => {
            extracted.length = 0;
            observer.error?.(error);
          },
          complete: () => {
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
function branch(goal, aggregator) {
  return enrichGroupInput(
    "branch",
    [],
    [goal],
    (input$) => new SimpleObservable3((observer) => {
      const goalSubs = [];
      const inputSub = input$.subscribe({
        error: observer.error,
        complete: observer.complete,
        next: (inputSubst) => {
          const collectedSubsts = [];
          const goalSub = goal(SimpleObservable3.of(inputSubst)).subscribe({
            error: observer.error,
            complete: () => {
              aggregator(observer, collectedSubsts, inputSubst);
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
        goalSubs.forEach((goalSub) => goalSub.unsubscribe());
        inputSub.unsubscribe();
      };
    })
  );
}

// src/core/query.ts
import { SimpleObservable as SimpleObservable4 } from "@swiftfall/observable";
function deepListWalk(val) {
  if (isLogicList(val)) {
    return logicListToArray(val).map(deepListWalk);
  } else if (Array.isArray(val)) {
    return val.map(deepListWalk);
  } else if (val && typeof val === "object" && !isVar(val)) {
    const out = {};
    for (const k in val) {
      if (Object.hasOwn(val, k)) {
        out[k] = deepListWalk(val[k]);
      }
    }
    return out;
  }
  return val;
}
function createLogicVarProxy(prefix = "") {
  const varMap = /* @__PURE__ */ new Map();
  const proxy = new Proxy({}, {
    get(_target, prop) {
      if (typeof prop !== "string") return void 0;
      if (prop === "_") return lvar();
      if (!varMap.has(prop)) {
        varMap.set(prop, lvar(`${prefix}${String(prop)}`));
      }
      return varMap.get(prop);
    },
    has: () => true,
    ownKeys: () => Array.from(varMap.keys()),
    getOwnPropertyDescriptor: () => ({
      enumerable: true,
      configurable: true
    })
  });
  return {
    proxy,
    varMap
  };
}
function formatSubstitutions(substs, formatter, limit) {
  const limitedSubsts = limit === Infinity ? substs : substs.take(limit);
  return {
    subscribe(observer) {
      const unsub = limitedSubsts.subscribe({
        next: (s) => {
          const result = {};
          for (const key in formatter) {
            if (key.startsWith("_")) continue;
            const term = formatter[key];
            result[key] = walk(term, s);
          }
          observer.next(deepListWalk(result));
        },
        error: observer.error,
        complete: observer.complete
      });
      if (typeof unsub === "function") return unsub;
      if (unsub && typeof unsub.unsubscribe === "function")
        return () => unsub.unsubscribe();
      return function noop() {
      };
    }
  };
}
var Query = class {
  _formatter = null;
  _rawSelector = null;
  _goals = [];
  _limit = Infinity;
  _logicVarProxy;
  _selectAllVars = false;
  constructor() {
    const { proxy } = createLogicVarProxy("q_");
    this._logicVarProxy = proxy;
    this._selectAllVars = true;
  }
  select(selector) {
    if (selector === "*") {
      this._formatter = null;
      this._rawSelector = null;
      this._selectAllVars = true;
    } else if (typeof selector === "function") {
      this._rawSelector = null;
      this._selectAllVars = false;
      this._formatter = selector(this._logicVarProxy);
    } else {
      this._formatter = null;
      this._selectAllVars = false;
      this._rawSelector = selector;
    }
    return this;
  }
  /**
   * Adds constraints (goals) to the query.
   */
  where(goalFn) {
    const result = goalFn(this._logicVarProxy);
    this._goals.push(...Array.isArray(result) ? result : [result]);
    return this;
  }
  /**
   * Sets the maximum number of results.
   */
  limit(n) {
    this._limit = n;
    return this;
  }
  getSubstObservale() {
    const initialSubst = /* @__PURE__ */ new Map();
    const combinedGoal = and(...this._goals);
    const substStream = combinedGoal(SimpleObservable4.of(initialSubst));
    return substStream;
  }
  getObservable() {
    if (this._goals.length === 0) {
      throw new Error("Query must have at least one .where() clause.");
    }
    let formatter = this._formatter;
    if (this._selectAllVars) {
      formatter = {
        ...this._logicVarProxy
      };
    } else if (this._rawSelector) {
      formatter = {
        result: this._rawSelector
      };
    } else if (!formatter) {
      formatter = {
        ...this._logicVarProxy
      };
    }
    const initialSubst = /* @__PURE__ */ new Map();
    const combinedGoal = and(...this._goals);
    const substStream = combinedGoal(SimpleObservable4.of(initialSubst));
    const results = formatSubstitutions(substStream, formatter, this._limit);
    const rawSelector = this._rawSelector;
    return {
      subscribe(observer) {
        return results.subscribe({
          next: (result) => {
            if (rawSelector) {
              observer.next(result.result);
            } else {
              observer.next(result);
            }
          },
          error: observer.error,
          complete: observer.complete
        });
      }
    };
  }
  /**
   * Makes the Query object itself an async iterable.
   * Properly propagates cancellation upstream when the consumer stops early.
   */
  async *[Symbol.asyncIterator]() {
    const observable = this.getObservable();
    const queue = [];
    let completed = false;
    let error = null;
    let resolveNext = null;
    const nextPromise = () => new Promise((resolve) => {
      resolveNext = resolve;
    });
    const subcription = observable.subscribe({
      next: (result) => {
        queue.push(result);
        if (resolveNext) {
          resolveNext();
          resolveNext = null;
        }
      },
      error: (err) => {
        error = err;
        completed = true;
        if (resolveNext) {
          resolveNext();
          resolveNext = null;
        }
      },
      complete: () => {
        completed = true;
        if (resolveNext) {
          resolveNext();
          resolveNext = null;
        }
      }
    });
    try {
      while (!completed || queue.length > 0) {
        if (queue.length === 0) {
          await nextPromise();
        }
        while (queue.length > 0) {
          const item = queue.shift();
          if (item !== void 0) {
            yield item;
          }
        }
        if (error) throw error;
      }
    } finally {
      subcription.unsubscribe?.();
    }
  }
  /**
   * Executes the query and returns all results as an array.
   */
  async toArray() {
    const observable = this.getObservable();
    const results = [];
    return new Promise((resolve, reject) => {
      observable.subscribe({
        next: (result) => {
          results.push(result);
        },
        error: reject,
        complete: () => resolve(results)
      });
    });
  }
  /**
   * Returns the observable stream directly for reactive programming.
   */
  toObservable() {
    return this.getObservable();
  }
};
function query() {
  return new Query();
}

// src/relations/aggregates.ts
import { SimpleObservable as SimpleObservable6 } from "@swiftfall/observable";

// src/relations/aggregates-base.ts
import { SimpleObservable as SimpleObservable5 } from "@swiftfall/observable";
function collect_and_process_base(processor) {
  return (input$) => new SimpleObservable5((observer) => {
    const buffer = [];
    const subscription = input$.subscribe({
      next: (item) => buffer.push(item),
      error: (error) => {
        buffer.length = 0;
        observer.error?.(error);
      },
      complete: () => {
        processor(buffer, observer);
        buffer.length = 0;
        observer.complete?.();
      }
    });
    return () => {
      subscription.unsubscribe?.();
      buffer.length = 0;
    };
  });
}
function group_by_streamo_base(keyVar, valueVar, outVar, drop, aggregator) {
  return (input$) => new SimpleObservable5((observer) => {
    const groups = /* @__PURE__ */ new Map();
    const subscription = input$.subscribe({
      next: (s) => {
        const key = walk(keyVar, s);
        const keyStr = JSON.stringify(key);
        if (!groups.has(keyStr)) {
          groups.set(keyStr, {
            key,
            values: [],
            substitutions: []
          });
        }
        const group = groups.get(keyStr);
        if (valueVar !== null) {
          const value = walk(valueVar, s);
          group.values.push(value);
        }
        group.substitutions.push(s);
      },
      error: (error) => {
        groups.clear();
        observer.error?.(error);
      },
      complete: () => {
        if (drop) {
          for (const { key, values, substitutions } of groups.values()) {
            const aggregated = aggregator(values, substitutions);
            const subst = /* @__PURE__ */ new Map();
            const subst1 = unify(keyVar, key, subst);
            if (subst1 === null) continue;
            const subst2 = unify(outVar, aggregated, subst1);
            if (subst2 === null) continue;
            observer.next(subst2);
          }
        } else {
          for (const { key, values, substitutions } of groups.values()) {
            const aggregated = aggregator(values, substitutions);
            for (const subst of substitutions) {
              const subst1 = unify(keyVar, key, subst);
              if (subst1 === null) continue;
              const subst2 = unify(outVar, aggregated, subst1);
              if (subst2 === null) continue;
              observer.next(subst2);
            }
          }
        }
        groups.clear();
        observer.complete?.();
      }
    });
    return () => {
      subscription.unsubscribe?.();
      groups.clear();
    };
  });
}

// src/relations/aggregates.ts
function count_value_streamo(x, value, count) {
  return (input$) => new SimpleObservable6((observer) => {
    const substitutions = [];
    const subscription = input$.subscribe({
      next: (s) => substitutions.push(s),
      error: (error) => {
        substitutions.length = 0;
        observer.error?.(error);
      },
      complete: () => {
        let n = 0;
        for (const s of substitutions) {
          const val = walk(x, s);
          const target = walk(value, s);
          if (JSON.stringify(val) === JSON.stringify(target)) n++;
        }
        eq(
          count,
          n
        )(SimpleObservable6.of(/* @__PURE__ */ new Map())).subscribe({
          next: observer.next,
          error: observer.error,
          complete: () => {
            substitutions.length = 0;
            observer.complete?.();
          }
        });
      }
    });
    return () => {
      subscription.unsubscribe?.();
      substitutions.length = 0;
    };
  });
}
function group_by_count_streamo(x, count, drop = false) {
  return group_by_streamo_base(
    x,
    // keyVar
    null,
    // valueVar (not needed for counting)
    count,
    // outVar
    drop,
    // drop
    (_, substitutions) => substitutions.length
    // aggregator: count substitutions
  );
}
function sort_by_streamo(x, orderOrFn) {
  return collect_and_process_base(
    (buffer, observer) => {
      const pairs = buffer.map((subst) => ({
        value: walk(x, subst),
        subst
      }));
      const orderFn = (() => {
        if (typeof orderOrFn === "function") {
          return orderOrFn;
        }
        if (typeof orderOrFn === "string") {
          if (orderOrFn === "desc") {
            return descComparator;
          }
        }
        return ascComparator;
      })();
      const comparator = (a, b) => orderFn(a.value, b.value);
      pairs.sort(comparator);
      for (const { subst } of pairs) {
        observer.next(subst);
      }
    }
  );
}
var descComparator = (a, b) => {
  if (a < b) return 1;
  if (a > b) return -1;
  return 0;
};
var ascComparator = (a, b) => {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
};
function take_streamo(n) {
  return (input$) => new SimpleObservable6((observer) => {
    let count = 0;
    const subscription = input$.subscribe({
      next: (item) => {
        if (count < n) {
          observer.next(item);
          count++;
          if (count === n) {
            observer.complete?.();
            subscription.unsubscribe?.();
          }
        }
      },
      error: observer.error,
      complete: observer.complete
    });
    return () => subscription.unsubscribe?.();
  });
}
function group_by_collect_streamo(keyVar, valueVar, outList, drop = false) {
  return group_by_streamo_base(
    keyVar,
    // keyVar
    valueVar,
    // valueVar
    outList,
    // outVar
    drop,
    // drop
    (values, _) => arrayToLogicList(values)
    // aggregator: collect into list
  );
}
function group_by_collect_distinct_streamo(keyVar, valueVar, outList, drop = false) {
  return group_by_streamo_base(
    keyVar,
    // keyVar
    valueVar,
    // valueVar
    outList,
    // outVar
    drop,
    // drop
    (values, _) => arrayToLogicList([...new Set(values)])
    // aggregator: collect into list
  );
}
function collect_streamo(valueVar, outList, drop = false) {
  return collect_and_process_base((buffer, observer) => {
    const results = buffer.map((x) => walk(valueVar, x));
    let s;
    if (drop) {
      s = /* @__PURE__ */ new Map();
    } else {
      s = buffer[0] ?? /* @__PURE__ */ new Map();
    }
    const newSubst = unify(results, outList, s);
    if (newSubst) {
      observer.next(newSubst);
    }
  });
}

// src/relations/aggregates-subqueries.ts
function aggregateRelFactory(aggFn, dedup = false) {
  return (x, goal, out) => {
    return enrichGroupInput(
      "aggregateRelFactory",
      [],
      [goal],
      Subquery(
        goal,
        x,
        // extract x from each subgoal result
        out,
        // bind the aggregated result to this variable
        (extractedValues, _) => {
          const values = dedup ? deduplicate(extractedValues) : extractedValues;
          return aggFn(values);
        }
      )
    );
  };
}
var collecto = aggregateRelFactory(
  (arr) => arrayToLogicList(arr),
  false
);
var collect_distincto = aggregateRelFactory(
  (arr) => arrayToLogicList(arr),
  true
);
var counto = aggregateRelFactory((arr) => arr.length, false);
var count_distincto = aggregateRelFactory((arr) => arr.length, true);
function count_valueo(x, goal, value, count) {
  return Subquery(
    goal,
    x,
    // extract x from each subgoal result
    count,
    // bind the count to this variable
    (extractedValues, originalSubst) => {
      const targetValue = walk(value, originalSubst);
      return extractedValues.filter(
        (val) => JSON.stringify(val) === JSON.stringify(targetValue)
      ).length;
    }
  );
}
function groupAggregateRelFactory(aggFn, dedup = false) {
  return (keyVar, valueVar, goal, outValueAgg) => {
    const group_by_rel = dedup ? group_by_collect_distinct_streamo : group_by_collect_streamo;
    const aggFnName = aggFn?.displayName || aggFn.name || "unknown";
    return enrichGroupInput(
      `groupAggregateRelFactory ${aggFnName}`,
      [],
      [goal],
      fresh(
        (in_outValueAgg) => branch(
          and(goal, group_by_rel(keyVar, valueVar, in_outValueAgg, true)),
          (observer, substs, subst) => {
            for (const oneSubst of substs) {
              const keyVal = walk(keyVar, oneSubst);
              if (isVar(keyVal)) {
                continue;
              }
              const valueAggVal = walk(in_outValueAgg, oneSubst);
              if (isVar(valueAggVal)) {
                continue;
              }
              const convertedAgg = aggFn(valueAggVal);
              const s2 = unify(keyVar, keyVal, subst);
              if (!s2) continue;
              const s3 = unify(outValueAgg, convertedAgg, s2);
              if (!s3) continue;
              observer.next(s3);
            }
          }
        )
      )
    );
  };
}
var group_by_collecto = groupAggregateRelFactory(
  function group_by_collecto2(x) {
    return x;
  }
);
var group_by_counto = groupAggregateRelFactory(
  function group_by_counto2(items) {
    return logicListToArray(items).length;
  }
);
function deduplicate(items) {
  const seen = /* @__PURE__ */ new Set();
  const result = [];
  for (const item of items) {
    const k = JSON.stringify(item);
    if (!seen.has(k)) {
      seen.add(k);
      result.push(item);
    }
  }
  return result;
}

// src/relations/control.ts
import util from "util";
import { SimpleObservable as SimpleObservable7 } from "@swiftfall/observable";
var uniqueo = (t, g) => enrichGroupInput(
  "uniqueo",
  [g],
  [],
  (input$) => input$.flatMap((s) => {
    const seen = /* @__PURE__ */ new Set();
    return g(SimpleObservable7.of(s)).flatMap((s2) => {
      const w_t = walk(t, s2);
      if (isVar(w_t)) {
        return SimpleObservable7.of(s2);
      }
      const key = JSON.stringify(w_t);
      if (seen.has(key)) return SimpleObservable7.empty();
      seen.add(key);
      return SimpleObservable7.of(s2);
    });
  })
);
function not(goal) {
  return enrichGroupInput(
    "not",
    [],
    [goal],
    (input$) => input$.flatMap((s) => {
      return new SimpleObservable7((observer) => {
        let hasSolutions = false;
        const sub = goal(SimpleObservable7.of(s)).subscribe({
          next: (subst) => {
            if (!subst.has(SUSPENDED_CONSTRAINTS)) {
              hasSolutions = true;
            }
          },
          error: (err) => observer.error?.(err),
          complete: () => {
            if (!hasSolutions) {
              observer.next(s);
            }
            observer.complete?.();
          }
        });
        return () => sub.unsubscribe();
      });
    })
  );
}
function gv1_not(goal) {
  return enrichGroupInput(
    "not",
    [],
    [goal],
    (input$) => input$.flatMap((s) => {
      return new SimpleObservable7((observer) => {
        let hasSolutions = false;
        const sub = goal(SimpleObservable7.of(s)).subscribe({
          next: () => {
            hasSolutions = true;
          },
          error: (err) => observer.error?.(err),
          complete: () => {
            if (!hasSolutions) {
              observer.next(s);
            }
            observer.complete?.();
          }
        });
        return () => sub.unsubscribe();
      });
    })
  );
}
function old_not(goal) {
  return enrichGroupInput(
    "not",
    [],
    [goal],
    (input$) => input$.flatMap((s) => {
      let found = false;
      return new SimpleObservable7((observer) => {
        goal(SimpleObservable7.of(s)).subscribe({
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
function neqo(x, y) {
  return suspendable(
    [x, y],
    (values, subst) => {
      const [xVal, yVal] = values;
      const xGrounded = !isVar(xVal);
      const yGrounded = !isVar(yVal);
      if (xGrounded && yGrounded) {
        return xVal !== yVal ? subst : null;
      }
      if (!xGrounded && !yGrounded) {
        if (xVal.id === yVal.id) {
          return null;
        }
      }
      return CHECK_LATER;
    },
    0
  );
}
function old_neqo(x, y) {
  return suspendable(
    [x, y],
    (values, subst) => {
      return CHECK_LATER;
      const [xVal, yVal] = values;
      const xGrounded = !isVar(xVal);
      const yGrounded = !isVar(yVal);
      if (xGrounded && yGrounded) {
        return xVal !== yVal ? subst : null;
      }
      return CHECK_LATER;
    },
    0
  );
}
function onceo(goal) {
  return (input$) => goal(input$).take(1);
}
function succeedo() {
  return (input$) => input$.flatMap(
    (s) => new SimpleObservable7((observer) => {
      observer.next(s);
      observer.complete?.();
    })
  );
}
function failo() {
  return (_input$) => SimpleObservable7.empty();
}
function groundo(term) {
  return (input$) => input$.flatMap(
    (s) => new SimpleObservable7((observer) => {
      const walked = walk(term, s);
      function isGround(t) {
        if (isVar(t)) return false;
        if (Array.isArray(t)) {
          return t.every(isGround);
        }
        if (t && typeof t === "object" && "tag" in t) {
          if (t.tag === "cons") {
            const l = t;
            return isGround(l.head) && isGround(l.tail);
          }
          if (t.tag === "nil") {
            return true;
          }
        }
        if (t && typeof t === "object" && !("tag" in t)) {
          return Object.values(t).every(isGround);
        }
        return true;
      }
      if (isGround(walked)) {
        observer.next(s);
      }
      observer.complete?.();
    })
  );
}
function nonGroundo(term) {
  return not(groundo(term));
}
function substLog(msg, onlyVars = false) {
  return enrichGroupInput(
    "substLog",
    [],
    [],
    (input$) => new SimpleObservable7((observer) => {
      const sub = input$.subscribe({
        next: (s) => {
          const ns = onlyVars ? Object.fromEntries(
            [...s.entries()].filter(([k, v]) => typeof k === "string")
          ) : s;
          console.log(
            `[substLog] ${msg}:`,
            util.inspect(ns, {
              depth: null,
              colors: true
            })
          );
          observer.next(s);
        },
        error: observer.error,
        complete: observer.complete
      });
      return () => sub.unsubscribe();
    })
  );
}
var thruCountId = 0;
function thruCount(msg, level = 1e3) {
  const id = ++thruCountId;
  return enrichGroupInput(
    "thruCount",
    [],
    [],
    (input$) => new SimpleObservable7((observer) => {
      let cnt = 0;
      const sub = input$.subscribe({
        next: (s) => {
          cnt++;
          let currentLevel = 1;
          if (cnt >= 10) currentLevel = 10;
          if (cnt >= 100) currentLevel = 100;
          if (cnt >= 1e3) currentLevel = 1e3;
          if (cnt % currentLevel === 0) {
            let nonSymbolKeyCount = 0;
            for (const key of s.keys()) {
              if (typeof key !== "symbol") nonSymbolKeyCount++;
            }
            const suspendedCount = getSuspendsFromSubst(s).length;
            console.log("THRU", id, msg, cnt, {
              nonSymbolKeyCount,
              suspendedCount
            });
          }
          observer.next(s);
        },
        error: observer.error,
        complete: () => {
          console.log("THRU COMPLETE", id, msg, cnt);
          observer.complete?.();
        }
      });
      return () => sub.unsubscribe();
    })
  );
}
function fail() {
  return (input$) => new SimpleObservable7((observer) => {
    const sub = input$.subscribe({
      next: (s) => {
      },
      error: observer.error,
      complete: observer.complete
    });
    return () => sub.unsubscribe();
  });
}

// src/relations/lists.ts
import { SimpleObservable as SimpleObservable8 } from "@swiftfall/observable";
function membero(x, list) {
  return enrichGroupInput(
    "membero",
    [],
    [],
    (input$) => new SimpleObservable8((observer) => {
      const subscriptions = [];
      let cancelled = false;
      let active = 0;
      let inputComplete = false;
      const checkComplete = () => {
        if (inputComplete && active === 0 && !cancelled) {
          observer.complete?.();
        }
      };
      const inputSub = input$.subscribe({
        next: (s) => {
          if (cancelled) return;
          const l = walk(list, s);
          if (Array.isArray(l)) {
            for (let i = 0; i < l.length; i++) {
              if (cancelled) break;
              const item = l[i];
              const s2 = unify(x, item, s);
              if (s2) observer.next(s2);
            }
          } else if (l && typeof l === "object" && "tag" in l && l.tag === "cons") {
            if (cancelled) return;
            const s1 = unify(x, l.head, s);
            if (s1) observer.next(s1);
            active++;
            const sub = membero(
              x,
              l.tail
            )(SimpleObservable8.of(s)).subscribe({
              next: (result) => {
                if (!cancelled) observer.next(result);
              },
              error: (err) => {
                if (!cancelled) observer.error?.(err);
              },
              complete: () => {
                active--;
                checkComplete();
              }
            });
            subscriptions.push(sub);
          }
        },
        error: (err) => {
          if (!cancelled) observer.error?.(err);
        },
        complete: () => {
          inputComplete = true;
          checkComplete();
        }
      });
      subscriptions.push(inputSub);
      return () => {
        cancelled = true;
        subscriptions.forEach((sub) => {
          try {
            sub?.unsubscribe?.();
          } catch (e) {
          }
        });
        subscriptions.length = 0;
      };
    })
  );
}
function firsto(x, xs) {
  return (input$) => new SimpleObservable8((observer) => {
    input$.subscribe({
      next: (s) => {
        const l = walk(xs, s);
        if (isCons(l)) {
          const consNode = l;
          const s1 = unify(x, consNode.head, s);
          if (s1) observer.next(s1);
        }
        observer.complete?.();
      },
      error: observer.error,
      complete: observer.complete
    });
  });
}
function resto(xs, tail) {
  return (input$) => new SimpleObservable8((observer) => {
    input$.subscribe({
      next: (s) => {
        const l = walk(xs, s);
        if (isCons(l)) {
          const consNode = l;
          const s1 = unify(tail, consNode.tail, s);
          if (s1) observer.next(s1);
        }
        observer.complete?.();
      },
      error: observer.error,
      complete: observer.complete
    });
  });
}
function appendo(xs, ys, zs) {
  return (input$) => new SimpleObservable8((observer) => {
    input$.subscribe({
      next: (s) => {
        const xsVal = walk(xs, s);
        if (isCons(xsVal)) {
          const consNode = xsVal;
          const head = consNode.head;
          const tail = consNode.tail;
          const rest = lvar();
          const s1 = unify(
            zs,
            {
              tag: "cons",
              head,
              tail: rest
            },
            s
          );
          if (s1) {
            appendo(
              tail,
              ys,
              rest
            )(SimpleObservable8.of(s1)).subscribe({
              next: observer.next,
              error: observer.error,
              complete: observer.complete
            });
            return;
          }
        } else if (isNil(xsVal)) {
          const s1 = unify(ys, zs, s);
          if (s1) observer.next(s1);
        }
        observer.complete?.();
      },
      error: observer.error,
      complete: observer.complete
    });
  });
}
function lengtho(arrayOrList, length) {
  return (input$) => new SimpleObservable8((observer) => {
    input$.subscribe({
      next: (s) => {
        const walkedArray = walk(arrayOrList, s);
        const walkedLength = walk(length, s);
        let actualLength;
        if (isLogicList(walkedArray)) {
          actualLength = logicListToArray(walkedArray).length;
        } else if (Array.isArray(walkedArray)) {
          actualLength = walkedArray.length;
        } else {
          return;
        }
        const unified = unify(actualLength, walkedLength, s);
        if (unified !== null) {
          observer.next(unified);
        }
      },
      error: observer.error,
      complete: observer.complete
    });
  });
}
function permuteo(xs, ys) {
  return (input$) => new SimpleObservable8((observer) => {
    input$.subscribe({
      next: (s) => {
        const xsVal = walk(xs, s);
        if (isNil(xsVal)) {
          eq(
            ys,
            nil
          )(SimpleObservable8.of(s)).subscribe({
            next: observer.next,
            error: observer.error,
            complete: observer.complete
          });
          return;
        }
        if (isCons(xsVal)) {
          const arr = logicListToArray(xsVal);
          let completedCount = 0;
          for (const head of arr) {
            const rest = lvar();
            and(
              removeFirsto(xsVal, head, rest),
              permuteo(rest, lvar()),
              eq(ys, cons(head, lvar()))
            )(SimpleObservable8.of(s)).subscribe({
              next: (s1) => {
                const ysVal2 = walk(ys, s1);
                if (isCons(ysVal2)) {
                  eq(
                    ysVal2.tail,
                    walk(lvar(), s1)
                  )(SimpleObservable8.of(s1)).subscribe({
                    next: observer.next,
                    error: observer.error
                  });
                }
              },
              error: observer.error,
              complete: () => {
                completedCount++;
                if (completedCount === arr.length) {
                  observer.complete?.();
                }
              }
            });
          }
          if (arr.length === 0) {
            observer.complete?.();
          }
        } else {
          observer.complete?.();
        }
      },
      error: observer.error,
      complete: observer.complete
    });
  });
}
function mapo(rel, xs, ys) {
  return (input$) => new SimpleObservable8((observer) => {
    let active = 0;
    let completed = false;
    const subscription = input$.subscribe({
      next: (s) => {
        active++;
        const xsVal = walk(xs, s);
        if (isNil(xsVal)) {
          eq(
            ys,
            nil
          )(SimpleObservable8.of(s)).subscribe({
            next: observer.next,
            error: observer.error,
            complete: () => {
              active--;
              if (completed && active === 0) observer.complete?.();
            }
          });
          return;
        }
        if (isCons(xsVal)) {
          const xHead = xsVal.head;
          const xTail = xsVal.tail;
          const yHead = lvar();
          const yTail = lvar();
          and(
            eq(ys, cons(yHead, yTail)),
            rel(xHead, yHead),
            mapo(rel, xTail, yTail)
          )(SimpleObservable8.of(s)).subscribe({
            next: observer.next,
            error: observer.error,
            complete: () => {
              active--;
              if (completed && active === 0) observer.complete?.();
            }
          });
        } else {
          active--;
          if (completed && active === 0) observer.complete?.();
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
function removeFirsto(xs, x, ys) {
  return (input$) => new SimpleObservable8((observer) => {
    let active = 0;
    let completed = false;
    const subscription = input$.subscribe({
      next: (s) => {
        active++;
        const xsVal = walk(xs, s);
        if (isNil(xsVal)) {
          active--;
          if (completed && active === 0) observer.complete?.();
          return;
        }
        if (isCons(xsVal)) {
          const walkedX = walk(x, s);
          const walkedHead = walk(xsVal.head, s);
          if (JSON.stringify(walkedHead) === JSON.stringify(walkedX)) {
            eq(
              ys,
              xsVal.tail
            )(SimpleObservable8.of(s)).subscribe({
              next: observer.next,
              error: observer.error,
              complete: () => {
                active--;
                if (completed && active === 0) observer.complete?.();
              }
            });
          } else {
            const rest = lvar();
            and(
              eq(ys, cons(xsVal.head, rest)),
              removeFirsto(xsVal.tail, x, rest)
            )(SimpleObservable8.of(s)).subscribe({
              next: observer.next,
              error: observer.error,
              complete: () => {
                active--;
                if (completed && active === 0) observer.complete?.();
              }
            });
          }
        } else {
          active--;
          if (completed && active === 0) observer.complete?.();
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
function alldistincto(xs) {
  return (input$) => new SimpleObservable8((observer) => {
    input$.subscribe({
      next: (s) => {
        const arr = walk(xs, s);
        let jsArr = [];
        if (arr && typeof arr === "object" && "tag" in arr) {
          let cur = arr;
          while (isCons(cur)) {
            jsArr.push(cur.head);
            cur = cur.tail;
          }
        } else if (Array.isArray(arr)) {
          jsArr = arr;
        }
        const seen = /* @__PURE__ */ new Set();
        let allDistinct = true;
        for (const v of jsArr) {
          const key = JSON.stringify(v);
          if (seen.has(key)) {
            allDistinct = false;
            break;
          }
          seen.add(key);
        }
        if (allDistinct) observer.next(s);
        observer.complete?.();
      },
      error: observer.error,
      complete: observer.complete
    });
  });
}

// src/relations/numeric.ts
import { SimpleObservable as SimpleObservable9 } from "@swiftfall/observable";
function gto(x, y) {
  return suspendable(
    [x, y],
    (values, subst) => {
      const [xVal, yVal] = values;
      const xGrounded = !isVar(xVal);
      const yGrounded = !isVar(yVal);
      if (xGrounded && yGrounded) {
        return xVal > yVal ? subst : null;
      }
      return CHECK_LATER;
    },
    2
  );
}
function lto(x, y) {
  return suspendable(
    [x, y],
    (values, subst) => {
      const [xVal, yVal] = values;
      const xGrounded = !isVar(xVal);
      const yGrounded = !isVar(yVal);
      if (xGrounded && yGrounded) {
        return xVal < yVal ? subst : null;
      }
      return CHECK_LATER;
    },
    2
  );
}
function gteo(x, y) {
  return suspendable(
    [x, y],
    (values, subst) => {
      const [xVal, yVal] = values;
      const xGrounded = !isVar(xVal);
      const yGrounded = !isVar(yVal);
      if (xGrounded && yGrounded) {
        return xVal >= yVal ? subst : null;
      }
      return CHECK_LATER;
    },
    2
  );
}
function lteo(x, y) {
  return suspendable(
    [x, y],
    (values, subst) => {
      const [xVal, yVal] = values;
      const xGrounded = !isVar(xVal);
      const yGrounded = !isVar(yVal);
      if (xGrounded && yGrounded) {
        return xVal <= yVal ? subst : null;
      }
      return CHECK_LATER;
    },
    2
  );
}
function pluso(x, y, z) {
  return suspendable([x, y, z], (values, subst) => {
    const [xVal, yVal, zVal] = values;
    const xGrounded = !isVar(xVal);
    const yGrounded = !isVar(yVal);
    const zGrounded = !isVar(zVal);
    if (xGrounded && yGrounded && zGrounded) {
      return xVal + yVal === zVal ? subst : null;
    } else if (xGrounded && yGrounded) {
      return unify(z, xVal + yVal, subst);
    } else if (xGrounded && zGrounded) {
      return unify(y, zVal - xVal, subst);
    } else if (yGrounded && zGrounded) {
      return unify(x, zVal - yVal, subst);
    }
    return CHECK_LATER;
  });
}
var minuso = (x, y, z) => pluso(z, y, x);
function multo(x, y, z) {
  return suspendable([x, y, z], (values, subst) => {
    const [xVal, yVal, zVal] = values;
    const xGrounded = !isVar(xVal);
    const yGrounded = !isVar(yVal);
    const zGrounded = !isVar(zVal);
    if (xGrounded && yGrounded && zGrounded) {
      return xVal * yVal === zVal ? subst : null;
    }
    if (xGrounded && yGrounded) {
      return unify(z, xVal * yVal, subst);
    }
    if (zGrounded && zVal !== 0) {
      if (xGrounded && xVal === 0) return null;
      if (yGrounded && yVal === 0) return null;
    }
    if (xGrounded && zGrounded) {
      return unify(y, zVal / xVal, subst);
    } else if (yGrounded && zGrounded) {
      return unify(x, zVal / yVal, subst);
    }
    return CHECK_LATER;
  });
}
var dividebyo = (x, y, z) => multo(z, y, x);
function maxo(variable) {
  return (input$) => new SimpleObservable9((observer) => {
    const substitutions = [];
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
        let maxValue;
        const maxSubstitutions = [];
        for (const s of substitutions) {
          const value = walk(variable, s);
          if (typeof value === "number") {
            if (maxValue === void 0 || value > maxValue) {
              maxValue = value;
              maxSubstitutions.length = 0;
              maxSubstitutions.push(s);
            } else if (value === maxValue) {
              maxSubstitutions.push(s);
            }
          }
        }
        for (const s of maxSubstitutions) {
          observer.next(s);
        }
        observer.complete?.();
      }
    });
    return () => subscription.unsubscribe?.();
  });
}
function mino(variable) {
  return (input$) => new SimpleObservable9((observer) => {
    const substitutions = [];
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
        let minValue;
        const minSubstitutions = [];
        for (const s of substitutions) {
          const value = walk(variable, s);
          if (typeof value === "number") {
            if (minValue === void 0 || value < minValue) {
              minValue = value;
              minSubstitutions.length = 0;
              minSubstitutions.push(s);
            } else if (value === minValue) {
              minSubstitutions.push(s);
            }
          }
        }
        for (const s of minSubstitutions) {
          observer.next(s);
        }
        observer.complete?.();
      }
    });
    return () => subscription.unsubscribe?.();
  });
}

// src/relations/objects.ts
import { SimpleObservable as SimpleObservable10 } from "@swiftfall/observable";
function extract(inputVar, mapping) {
  return (input$) => input$.flatMap(
    (s) => new SimpleObservable10((observer) => {
      const inputValue = walk(inputVar, s);
      if (typeof inputValue !== "object" || inputValue === null) {
        observer.complete?.();
        return;
      }
      const extractRecursive = (sourceValue, targetMapping, currentSubst2) => {
        if (isVar(targetMapping)) {
          return unify(targetMapping, sourceValue, currentSubst2);
        } else if (Array.isArray(targetMapping)) {
          if (!Array.isArray(sourceValue) || sourceValue.length !== targetMapping.length) {
            return null;
          }
          let resultSubst = currentSubst2;
          for (let i = 0; i < targetMapping.length; i++) {
            const nextSubst = extractRecursive(
              sourceValue[i],
              targetMapping[i],
              resultSubst
            );
            if (nextSubst === null) return null;
            resultSubst = nextSubst;
          }
          return resultSubst;
        } else if (typeof targetMapping === "object" && targetMapping !== null) {
          if (typeof sourceValue !== "object" || sourceValue === null) {
            return null;
          }
          let resultSubst = currentSubst2;
          for (const [key, targetValue] of Object.entries(targetMapping)) {
            const sourceNestedValue = sourceValue[key];
            const nextSubst = extractRecursive(
              sourceNestedValue,
              targetValue,
              resultSubst
            );
            if (nextSubst === null) return null;
            resultSubst = nextSubst;
          }
          return resultSubst;
        } else {
          return sourceValue === targetMapping ? currentSubst2 : null;
        }
      };
      let currentSubst = s;
      for (const [key, outputMapping] of Object.entries(mapping)) {
        const value = inputValue[key];
        const nextSubst = extractRecursive(
          value,
          outputMapping,
          currentSubst
        );
        if (nextSubst === null) {
          observer.complete?.();
          return;
        }
        currentSubst = nextSubst;
      }
      observer.next(currentSubst);
      observer.complete?.();
    })
  );
}
function extractEach(arrayVar, mapping) {
  return (input$) => input$.flatMap(
    (s) => new SimpleObservable10((observer) => {
      const arrayValue = walk(arrayVar, s);
      if (!Array.isArray(arrayValue)) {
        observer.complete?.();
        return;
      }
      for (const element of arrayValue) {
        if (typeof element === "object" && element !== null) {
          let currentSubst = s;
          let allUnified = true;
          for (const [key, outputVar] of Object.entries(mapping)) {
            const value = element[key];
            const unified = unify(outputVar, value, currentSubst);
            if (unified !== null) {
              currentSubst = unified;
            } else {
              allUnified = false;
              break;
            }
          }
          if (allUnified) {
            observer.next(currentSubst);
          }
        }
      }
      observer.complete?.();
    })
  );
}

// src/shared/logger.ts
import util2 from "util";
var DEFAULT_CONFIG = {
  enabled: false,
  allowedIds: /* @__PURE__ */ new Set([
    // "FLUSH_BATCH",
    // "FLUSH_BATCH_COMPLETE",
    // "GOAL_NEXT",
    // "UPSTREAM_GOAL_COMPLETE",
    // "GOAL_COMPLETE",
    // "GOAL_CANCELLED",
    // "FLUSH_BATCH_CANCELLED_AFTER_QUERY",
    // "FLUSH_BATCH_CANCELLED_DURING_ROWS",
    // "FLUSH_BATCH_CANCELLED_DURING_SUBST",
    // "DB_QUERY_BATCH",
    // "CACHE_HIT",
    // "CACHE_MISS",
    // "UNIFY_SUCCESS",
    // "UNIFY_FAILURE",
    // "GOAL_BATCH_KEY_UPDATED",
    // "ABOUT_TO_CALL_CACHE_OR_QUERY",
    // "CACHE_OR_QUERY_START",
    // "COMPATIBLE_GOALS",
    // "ABOUT_TO_PROCESS_GOAL",
    // "GOAL_GROUP_INFO",
    // "DB_ROWS",
    // "DB_NO_ROWS",
    // "FLUSH_BATCH",
    // "COMPATIBLE_MERGE_GOALS",
    // "DB_QUERY_MERGED",
    // "DB_ROWS_MERGED",
    // "ABOUT_TO_CALL_CACHE_OR_QUERY",
    // "USING_GOAL_MERGING",
    // "USING_GOAL_CACHING",
    // "USING_SUBSTITUTION_BATCHING",
    // "CACHE_PERFORMANCE",
    // "BATCH_PERFORMANCE",
    // "CACHE_HIT_IMMEDIATE",
    // "CACHE_MISS_TO_BATCH",
    // "PROCESSING_CACHE_MISSES",
    // "EXECUTING_QUERY_FOR_CACHE_MISSES",
    // "SINGLE_CACHE_MISS_WITH_GOAL_MERGING",
    // "EXECUTING_UNIFIED_QUERY",
    // "DB_QUERY_UNIFIED",
    // "POPULATING_CACHE_FOR_COMPATIBLE_GOALS",
    // "MERGING_COMPATIBLE_GOALS",
    // "COMPATIBLE_GOALS",
    // "CACHED_FOR_OTHER_GOAL",
    // "CROSS_GROUP_CACHE_CHECK",
    // "OUTER_GROUP_CACHE_POPULATION",
    // "GOAL_STARTED",
    // "FOUND_RELATED_GOALS",
    // "MERGE_COMPATIBILITY_CHECK",
    // "CACHE_COMPATIBILITY_CHECK",
    // "SINGLE_QUERY_COLUMN_SELECTION",
    // "MERGED_QUERY_COLUMN_SELECTION",
    // "GOAL_CREATED",
  ]),
  // empty means allow all
  deniedIds: /* @__PURE__ */ new Set([
    // "FACT_ADDED",
    // "UNIFY_FAILED",
    // "THIS_GOAL_ROWS",
    // "ALL_GOAL_ROWS",
    // "COMMON_GOALS",
    // "DB_QUERY",
    // "GOAL_CREATED",
    // "SAW_CACHE",
    // "SHARED_GOALS", // Disabled to reduce noise
    // "DB_QUERY", // Disabled to reduce noise
    // "DB_NO_ROWS",
    // "DB_ROWS",
    // "GOAL_CREATED", // Disabled to reduce noise
    // "MERGEABLE_CHECK", // Disabled to reduce noise
    // "PENDING_QUERIES_DEBUG", // Disabled to reduce noise
    // "MERGE_DEBUG", // Disabled to reduce noise
    // "PENDING_ADD", // Disabled to reduce noise
    // "CACHE_HIT", // Enabled to see cache hits
    // "SHARED_UNIFY", // Enabled to see shared unification
  ])
  // specific ids to deny
};
var Logger = class {
  constructor(config) {
    this.config = config;
  }
  log(id, data) {
    if (!this.config.enabled) return;
    if (this.config.deniedIds.has(id)) return;
    if (this.config.allowedIds.size > 0 && !this.config.allowedIds.has(id))
      return;
    let out;
    if (typeof data === "function") {
      out = data();
    } else {
      out = data;
    }
    if (typeof out === "string") {
      console.log(`[${id}] ${out}`);
    } else {
      console.log(
        `[${id}]`,
        util2.inspect(out, {
          depth: null,
          colors: true
        })
      );
    }
  }
};
var defaultLoggerInstance = null;
function getDefaultLogger() {
  if (!defaultLoggerInstance) {
    defaultLoggerInstance = new Logger(DEFAULT_CONFIG);
  }
  return defaultLoggerInstance;
}

// src/shared/utils.ts
var queryUtils = {
  /**
   * Walk all keys of an object with a substitution and return a new object
   */
  walkAllKeys(obj, subst) {
    const result = {};
    const keys = Object.keys(obj);
    for (const key of keys) {
      result[key] = walk(obj[key], subst);
    }
    return result;
  },
  /**
   * Walk all values in an array with a substitution
   */
  walkAllArray(arr, subst) {
    return arr.map((term) => walk(term, subst));
  },
  /**
   * Check if all query parameters are grounded (no variables)
   */
  allParamsGrounded(params) {
    const values = Object.values(params);
    for (let i = 0; i < values.length; i++) {
      if (isVar(values[i])) return false;
    }
    return true;
  },
  /**
   * Check if all array elements are grounded (no variables)
   */
  allArrayGrounded(arr) {
    for (let i = 0; i < arr.length; i++) {
      if (isVar(arr[i])) return false;
    }
    return true;
  },
  /**
   * Build query parts from parameters and substitution
   */
  buildQueryParts(params, subst) {
    const selectCols = Object.keys(params).sort();
    const walkedQ = {};
    const whereClauses = [];
    for (const col of selectCols) {
      walkedQ[col] = walk(params[col], subst);
      if (!isVar(walkedQ[col])) {
        whereClauses.push({
          column: col,
          value: walkedQ[col]
        });
      }
    }
    return {
      selectCols,
      whereClauses,
      walkedQ
    };
  },
  onlyGrounded(params) {
    return Object.fromEntries(
      Object.entries(params).filter(([key, value]) => !isVar(value))
    );
  },
  onlyVars(params) {
    return Object.fromEntries(
      Object.entries(params).filter(([key, value]) => isVar(value))
    );
  }
};
var unificationUtils = {
  /**
   * Unify all selectCols in a row with walkedQ and subst
   */
  unifyRowWithWalkedQ(selectCols, walkedQ, row, subst) {
    let s2 = subst;
    let needsClone = true;
    for (let i = 0; i < selectCols.length; i++) {
      const col = selectCols[i];
      if (!isVar(walkedQ[col])) {
        if (walkedQ[col] !== row[col]) {
          return null;
        }
      } else {
        if (needsClone) {
          s2 = new Map(subst);
          needsClone = false;
        }
        const unified = unify(walkedQ[col], row[col], s2);
        if (unified) {
          s2 = unified;
        } else {
          return null;
        }
      }
    }
    return s2;
  },
  /**
   * Unify arrays element by element
   */
  unifyArrays(queryArray, factArray, subst) {
    if (queryArray.length !== factArray.length) {
      return null;
    }
    return unify(queryArray, factArray, subst);
  }
};
var patternUtils = {
  /**
   * Check if all select columns are tags (have id property)
   */
  allSelectColsAreTags(cols) {
    const values = Object.values(cols);
    for (let i = 0; i < values.length; i++) {
      if (!values[i].id) return false;
    }
    return true;
  },
  /**
   * Separate query object into select and where columns
   */
  separateQueryColumns(queryObj) {
    const selectCols = {};
    const whereCols = {};
    const entries = Object.entries(queryObj);
    for (let i = 0; i < entries.length; i++) {
      const [key, value] = entries[i];
      if (isVar(value)) {
        selectCols[key] = value;
      } else {
        whereCols[key] = value;
      }
    }
    return {
      selectCols,
      whereCols
    };
  },
  /**
   * Separate array query into select and where terms
   */
  separateArrayQuery(queryArray) {
    const selectTerms = [];
    const whereTerms = [];
    const positions = [];
    for (let i = 0; i < queryArray.length; i++) {
      const term = queryArray[i];
      if (isVar(term)) {
        selectTerms.push(term);
        positions.push(i);
      } else {
        whereTerms.push(term);
      }
    }
    return {
      selectTerms,
      whereTerms,
      positions
    };
  },
  /**
   * Separate symmetric query values into select and where - optimized
   */
  separateSymmetricColumns(queryObj) {
    const selectCols = [];
    const whereCols = [];
    const values = Object.values(queryObj);
    for (let i = 0; i < values.length; i++) {
      const value = values[i];
      if (isVar(value)) {
        selectCols.push(value);
      } else {
        whereCols.push(value);
      }
    }
    return {
      selectCols,
      whereCols
    };
  }
};
var indexUtils = {
  /**
   * Returns the intersection of two sets
   */
  intersect(setA, setB) {
    const result = /* @__PURE__ */ new Set();
    setA.forEach((item) => {
      if (setB.has(item)) {
        result.add(item);
      }
    });
    return result;
  },
  /**
   * Returns true if a value is indexable (string, number, boolean, or null)
   */
  isIndexable(v) {
    return typeof v === "string" || typeof v === "number" || typeof v === "boolean" || v === null;
  },
  /**
   * Create an index for a specific position/key
   */
  createIndex() {
    return /* @__PURE__ */ new Map();
  },
  /**
   * Add a value to an index
   */
  addToIndex(index, key, factIndex) {
    let set = index.get(key);
    if (!set) {
      set = /* @__PURE__ */ new Set();
      index.set(key, set);
    }
    set.add(factIndex);
  }
};
var intersect = indexUtils.intersect;
var isIndexable = indexUtils.isIndexable;

// src/util/procedural-helpers.ts
import { SimpleObservable as SimpleObservable11 } from "@swiftfall/observable";
function aggregateVar(sourceVar, subgoal) {
  return (input$) => new SimpleObservable11((observer) => {
    let active = 0;
    let completed = false;
    const subscription = input$.subscribe({
      next: (s) => {
        active++;
        const results = [];
        let subgoalEmitted = false;
        subgoal(SimpleObservable11.of(s)).subscribe({
          next: (subst) => {
            subgoalEmitted = true;
            results.push(walk(sourceVar, subst));
          },
          error: observer.error,
          complete: () => {
            const s2 = new Map(s);
            s2.set(sourceVar.id, results);
            observer.next(s2);
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
function aggregateVarMulti(groupVars, aggVars, subgoal) {
  return (input$) => new SimpleObservable11((observer) => {
    let active = 0;
    let completed = false;
    const subscription = input$.subscribe({
      next: (s) => {
        active++;
        const groupMap = /* @__PURE__ */ new Map();
        subgoal(SimpleObservable11.of(s)).subscribe({
          next: (subst) => {
            const groupKey = JSON.stringify(
              groupVars.map((v) => walk(v, subst))
            );
            let aggArrays = groupMap.get(groupKey);
            if (!aggArrays) {
              aggArrays = aggVars.map(() => []);
              groupMap.set(groupKey, aggArrays);
            }
            for (let i = 0; i < aggVars.length; i++) {
              const value = walk(aggVars[i], subst);
              aggArrays[i].push(value);
            }
          },
          error: observer.error,
          complete: () => {
            if (groupMap.size === 0) {
              const s2 = new Map(s);
              aggVars.forEach((v, i) => s2.set(v.id, []));
              observer.next(s2);
            } else {
              for (const [groupKey, aggArrays] of groupMap.entries()) {
                const groupValues = JSON.parse(groupKey);
                const s2 = new Map(s);
                groupVars.forEach(
                  (v, index) => s2.set(v.id, groupValues[index])
                );
                aggVars.forEach((v, index) => s2.set(v.id, aggArrays[index]));
                observer.next(s2);
              }
            }
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
export {
  CHECK_LATER,
  GOAL_GROUP_ALL_GOALS,
  GOAL_GROUP_CONJ_GOALS,
  GOAL_GROUP_ID,
  GOAL_GROUP_PATH,
  Logger,
  SUSPENDED_CONSTRAINTS,
  Subquery,
  addSuspendToSubst,
  aggregateRelFactory,
  aggregateVar,
  aggregateVarMulti,
  alldistincto,
  and,
  appendo,
  arrayToLogicList,
  baseUnify,
  branch,
  chainGoals,
  collect_and_process_base,
  collect_distincto,
  collect_streamo,
  collecto,
  conde,
  conj,
  cons,
  count_distincto,
  count_value_streamo,
  count_valueo,
  counto,
  createEnrichedSubst,
  createLogicVarProxy,
  disj,
  dividebyo,
  eitherOr,
  enrichGroupInput,
  eq,
  extendSubst,
  extract,
  extractEach,
  fail,
  failo,
  firsto,
  fresh,
  getDefaultLogger,
  getSuspendsFromSubst,
  groundo,
  groupAggregateRelFactory,
  group_by_collect_distinct_streamo,
  group_by_collect_streamo,
  group_by_collecto,
  group_by_count_streamo,
  group_by_counto,
  group_by_streamo_base,
  gteo,
  gto,
  gv1_not,
  ifte,
  indexUtils,
  intersect,
  isCons,
  isIndexable,
  isLogicList,
  isNil,
  isVar,
  lengtho,
  lift,
  liftGoal,
  logicList,
  logicListToArray,
  lteo,
  lto,
  lvar,
  makeSuspendHandler,
  mapo,
  maxo,
  membero,
  mino,
  minuso,
  multo,
  neqo,
  nextGroupId,
  nil,
  nonGroundo,
  not,
  old_neqo,
  old_not,
  once,
  onceo,
  or,
  patternUtils,
  permuteo,
  pluso,
  project,
  projectJsonata,
  query,
  queryUtils,
  removeFirsto,
  removeSuspendFromSubst,
  resetVarCounter,
  resto,
  run,
  sort_by_streamo,
  substLog,
  succeedo,
  suspendable,
  take_streamo,
  thruCount,
  timeout,
  unificationUtils,
  unify,
  unifyWithConstraints,
  uniqueo,
  wakeUpSuspends,
  walk
};
//# sourceMappingURL=index.js.map