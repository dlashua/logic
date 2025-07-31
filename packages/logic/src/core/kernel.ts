import { SimpleObservable } from "observable";
import { SUSPENDED_CONSTRAINTS, wakeUpSuspends } from "./subst-suspends.js";
import type {
  ConsNode,
  Goal,
  LogicList,
  NilNode,
  Subst,
  Term,
  Var,
} from "./types.js";

// Well-known symbols for SQL query coordination
export const GOAL_GROUP_ID = Symbol("goal-group-id");
export const GOAL_GROUP_PATH = Symbol("goal-group-path");
export const GOAL_GROUP_CONJ_GOALS = Symbol("goal-group-conj-goals"); // Goals in immediate group
export const GOAL_GROUP_ALL_GOALS = Symbol("goal-group-all-goals"); // Goals across all related groups

let varCounter = 0;
let groupCounter = 0;
export function nextGroupId() {
  return groupCounter++;
}

/**
 * Creates a new, unique logic variable.
 * @param name An optional prefix for debugging.
 */
export function lvar(name = ""): Var {
  return {
    tag: "var",
    id: `${name}_${varCounter++}`,
  };
}

/**
 * Resets the global variable counter for deterministic tests.
 */
export function resetVarCounter(): void {
  varCounter = 0;
}

/**
 * Recursively finds the ultimate binding of a term in a given substitution.
 * Optimized to use iteration for variable chains and avoid deep recursion.
 * @param u The term to resolve.
 * @param s The substitution map.
 */
export function walk(u: Term, s: Subst): Term {
  let current = u;

  if (
    !isVar(current) &&
    !isCons(current) &&
    !Array.isArray(current) &&
    typeof current !== "object"
  ) {
    return current; // Fast exit for primitives
  }

  // Fast path for variable chains - use iteration instead of recursion
  while (isVar(current) && s.has(current.id)) {
    current = s.get(current.id);
  }

  // If we ended up with a non-variable, check if it needs structural walking
  if (isCons(current)) {
    // Walk both parts of the cons cell
    return cons(walk(current.head, s), walk(current.tail, s));
  }

  if (Array.isArray(current)) {
    return current.map((x) => walk(x, s));
  }

  if (
    current &&
    typeof current === "object" &&
    !isVar(current) &&
    !isLogicList(current)
  ) {
    const out: Record<string, Term> = {};
    for (const k in current) {
      if (Object.hasOwn(current, k)) {
        out[k] = walk((current as any)[k], s);
      }
    }
    return out;
  }

  return current;
}

/**
 * Extends a substitution by binding a variable to a value, with an occurs check.
 */
export function extendSubst(v: Var, val: Term, s: Subst): Subst | null {
  if (occursCheck(v, val, s)) {
    return null; // Occurs check failed
  }
  const s2 = new Map(s);
  s2.set(v.id, val);
  return s2;
}

/**
 * Checks if a variable `v` occurs within a term `x` to prevent infinite loops.
 */
function occursCheck(v: Var, x: Term, s: Subst): boolean {
  const resolvedX = walk(x, s);
  if (isVar(resolvedX)) {
    return v.id === resolvedX.id;
  }
  if (isCons(resolvedX)) {
    return (
      occursCheck(v, resolvedX.head, s) || occursCheck(v, resolvedX.tail, s)
    );
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

/**
 * The core unification algorithm. It attempts to make two terms structurally equivalent.
 * Optimized with fast paths for common cases.
 */
export function baseUnify(u: Term, v: Term, s: Subst | null): Subst | null {
  if (s === null) {
    return null;
  }

  // Fast path: if both terms are identical primitives, no walking needed
  if (u === v) {
    return s;
  }

  const uWalked = walk(u, s);
  const vWalked = walk(v, s);

  // Fast path: after walking, if they're still identical, succeed
  if (uWalked === vWalked) {
    return s;
  }

  if (isVar(uWalked)) return extendSubst(uWalked, vWalked, s);
  if (isVar(vWalked)) return extendSubst(vWalked, uWalked, s);

  // Fast paths for primitive types
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

  if (
    Array.isArray(uWalked) &&
    Array.isArray(vWalked) &&
    uWalked.length === vWalked.length
  ) {
    let currentSubst: Subst | null = s;
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

/**
 * Constraint-aware unify that wakes up suspended constraints when variables are bound
 */

export function unifyWithConstraints(
  u: Term,
  v: Term,
  s: Subst | null,
): Subst | null {
  const result = baseUnify(u, v, s);

  if (result !== null && s !== null) {
    // Fast path: if no constraints exist, skip constraint logic
    if (!result.has(SUSPENDED_CONSTRAINTS)) {
      return result;
    }

    // Check what new variables were bound
    const newlyBoundVars: string[] = [];
    for (const [key] of result) {
      if (!s.has(key) && typeof key === "string") {
        newlyBoundVars.push(key);
      }
    }

    // Wake up constraints for newly bound variables (if any)
    if (newlyBoundVars.length > 0) {
      return wakeUpSuspends(result, newlyBoundVars);
    }
  }

  return result;
}

export const unify = unifyWithConstraints;
// export const unify = baseUnify;

/**
 * Type guard to check if a term is a logic variable.
 */
export function isVar(x: Term): x is Var {
  return typeof x === "object" && x !== null && (x as Var).tag === "var";
}

/**
 * The canonical `nil` value, representing an empty logic list.
 */
export const nil: NilNode = { tag: "nil" };

/**
 * Creates a `cons` cell (a node in a logic list).
 */
export function cons(head: Term, tail: Term): ConsNode {
  return {
    tag: "cons",
    head,
    tail,
  };
}

/**
 * Converts a JavaScript array into a logic list.
 */
export function arrayToLogicList(arr: Term[]): LogicList {
  return arr.reduceRight<LogicList>((tail, head) => cons(head, tail), nil);
}

/**
 * A convenience function to create a logic list from arguments.
 */
export function logicList<T = unknown>(...items: T[]): LogicList {
  return arrayToLogicList(items);
}

/**
 * Type guard to check if a term is a `cons` cell.
 */
export function isCons(x: Term): x is ConsNode {
  return typeof x === "object" && x !== null && (x as ConsNode).tag === "cons";
}

/**
 * Type guard to check if a term is `nil`.
 */
export function isNil(x: Term): x is NilNode {
  return typeof x === "object" && x !== null && (x as NilNode).tag === "nil";
}

/**
 * Type guard to check if a term is a logic list.
 */
export function isLogicList(x: Term): x is LogicList {
  return isCons(x) || isNil(x);
}

/**
 * Converts a logic list to a JavaScript array.
 */
export function logicListToArray(list: Term): Term[] {
  const out = [];
  let cur = list;
  while (
    cur &&
    typeof cur === "object" &&
    "tag" in cur &&
    (cur as any).tag === "cons"
  ) {
    out.push((cur as any).head);
    cur = (cur as any).tail;
  }
  return out;
}

// Helper: Convert a single-substitution goal to the new protocol
export function liftGoal(
  singleGoal: (s: Subst) => SimpleObservable<Subst>,
): Goal {
  const groupType = singleGoal.name || "liftGoal";
  return enrichGroupInput(
    groupType,
    [],
    [],
    (input$) =>
      new SimpleObservable((observer) => {
        const subs = input$.subscribe({
          next: (s) => {
            const out$ = singleGoal(s);
            out$.subscribe({
              next: (s2) => observer.next(s2),
              error: (e) => observer.error?.(e),
              complete: () => {
                /* pass */
              }, // wait for all
            });
          },
          error: (e) => observer.error?.(e),
          complete: () => observer.complete?.(),
        });
        return () => subs.unsubscribe?.();
      }),
  );
}

// Helper: Chain goals (like flatMap for observables)
export function chainGoals(
  goals: Goal[],
  initial$: SimpleObservable<Subst>,
): SimpleObservable<Subst> {
  return goals.reduce((input$, goal) => goal(input$), initial$);
}

/**
 * Creates an enriched substitution with group metadata
 */
export function createEnrichedSubst(
  s: Subst,
  type: string,
  conjGoals: Goal[],
  disjGoals: Goal[],
  branch?: number,
): Subst {
  const groupId = nextGroupId();
  const parentPath = (s.get(GOAL_GROUP_PATH) as any[]) || [];
  const newPath = [
    ...parentPath,
    {
      type: Symbol(type),
      id: groupId,
      ...(branch !== undefined ? { branch } : {}),
    },
  ];
  const parentOuterGoals = (s.get(GOAL_GROUP_ALL_GOALS) as Goal[]) || [];
  // Recursively collect all innerGoals from the combined goals array
  function collectAllInnerConjGoals(goals: Goal[]): Goal[] {
    return goals.flatMap((goal) => {
      const innerGoals = (goal as any)?.conjGoals ?? ([] as Goal[]);
      if (innerGoals && innerGoals.length > 0) {
        return [...collectAllInnerConjGoals(innerGoals)];
      } else {
        return [goal];
      }
    });
  }
  function collectAllInnerGoals(goals: Goal[]): Goal[] {
    return goals.flatMap((goal) => {
      const innerConjGoals = (goal as any)?.conjGoals ?? ([] as Goal[]);
      const innerDisjGoals = (goal as any)?.disjGoals ?? ([] as Goal[]);
      const innerGoals = [...new Set([...innerConjGoals, ...innerDisjGoals])];
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
    ...new Set([...conjGoals, ...conjInnerConj]),
  ]);
  newSubst.set(GOAL_GROUP_ALL_GOALS, [
    ...new Set([
      ...parentOuterGoals,
      ...conjGoals,
      ...disjGoals,
      ...disjInnerAll,
    ]),
  ]);
  return newSubst;
}

/**
 * Unified helper for enriching input with group metadata
 */
export function enrichGroupInput(
  type: string,
  conjGoals: Goal[],
  disjGoals: Goal[],
  fn: (enrichedInput$: SimpleObservable<Subst>) => SimpleObservable<Subst>,
) {
  function newInput$(input$: SimpleObservable<Subst>) {
    const enrichedInput$ = input$.map((s) =>
      createEnrichedSubst(s, type, conjGoals, disjGoals),
    );
    return fn(enrichedInput$);
  }
  (newInput$ as any).conjGoals = conjGoals;
  (newInput$ as any).disjGoals = disjGoals;
  Object.defineProperty(newInput$, "name", { value: type });
  return newInput$;
}
