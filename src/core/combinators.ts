import { Goal, Subst, Term, Var } from "./types.ts";
import { unify, lvar } from "./kernel.ts";

/**
 * A goal that succeeds if two terms can be unified.
 */
export function eq(u: Term, v: Term): Goal {
  return async function* eqGoal(s: Subst | null) {
    const s_unified = await unify(u, v, s);
    if (s_unified !== null) {
      yield s_unified;
    }
  };
}

/**
 * Introduces new (fresh) logic variables into a sub-goal.
 */
export function fresh(f: (...vars: Var[]) => Goal): Goal {
  return async function* freshGoal(s: Subst) {
    const freshVars = Array.from({
      length: f.length 
    }, () => lvar());
    const subGoal = f(...freshVars);
    yield* subGoal(s);
  };
}

/**
 * Logical disjunction (OR).
 */
export function disj(g1: Goal, g2: Goal): Goal {
  return async function* disjGoal(s: Subst) {
    yield* g1(s);
    yield* g2(s);
  };
}

/**
 * Logical conjunction (AND).
 */
export function conj(g1: Goal, g2: Goal): Goal {
  return async function* conjGoal(s: Subst) {
    for await (const s1 of g1(s)) {
      yield* g2(s1);
    }
  };
}

/**
 * Helper for combining multiple goals with logical AND.
 */
export const and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) return (s) => (async function*() { yield s; })();
  return goals.reduce(conj);
};

/**
 * Helper for combining multiple goals with logical OR.
 */
export const or = (...goals: Goal[]): Goal => {
  if (goals.length === 0) return (s) => (async function*() { /* pass */ })();
  return goals.reduce(disj);
};

/**
 * Multi-clause disjunction (OR).
 */
export function conde(...clauses: Goal[][]): Goal {
  const clauseGoals = clauses.map(clause => and(...clause));
  return or(...clauseGoals);
}
