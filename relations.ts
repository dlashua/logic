// Relation helpers for MiniKanren-style logic programming

import * as L from "./logic_lib.ts";
import type { Subst, Term, Var } from "./core.ts";
import { isVar, lvar, unify, walk } from "./core.ts"

/**
 * A logic goal: a function from a substitution to an async generator of substitutions.
 */
export type Goal = (s: Subst) => AsyncGenerator<Subst>;

/**
 * Succeeds if u and v unify.
 */
export function eq(u: Term, v: Term): Goal {
  return async function* (s: Subst) {
    const s2 = await unify(u, v, s);
    if (s2) yield s2;
  };
}

/**
 * Introduces fresh logic variables for a subgoal.
 * Always expects the callback to return a Goal.
 */
export function fresh(f: (...vars: Var[]) => Goal): Goal {
  const n = f.length;
  return async function* (s: Subst) {
    const vars = Array.from({
      length: n 
    }, () => lvar());
    const goal = f(...vars);
    for await (const s1 of goal(s)) yield s1;
  };
}

/**
 * Logical OR: succeeds if either g1 or g2 succeeds.
 */
export function disj(g1: Goal, g2: Goal): Goal {
  return async function* (s: Subst) {
    for await (const s2 of g1(s)) yield s2;
    for await (const s2 of g2(s)) yield s2;
  };
}

/**
 * Logical AND: succeeds if both g1 and g2 succeed in sequence.
 */
export function conj(g1: Goal, g2: Goal): Goal {
  return async function* (s: Subst) {
    for await (const s1 of g1(s)) {
      for await (const s2 of g2(s1)) yield s2;
    }
  };
}

/**
 * Logic programming conditional (multi-statement AND per clause).
 * This is an OR of ANDs. Each argument is an array of goals. Those
 * goals are anded together and must all succeed. If the goals in the
 * first argument don't succeed, then the second argument is attempted.
 */
export function conde(...clauses: Goal[][]): Goal {
  return async function* (s: Subst) {
    for (const clause of clauses) {
      const goal = and(...clause);
      for await (const s1 of goal(s)) yield s1;
    }
  };
}

/**
 * Logical AND for multiple goals.
 */
export const and = (...goals: Goal[]) => {
  if (goals.length === 0) throw new Error("and requires at least one goal");
  return goals.reduce((a, b) => conj(a, b));
};
export const all = and;

/**
 * Logical OR for multiple goals.
 */
export const or = (...goals: Goal[]) => {
  if (goals.length === 0) throw new Error("or requires at least one goal");
  return goals.reduce((a, b) => disj(a, b));
};

// --- Relation Constructors ---

/**
 * Create a relation from a predicate that takes any number of arguments.
 */
export function filterRel(
  pred: (...args: any[]) => boolean,
): (...args: Term[]) => Goal {
  return (...args: Term[]) =>
    async function* (s: Subst) {
      const vals = await Promise.all(args.map((arg) => walk(arg, s)));
      if (pred(...vals)) yield s;
    };
}

export const gtc = filterRel((x, gt) => x > gt);

/**
 * Create a relation from a function mapping input terms to an output term.
 */
export const mapRel = <F extends (...args: any) => any>(fn: F) => {
  return (...args: Parameters<TermedArgs<F>>) =>
    async function* (s: Subst) {
      const vals = await Promise.all(
        args.map(async (arg) => await walk(arg, s)),
      );
      const inVals = vals.slice(0, -1);
      const outVal = vals[vals.length - 1];
      if (inVals.every((v) => typeof v !== "undefined" && !isVar(v))) {
        const result = fn(...(inVals as Parameters<F>));
        const s2 = await unify(outVal, result, s);
        if (s2) yield s2;
      }
    };
};

export const mapInline = <F extends (...args: any) => any>(
  fn: F,
  ...args: Parameters<TermedArgs<F>>
) => {
  const mr = mapRel(fn);
  return mr(...args);
};

/**
 * mapInlineLazy: Like mapInline, but stores the mapping as a thunk for lazy evaluation.
 */
export const mapInlineLazy = <F extends (...args: any) => any>(
  fn: F,
  ...args: Parameters<TermedArgs<F>>
) => {
  return async function* (s: Subst) {
    const inArgs = args.slice(0, -1);
    const outVar = args[args.length - 1];
    if (isVar(outVar)) {
      s.set(outVar.id, async () =>
        fn(...(await Promise.all(inArgs.map((arg) => walk(arg, s))))),
      );
      yield s;
    } else {
      const result = fn(
        ...(await Promise.all(inArgs.map((arg) => walk(arg, s)))),
      );
      const s2 = await unify(outVar, result, s);
      if (s2) yield s2;
    }
  };
};

/**
 * Type helper for mapping function signatures to relation signatures.
 *
 * When you create a new relation using mapRel, mapInline, or mapInlineLazy, this type ensures:
 *   - All input arguments are wrapped as Term<T> (logic variables or values)
 *   - The last argument is the output term (Term<R>)
 *   - The resulting function returns a Goal
 *
 * Example:
 *   // A function (x: number, y: number) => number
 *   const plus = (x: number, y: number) => x + y;
 *   // Create a relation: pluso(x, y, z)
 *   const pluso: (...args: TermedArgs<typeof plus>) => Goal = mapRel(plus);
 *   // pluso(x, y, z) is now type-safe: (Term<number>, Term<number>, Term<number>) => Goal
 *
 * This allows users to define new relations with correct and accurate types, improving safety and developer experience.
 */
export type TermedArgs<T extends (...args: any) => any> = T extends (
  ...args: infer A
) => infer R
  ? (...args: [...{ [I in keyof A]: Term<A[I]> | A[I] }, out: Term<R>]) => Goal
  : never;

/**
 * Type helper for defining a relation with argument inference.
 */
export function Rel<F extends (...args: any) => any>(
  fn: F,
): (...args: Parameters<F>) => Goal {
  return fn;
}

export function ___Rel<F extends (...args: any) => any>(
  fn: F,
): (...args: Parameters<F>) => Goal {
  const fnName = fn.name;
  return (...args) => {
    const start = Date.now();
    const res = fn(...args);
    console.log("REL", fnName, Date.now() - start);
    return res;
  };
}

// --- List relations moved to relations-list.ts ---
// export function membero ...
// export function firsto ...
// export function resto ...
// export function appendo ...
// export function permuteo ...
// export function mapo ...
// export function removeFirsto ...

/**
 * not(goal): Succeeds if the given goal fails (negation as failure).
 */
export function not(goal: Goal): Goal {
  return async function* (s: Subst) {
    for await (const _ of goal(s)) return;
    yield s;
  };
}

/**
 * pluso(x, y, z): x + y = z
 */
export const pluso = mapRel((x: number, y: number) => x + y);
/**
 * subo(x, y, z): x - y = z
 */
export const subo = mapRel((x: number, y: number) => x - y);
/**
 * multo(x, y, z): x * y = z
 */
export const multo = mapRel((x: number, y: number) => x * y);
/**
 * divo(x, y, z): x / y = z (integer division)
 */
export const divo = mapRel((x: number, y: number) => Math.floor(x / y));

/**
 * ifte: logic programming if-then-else (soft cut).
 * Tries g1; if it succeeds, yields only those results and does NOT try g2.
 * If g1 fails, tries g2.
 * Alias: eitherOr
 */
export function ifte(g1: Goal, g2: Goal): Goal {
  return async function* (s: Subst) {
    let found = false;
    for await (const s1 of g1(s)) {
      found = true;
      yield s1;
    }
    if (!found) {
      for await (const s2 of g2(s)) yield s2;
    }
  };
}
export const eitherOr = ifte;
export const neq_C = L.Rel((x: Term, y: Term) => L.not(L.eq(x, y)));

export const distincto_G = L.Rel((t: Term, g: Goal) => {
  // Track seen values for t in this execution
  return async function* (s: Subst) {
    const seen = new Set();
    for await (const s2 of g(s)) {
      const w_t = await L.walk(t, s2);
      if (L.isVar(w_t)) {
        yield s2;
        continue;
      }
      const key = JSON.stringify(w_t);
      if (seen.has(key)) continue;
      seen.add(key);
      yield s2;
    }
  };
});
