// Relation helpers for MiniKanren-style logic programming

import * as L from "./logic_lib.ts";
import type { Subst, Term, Var } from "./core.ts";
import { isVar, lvar, unify, walk } from "./core.ts";
import { Rel as LogicRel, not as LogicNot } from "./logic_lib.ts";

// --- Goal Metadata and Optimizer ---
export interface GoalMetadata extends Record<string, any> {
  name: string;
  args: any[];
}

export type GoalFn = (s: Subst) => AsyncGenerator<Subst>;
export type Goal = GoalFn & { _metadata?: GoalMetadata };

export function toGoal(fn: GoalFn, metadata?: GoalMetadata): Goal {
  const g = fn as Goal;
  if (metadata) (g as any)._metadata = metadata;
  return g;
}

/**
 * Succeeds if u and v unify.
 */
export function eq(u: Term, v: Term): Goal {
  return toGoal(
    async function* (s: Subst) {
      const s2 = await unify(u, v, s);
      if (s2) yield s2;
    },
    {
      name: "eq",
      args: [u, v],
    },
  );
}

/**
 * Introduces fresh logic variables for a subgoal.
 * Always expects the callback to return a Goal.
 */
export function fresh(f: (...vars: Var[]) => Goal): Goal {
  const n = f.length;
  const goalFunc = async function* (s: Subst) {
    const vars = Array.from({
      length: n 
    }, () => lvar());
    const goal = f(...vars);
    for await (const s1 of goal(s)) yield s1;
  };
  return toGoal(goalFunc, {
    name: "fresh",
    args: [f],
  });
}

/**
 * Logical OR: succeeds if either g1 or g2 succeeds.
 */
export function disj(g1: Goal, g2: Goal): Goal {
  return toGoal(
    async function* (s: Subst) {
      for await (const s2 of g1(s)) yield s2;
      for await (const s2 of g2(s)) yield s2;
    },
    {
      name: "disj",
      args: [g1, g2],
    },
  );
}

/**
 * Logical AND: succeeds if both g1 and g2 succeed in sequence.
 */
export function conj(g1: Goal, g2: Goal): Goal {
  return toGoal(
    async function* (s: Subst) {
      for await (const s1 of g1(s)) {
        for await (const s2 of g2(s1)) yield s2;
      }
    },
    {
      name: "conj",
      args: [g1, g2],
    },
  );
}

/**
 * Logic programming conditional (multi-statement AND per clause).
 * This is an OR of ANDs. Each argument is an array of goals. Those
 * goals are anded together and must all succeed. If the goals in the
 * first argument don't succeed, then the second argument is attempted.
 */
export function conde(...clauses: Goal[][]): Goal {
  const goalFunc = async function* (s: Subst) {
    for (const clause of clauses) {
      const goal = and(...clause);
      for await (const s1 of goal(s)) yield s1;
    }
  };
  return toGoal(goalFunc, {
    name: "conde",
    args: clauses,
  });
}

/**
 * Logical AND for multiple goals.
 */
export const and = (...gs: Goal[]): Goal => {
  return toGoal(
    async function* (s: Subst) {
      // Allow registered hooks to optimize the goals
      // Use original goals if no hooks are registered or all hooks return undefined
      const hookResults = await Promise.all(
        andOptimizerHooks.map(async (hook) => {
          const result = await hook(gs, s);
          return result ? [result] : undefined;
        })
      );

      const optimizedGoals = hookResults
        .filter((res): res is Goal[] => res !== undefined)
        .flat();

      const finalGoals = optimizedGoals.length > 0 ? optimizedGoals : gs;

      // Sequential goal chaining using reduce
      const result: (s: Subst) => AsyncGenerator<Subst> = finalGoals.reduce<
        (s: Subst) => AsyncGenerator<Subst>
          >(
          (acc, goal) => async function* (s: Subst) {
            for await (const s1 of acc(s)) {
              yield* goal(s1);
            }
          },
          async function* (s: Subst) {
            yield s;
          }
          );

      yield* result(s);
    },
    {
      name: "and_default",
      args: gs,
    }
  );
}
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
  return (...args: Term[]) => {
    const goalFunc = async function* (s: Subst) {
      const vals = await Promise.all(args.map((arg) => walk(arg, s)));
      if (pred(...vals)) yield s;
    };
    return toGoal(goalFunc, {
      name: "filterRel",
      args: [pred, ...args],
    });
  };
}

export const gtc = filterRel((x, gt) => x > gt);

/**
 * Create a relation from a function mapping input terms to an output term.
 */
export const mapRel = <F extends (...args: any) => any>(fn: F) => {
  return (...args: Parameters<TermedArgs<F>>) => {
    const goalFunc = async function* (s: Subst) {
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
    return toGoal(goalFunc, {
      name: "mapRel",
      args: [fn, ...args],
    });
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
  const goalFunc = async function* (s: Subst) {
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
  return toGoal(goalFunc, {
    name: "mapInlineLazy",
    args: [fn, ...args],
  });
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
  return (...args) => {
    const res = fn(...args);
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
  const goalFunc = async function* (s: Subst) {
    for await (const _ of goal(s)) return;
    yield s;
  };
  return toGoal(goalFunc, {
    name: "not",
    args: [goal],
  });
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
  const goalFunc = async function* (s: Subst) {
    let found = false;
    for await (const s1 of g1(s)) {
      found = true;
      yield s1;
    }
    if (!found) {
      for await (const s2 of g2(s)) yield s2;
    }
  };
  return toGoal(goalFunc, {
    name: "ifte",
    args: [g1, g2],
  });
}
export const eitherOr = ifte;
export const neq_C = LogicRel((x: Term, y: Term) => LogicNot(eq(x, y)));

export const distincto_G = LogicRel((t: Term, g: Goal) => {
  // Track seen values for t in this execution
  const goalFunc = async function* (s: Subst) {
    const seen = new Set();
    for await (const s2 of g(s)) {
      const w_t = await walk(t, s2);
      if (isVar(w_t)) {
        yield s2;
        continue;
      }
      const key = JSON.stringify(w_t);
      if (seen.has(key)) continue;
      seen.add(key);
      yield s2;
    }
  };
  return toGoal(goalFunc, {
    name: "distincto_G",
    args: [t, g],
  });
});

// --- Optimizer Hook System ---
const andOptimizerHooks: ((goals: Goal[], s: Subst) => Promise<Goal | undefined>)[] = [];
export async function registerAndOptimizerHook(hook: (goals: Goal[], s: Subst) => Promise<Goal | undefined>) {
  andOptimizerHooks.push(hook);
}


