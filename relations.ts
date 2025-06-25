// Relation helpers for MiniKanren-style logic programming

import type { Subst, Term, Var } from "./core.ts";
import { isVar, lvar, unify, walk } from "./core.ts"

/**
 * A logic goal: a function from a substitution to an async generator of substitutions.
 */
export type Goal = (s: Subst) => AsyncGenerator<Subst>;

/**
 * Succeeds if u and v unify.
 */
export function eq(u: Term, v: Term) {
  const goal = async function* eq (s: Subst) {
    const s2 = await unify(u, v, s);
    if (s2) yield s2;
  };
  return maybeProfile(goal);
}

/**
 * Introduces fresh logic variables for a subgoal.
 * Always expects the callback to return a Goal.
 */
export function fresh(f: (...vars: Var[]) => Goal) {
  const n = f.length;
  const goal = async function* fresh (s: Subst) {
    const vars = Array.from({
      length: n 
    }, () => lvar());
    const subgoal = f(...vars);
    for await (const s1 of subgoal(s)) yield s1;
  };
  return maybeProfile(goal);
}

/**
 * Logical OR: succeeds if either g1 or g2 succeeds.
 */
export function disj(g1: Goal, g2: Goal) {
  const goal = async function* disj (s: Subst) {
    for await (const s2 of g1(s)) yield s2;
    for await (const s2 of g2(s)) yield s2;
  };
  return maybeProfile(goal);
}

/**
 * Logical AND: succeeds if both g1 and g2 succeed in sequence.
 */
export function conj(g1: Goal, g2: Goal) {
  const goal = async function* conj (s: Subst) {
    for await (const s1 of g1(s)) {
      for await (const s2 of g2(s1)) yield s2;
    }
  };
  return maybeProfile(goal);
}

/**
 * Logic programming conditional (multi-statement AND per clause).
 * This is an OR of ANDs. Each argument is an array of goals. Those
 * goals are anded together and must all succeed. If the goals in the
 * first argument don't succeed, then the second argument is attempted.
 */
export function conde(...clauses: Goal[][]) {
  const goal = async function* conde (s: Subst) {
    for (const clause of clauses) {
      const subgoal = and(...clause);
      for await (const s1 of subgoal(s)) yield s1;
    }
  };
  return maybeProfile(goal);
}

/**
 * Logical AND for multiple goals.
 */
export const and = (...goals: Goal[]) => {
  if (goals.length === 0) throw new Error("and requires at least one goal");
  return maybeProfile(goals.reduce((a, b) => conj(a, b)));
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
  return (...args: Term[]) => maybeProfile(async function* filterRel (s: Subst) {
    const vals = await Promise.all(args.map((arg) => walk(arg, s)));
    if (pred(...vals)) yield s;
  });
}

export const gtc = filterRel((x, gt) => x > gt);

/**
 * Create a relation from a function mapping input terms to an output term.
 */
export const mapRel = <F extends (...args: any) => any>(fn: F) => {
  return (...args: Parameters<TermedArgs<F>>) => maybeProfile(async function* mapRel (s: Subst) {
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
  });
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
  return maybeProfile(async function* mapInlineLazy (s: Subst) {
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
): (...args: Parameters<F>) => ProfilableGoal {
  return (...args: Parameters<F>) => {
    const goal = fn(...args);
    // Always set a custom property for the logical name
    if (typeof goal === "function" && fn.name) {
      (goal as any).__logicName = fn.name;
    }
    return maybeProfile(goal);
  };
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
  const g = async function* not (s: Subst) {
    for await (const _ of goal(s)) return;
    yield s;
  };
  return maybeProfile(g);
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
  return async function* ifte (s: Subst) {
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
export const neq_C = Rel((x: Term, y: Term) => maybeProfile(not(eq(x, y))));
export const distincto_G = Rel((t: Term, g: Goal) => maybeProfile(async function* distincto_G (s: Subst) {
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
}));

// --- Universal Profiling Support ---
export let LOGIC_PROFILING_ENABLED = false;
// Use file:line as the unique key for profiling
export const logicProfileData = new Map<string, { count: number; totalTime: number; goal: Goal }>();
const relationDisplayNames = new WeakMap<Goal, string>();
const relationIdCounter = 1;
function getRelationDisplayName(goal: Goal): string {
  return (goal as any).__logicName ?? goal.name ?? "anonymous";
}
export function enableLogicProfiling() { LOGIC_PROFILING_ENABLED = true; }
export function disableLogicProfiling() { LOGIC_PROFILING_ENABLED = false; logicProfileData.clear(); }
export function printLogicProfileRecap() {
  // Gather all entries
  const entries = Array.from(logicProfileData.entries()).map(([fileLine, data]) => ({
    fileLine,
    name: getRelationDisplayName(data.goal),
    count: data.count,
    totalTime: data.totalTime,
  }));
  // Top 10 by count
  const topByCount = entries.slice().sort((a, b) => b.count - a.count).slice(0, 30);
  // Top 10 by time
  // const topByTime = entries.slice().sort((a, b) => b.totalTime - a.totalTime).slice(0, 10);
  console.log("Profiling Recap: Top 30 by Count");
  for (const e of topByCount) {
    console.log(`Relation: ${e.name}, Count: ${e.count}, Total Time: ${e.totalTime}ms, Source: ${e.fileLine}`);
  }
  // console.log("\nProfiling Recap: Top 10 by Total Time");
  // for (const e of topByTime) {
  //   console.log(`Relation: ${e.name}, Count: ${e.count}, Total Time: ${e.totalTime}ms, Source: ${e.fileLine}`);
  // }
}

export type ProfiledGoal = Goal & { __isProfiled: true, __isProfilable: true };

function wrapGoalForProfiling(goal: Goal): ProfiledGoal {
  // Do NOT skip if already profiled! We want to profile every layer.
  // Capture stack trace at creation time
  const err = new Error();
  let fileLine = "unknown";
  if (err.stack) {
    const stackLines = err.stack.split("\n");
    const userFrame = stackLines.find(l => l.includes(".ts") && !l.includes("relations.ts"));
    if (userFrame) {
      const re = /\((.*):(\d+):(\d+)\)/;
      const match = re.exec(userFrame);
      if (match) {
        fileLine = `${match[1]}:${match[2]}`;
      }
    }
  }
  (goal as any).__fileLine = fileLine;
  const wrapped: Goal = (s: Subst) => {
    const start = Date.now();
    const gen = goal(s);
    let finished = false;
    async function* profiledGen() {
      try {
        for await (const v of gen) {
          yield v;
        }
        finished = true;
      } finally {
        if (finished) {
          const key = fileLine;
          const entry = logicProfileData.get(key) ?? {
            count: 0,
            totalTime: 0,
            goal 
          };
          logicProfileData.set(key, {
            count: entry.count + 1,
            totalTime: entry.totalTime + (Date.now() - start),
            goal,
          });
        }
      }
    }
    return profiledGen();
  };
  // Set the custom logic name property
  (wrapped as any).__logicName = (goal as any).__logicName ?? goal.name;
  (wrapped as any).__isProfiled = true;
  (wrapped as any).__isProfilable = true;
  return wrapped as ProfiledGoal;
}

export type ProfilableGoal = Goal & { __isProfilable: true };
export function maybeProfile(goal: Goal | ProfilableGoal): ProfilableGoal {
  if (LOGIC_PROFILING_ENABLED) return wrapGoalForProfiling(goal);
  (goal as ProfilableGoal).__isProfilable = true;
  return goal as ProfilableGoal;
}
