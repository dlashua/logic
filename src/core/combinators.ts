import {
  Goal,
  Subst,
  Term,
  Var,
  LiftedArgs
} from "./types.ts";
import { unify, lvar, walk, isVar } from "./kernel.ts";

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

/**
 * Lifts a pure JavaScript function into a Goal function.
 * The lifted function takes the same parameters as the original function,
 * but each parameter is wrapped in Term<>. It also adds a final 'out' parameter
 * that represents the result of the function call, also wrapped in Term<>.
 */
export function lift<T extends (...args: any) => any>(fn: T): LiftedArgs<T> {
  return ((...args: any[]) => {
    // Extract the 'out' parameter (last argument)
    const out = args[args.length - 1];
    const inputArgs = args.slice(0, -1);
    
    return async function* liftedGoal(s: Subst) {
      try {
        // Walk all input arguments to resolve any variables
        const resolvedArgs = await Promise.all(
          inputArgs.map(arg => walk(arg, s))
        );
        
        // Check if all arguments are ground (no variables)
        const hasVariables = resolvedArgs.some(arg => isVar(arg));
        
        if (!hasVariables) {
          // All arguments are ground, we can call the function
          const result = fn(...resolvedArgs);
          
          // Unify the result with the output parameter
          const unified = await unify(out, result, s);
          if (unified !== null) {
            yield unified;
          }
        }
        // If there are variables in the input, the goal fails
        // (we can't reverse-engineer the function)
      } catch (error) {
        // If the function throws, the goal fails silently
      }
    };
  }) as LiftedArgs<T>;
}

/**
 * Soft-cut if-then-else combinator.
 * ifte(ifGoal, thenGoal, elseGoal) succeeds with thenGoal if ifGoal succeeds,
 * otherwise succeeds with elseGoal.
 */
export function ifte(ifGoal: Goal, thenGoal: Goal, elseGoal: Goal): Goal {
  return async function* ifteGoal(s: Subst) {
    let succeeded = false;
    const results: Subst[] = [];
    for await (const s1 of ifGoal(s)) {
      succeeded = true;
      results.push(s1);
    }
    if (succeeded) {
      for (const s1 of results) {
        yield* thenGoal(s1);
      }
    } else {
      yield* elseGoal(s);
    }
  };
}
