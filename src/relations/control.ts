import { Goal, Subst, Term } from "../core/types.ts";
import { walk , isVar } from "../core/kernel.ts";
import { eq } from "../core/combinators.ts";

export function lift<F extends (...args: any) => any>(
  fn: F,
): (...args: Parameters<F>) => Goal {
  return (...args: Parameters<F>) => {
    const goal = async function* liftGoal(s: Subst) {
      // Walk all arguments with the current substitution
      const walkedArgs = await Promise.all(args.map(arg => walk(arg, s)));
      // Call the underlying relation function with grounded arguments
      const subgoal = fn(...walkedArgs);
      for await (const s1 of subgoal(s)) {
        yield s1;
      }
    };
    // Always set a custom property for the logical name
    if (typeof goal === "function" && fn.name) {
      (goal as any).__logicName = fn.name;
    }
    return goal;
  };
}

export const uniqueo = lift((t: Term, g: Goal) => 
  async function* uniqueo(s: Subst) {
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
  }
);

export function not(goal: Goal): Goal {
  const g = async function* not(s: Subst) {
    let found = false;
    for await (const _subst of goal(s)) {
      found = true;
      break;
    }
    if (!found) yield s;
  };
  return g;
}

export const neqo = lift((x: Term, y: Term) => not(eq(x, y)));
