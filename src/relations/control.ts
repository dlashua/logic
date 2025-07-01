import { Goal, Subst, Term } from "../core/types.ts";
import { walk , isVar } from "../core/kernel.ts";
import { eq } from "../core/combinators.ts";



export const uniqueo = (t: Term, g: Goal) => 
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
  };

export function not(goal: Goal): Goal {
  const g = async function* not(s: Subst) {
    let found = false;
    for await (const subst of goal(s)) {
      // Check if this result only added bindings that were already in the original substitution
      // If it added new variable bindings, we don't consider this a "safe" success
      let addedNewBindings = false;
      for (const [key, value] of subst) {
        if (!s.has(key)) {
          addedNewBindings = true;
          break;
        }
      }
      
      // If the goal succeeded without adding new bindings, it's a genuine success
      if (!addedNewBindings) {
        found = true;
        break;
      }
    }
    if (!found) yield s;
  };
  return g;
}

export const neqo = (x: Term, y: Term) => not(eq(x, y));
