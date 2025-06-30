import { Goal, Subst, Term } from "../core/types.ts";
import { walk } from "../core/kernel.ts";

/**
 * A goal that succeeds if the numeric value in the first term is greater than
 * the numeric value in the second term.
 */
export function gto(x: Term, y: Term): Goal {
  return async function* gtoGoal(s: Subst) {
    const xWalked = await walk(x, s);
    const yWalked = await walk(y, s);
    
    // Both must be grounded to numeric values
    if (typeof xWalked === 'number' && typeof yWalked === 'number') {
      if (xWalked > yWalked) {
        yield s;
      }
    }
    // If either is ungrounded, this constraint cannot be satisfied
  };
}
