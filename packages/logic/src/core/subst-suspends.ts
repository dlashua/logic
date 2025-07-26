import { isVar, walk } from "./kernel.js";
import { CHECK_LATER } from "./suspend-helper.js";
import type { Subst } from "./types.js";

export const SUSPENDED_CONSTRAINTS = Symbol("suspended-constraints");

export interface SuspendedConstraint {
  id: string;
  resumeFn: (subst: Subst) => Subst | null | typeof CHECK_LATER;
  watchedVars: string[];
}

let constraintCounter = 0;
const MAX_COUNTER = 1000000; // Reset after 1M constraints to prevent overflow

export function addSuspendToSubst(
  subst: Subst,
  resumeFn: (subst: Subst) => Subst | null | typeof CHECK_LATER,
  watchedVars: string[],
): Subst {
  const suspends =
    (subst.get(SUSPENDED_CONSTRAINTS) as SuspendedConstraint[]) || [];

  // **Issue #3: Automatic pruning** - Check if constraint is already irrelevant
  const stillRelevantVars = watchedVars.filter((varId) => {
    const value = walk({ tag: "var", id: varId }, subst);
    return isVar(value); // Only keep if variable is still unbound
  });

  // If no variables left to watch, don't add the constraint
  if (stillRelevantVars.length === 0) {
    return subst;
  }

  // Reset counter periodically to prevent unbounded growth
  if (constraintCounter >= MAX_COUNTER) {
    constraintCounter = 0;
  }

  const newSuspend: SuspendedConstraint = {
    id: `constraint_${constraintCounter++}`,
    resumeFn,
    // watchedVars: stillRelevantVars // Use pruned list
    watchedVars,
  };

  const newSubst = new Map(subst);
  newSubst.set(SUSPENDED_CONSTRAINTS, [...suspends, newSuspend]);
  return newSubst;
}

export function getSuspendsFromSubst(subst: Subst): SuspendedConstraint[] {
  return (subst.get(SUSPENDED_CONSTRAINTS) as SuspendedConstraint[]) || [];
}

export function removeSuspendFromSubst(
  subst: Subst,
  suspendIds: string[],
): Subst {
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

export function wakeUpSuspends(
  subst: Subst,
  newlyBoundVars: string[],
): Subst | null {
  const suspends = getSuspendsFromSubst(subst);
  if (suspends.length === 0) {
    return subst;
  }

  // Partition suspends: those to wake up, and those to keep
  const [toWake, toKeep] = suspends.reduce<
    [SuspendedConstraint[], SuspendedConstraint[]]
  >(
    ([wake, keep], s) =>
      s.watchedVars.some((v) => newlyBoundVars.includes(v))
        ? [[...wake, s], keep]
        : [wake, [...keep, s]],
    [[], []],
  );

  // Remove only the suspends to be woken up
  // let currentSubst = removeSuspendFromSubst(subst, toWake.map(x => x.id));
  let currentSubst = subst;

  for (const suspend of toWake) {
    const result = suspend.resumeFn(currentSubst);
    if (result === null) {
      return null;
    } else if (result === CHECK_LATER) {
      // If still needs to be suspended, add back to suspends
      toKeep.push(suspend);
    } else {
      toKeep.push(suspend);

      currentSubst = result;
    }
  }

  // currentSubst = removeSuspendFromSubst(subst, toWake.map(x => x.id));

  // Restore any suspends that remain (not woken, or still suspended)
  // if (toKeep.length > 0) {
  //   const newSubst = new Map(currentSubst);
  //   newSubst.set(SUSPENDED_CONSTRAINTS, toKeep);
  //   return newSubst;
  // }
  return currentSubst;
}
