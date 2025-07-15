import { Subst } from "./types.ts";

export const SUSPENDED_CONSTRAINTS = Symbol('suspended-constraints');

export interface SuspendedConstraint {
  id: string;
  resumeFn: (subst: Subst) => Subst | null;
  watchedVars: string[];
}

let constraintCounter = 0;

export function addSuspendToSubst(
  subst: Subst,
  resumeFn: (subst: Subst) => Subst | null,
  watchedVars: string[]
): Subst {
  const suspends = (subst.get(SUSPENDED_CONSTRAINTS) as SuspendedConstraint[]) || [];
  const newSuspend: SuspendedConstraint = {
    id: `constraint_${constraintCounter++}`,
    resumeFn,
    watchedVars
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
  suspendIds: string[]
): Subst {
  const suspends = getSuspendsFromSubst(subst);
  const filteredSuspends = suspends.filter(c => !suspendIds.includes(c.id));
  
  const newSubst = new Map(subst);
  if (filteredSuspends.length === 0) {
    newSubst.delete(SUSPENDED_CONSTRAINTS);
  } else {
    newSubst.set(SUSPENDED_CONSTRAINTS, filteredSuspends);
  }
  return newSubst;
}

export function wakeUpSuspends(subst: Subst, newlyBoundVars: string[]): Subst {
  let currentSubst = subst;
  let changed = true;

  // Iterate until no new bindings are made (fixpoint)
  while (changed) {
    const suspends = getSuspendsFromSubst(currentSubst);
    if (suspends.length === 0) break;

    changed = false;
    const suspendsToRemove: string[] = [];

    for (const suspend of suspends) {
      const hasNewBinding = suspend.watchedVars.some(varId => 
        newlyBoundVars.includes(varId)
      );

      if (hasNewBinding) {
        const substWithoutThisSuspend = removeSuspendFromSubst(currentSubst, [suspend.id]);
        try {
          const result = suspend.resumeFn(substWithoutThisSuspend);
          if (result !== null) {
            // Check for new bindings to trigger further wake-ups
            const newBindings = [...result.keys()].filter(
              k => typeof k === 'string' && !currentSubst.has(k)
            );
            if (newBindings.length > 0) {
              newlyBoundVars.push(...newBindings);
              changed = true;
            }
            currentSubst = result;
            suspendsToRemove.push(suspend.id);
          } else {
            // Re-add the constraint if it fails to resolve
            currentSubst = addSuspendToSubst(substWithoutThisSuspend, suspend.resumeFn, suspend.watchedVars);
          }
        } catch (error) {
          console.error(`Error resuming constraint ${suspend.id}:`, error);
          // Continue processing other constraints
        }
      }
    }

    if (suspendsToRemove.length > 0) {
      currentSubst = removeSuspendFromSubst(currentSubst, suspendsToRemove);
      changed = true;
    }
  }

  return currentSubst;
}