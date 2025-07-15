import { Subst } from "./types.ts";

// Symbol for storing constraints in substitutions
export const SUSPENDED_CONSTRAINTS = Symbol('suspended-constraints');

export interface SuspendedConstraint {
  id: string;
  resumeFn: (subst: Subst) => Subst | null;
  watchedVars: string[];
}

/**
 * Add a constraint to a substitution
 */
export function addSuspendToSubst(
  subst: Subst,
  resumeFn: (subst: Subst) => Subst | null,
  watchedVars: string[]
): Subst {
  const suspends = (subst.get(SUSPENDED_CONSTRAINTS) as SuspendedConstraint[]) || [];
  const newSuspends: SuspendedConstraint = {
    id: `constraint_${Date.now()}_${Math.random()}`,
    resumeFn,
    watchedVars
  };
  
  const newSubst = new Map(subst);
  newSubst.set(SUSPENDED_CONSTRAINTS, [...suspends, newSuspends]);
  return newSubst;
}

/**
 * Get constraints from a substitution
 */
function getSuspendsFromSubst(subst: Subst): SuspendedConstraint[] {
  return (subst.get(SUSPENDED_CONSTRAINTS) as SuspendedConstraint[]) || [];
}

/**
 * Remove constraints from a substitution
 */
function removeSuspendFromSubst(
  subst: Subst, 
  suspendIds: string[]
): Subst {
  const suspends = getSuspendsFromSubst(subst);
  const filteredSuspends = suspends.filter(c => !suspendIds.includes(c.id));
  
  if (filteredSuspends.length === 0) {
    const newSubst = new Map(subst);
    newSubst.delete(SUSPENDED_CONSTRAINTS);
    return newSubst;
  } else {
    const newSubst = new Map(subst);
    newSubst.set(SUSPENDED_CONSTRAINTS, filteredSuspends);
    return newSubst;
  }
}

/**
 * Check if any constraints can be woken up by newly bound variables
 */
export function wakeUpSuspends(subst: Subst, newlyBoundVars: string[]): Subst {
  const suspends = getSuspendsFromSubst(subst);
  if (suspends.length === 0) return subst;
  
  let currentSubst = subst;
  const suspendsToRemove: string[] = [];
  
  for (const suspend of suspends) {
    // Check if any watched variables were newly bound
    const hasNewBinding = suspend.watchedVars.some(varId => 
      newlyBoundVars.includes(varId)
    );
    
    if (hasNewBinding) {
      // Remove this constraint from the current substitution before evaluating
      const substWithoutThisSuspend = removeSuspendFromSubst(
        currentSubst, 
        [suspend.id]
      );
      
      // Try to evaluate the constraint
      const result = suspend.resumeFn(substWithoutThisSuspend);
      if (result !== null) {
        currentSubst = result;
        suspendsToRemove.push(suspend.id);
      } else {
        // Keep the constraint in the substitution
      }
    }
  }
  
  // Remove successfully evaluated constraints
  if (suspendsToRemove.length > 0) {
    currentSubst = removeSuspendFromSubst(currentSubst, suspendsToRemove);
  }
  
  return currentSubst;
}
