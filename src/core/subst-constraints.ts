import { Subst } from "./types.ts";

// Symbol for storing constraints in substitutions
export const SUSPENDED_CONSTRAINTS = Symbol('suspended-constraints');

export interface SuspendedConstraint {
  id: string;
  constraint: (subst: Subst) => Subst | null;
  watchedVars: string[];
}

/**
 * Add a constraint to a substitution
 */
export function addConstraintToSubst(
  subst: Subst,
  constraint: (subst: Subst) => Subst | null,
  watchedVars: string[]
): Subst {
  const constraints = (subst.get(SUSPENDED_CONSTRAINTS) as SuspendedConstraint[]) || [];
  const newConstraint: SuspendedConstraint = {
    id: `constraint_${Date.now()}_${Math.random()}`,
    constraint,
    watchedVars
  };
  
  const newSubst = new Map(subst);
  newSubst.set(SUSPENDED_CONSTRAINTS, [...constraints, newConstraint]);
  return newSubst;
}

/**
 * Get constraints from a substitution
 */
function getConstraintsFromSubst(subst: Subst): SuspendedConstraint[] {
  return (subst.get(SUSPENDED_CONSTRAINTS) as SuspendedConstraint[]) || [];
}

/**
 * Remove constraints from a substitution
 */
function removeConstraintsFromSubst(
  subst: Subst, 
  constraintIds: string[]
): Subst {
  const constraints = getConstraintsFromSubst(subst);
  const filteredConstraints = constraints.filter(c => !constraintIds.includes(c.id));
  
  if (filteredConstraints.length === 0) {
    const newSubst = new Map(subst);
    newSubst.delete(SUSPENDED_CONSTRAINTS);
    return newSubst;
  } else {
    const newSubst = new Map(subst);
    newSubst.set(SUSPENDED_CONSTRAINTS, filteredConstraints);
    return newSubst;
  }
}

/**
 * Check if any constraints can be woken up by newly bound variables
 */
export function wakeUpConstraints(subst: Subst, newlyBoundVars: string[]): Subst {
  const constraints = getConstraintsFromSubst(subst);
  if (constraints.length === 0) return subst;
  
  let currentSubst = subst;
  const constraintsToRemove: string[] = [];
  
  for (const constraint of constraints) {
    // Check if any watched variables were newly bound
    const hasNewBinding = constraint.watchedVars.some(varId => 
      newlyBoundVars.includes(varId)
    );
    
    if (hasNewBinding) {
      // Remove this constraint from the current substitution before evaluating
      const substWithoutThisConstraint = removeConstraintsFromSubst(
        currentSubst, 
        [constraint.id]
      );
      
      // Try to evaluate the constraint
      const result = constraint.constraint(substWithoutThisConstraint);
      if (result !== null) {
        currentSubst = result;
        constraintsToRemove.push(constraint.id);
      } else {
        // Keep the constraint in the substitution
      }
    }
  }
  
  // Remove successfully evaluated constraints
  if (constraintsToRemove.length > 0) {
    currentSubst = removeConstraintsFromSubst(currentSubst, constraintsToRemove);
  }
  
  return currentSubst;
}
