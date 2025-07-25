import { Term, Subst, Goal, Var } from "./types.js";
import { SimpleObservable } from "./observable.js";
import { isVar, walk, unify, lvar } from "./kernel.js";
import { CHECK_LATER } from "./suspend-helper.js";

/**
 * Finite Domain Variable - a variable with an explicit domain of possible values
 */
export interface FDVar {
  id: string;
  domain: Set<any>;
  originalDomain: any[];
}

/**
 * CSP Constraint - a constraint that can propagate domain reductions
 */
export interface CSPConstraint {
  variables: string[]; // variable IDs this constraint affects
  propagate: (domains: Map<string, Set<any>>) => boolean; // returns false if unsatisfiable
  name: string; // for debugging
}

/**
 * CSP Solver state
 */
export class CSPSolver {
  private domains = new Map<string, Set<any>>();
  private constraints: CSPConstraint[] = [];
  private originalDomains = new Map<string, any[]>();

  addVariable(id: string, domain: any[]): void {
    this.domains.set(id, new Set(domain));
    this.originalDomains.set(id, [...domain]);
  }

  addConstraint(constraint: CSPConstraint): void {
    this.constraints.push(constraint);
  }

  /**
   * Remove a value from a variable's domain
   */
  removeValue(varId: string, value: any): boolean {
    const domain = this.domains.get(varId);
    if (!domain) return true;
    
    domain.delete(value);
    return domain.size > 0; // false if domain becomes empty
  }

  /**
   * Set a variable to a specific value (singleton domain)
   */
  assignValue(varId: string, value: any): boolean {
    const domain = this.domains.get(varId);
    if (!domain) return false;
    
    if (!domain.has(value)) return false; // value not in domain
    
    domain.clear();
    domain.add(value);
    return true;
  }

  /**
   * Get the current domain of a variable
   */
  getDomain(varId: string): Set<any> | undefined {
    return this.domains.get(varId);
  }

  /**
   * Check if a variable has a singleton domain (is assigned)
   */
  isAssigned(varId: string): boolean {
    const domain = this.domains.get(varId);
    return domain ? domain.size === 1 : false;
  }

  /**
   * Get the assigned value of a variable (if singleton domain)
   */
  getAssignedValue(varId: string): any | undefined {
    const domain = this.domains.get(varId);
    if (domain && domain.size === 1) {
      return Array.from(domain)[0];
    }
    return undefined;
  }

  /**
   * Constraint propagation - repeatedly apply constraints until fixpoint
   */
  propagate(): boolean {
    let changed = true;
    let iterations = 0;
    const maxIterations = 100; // prevent infinite loops
    
    while (changed && iterations < maxIterations) {
      changed = false;
      iterations++;
      
      for (const constraint of this.constraints) {
        if (!constraint.propagate(this.domains)) {
          return false; // constraint failed
        }
        
        // Check if any domain became empty
        for (const [varId, domain] of this.domains) {
          if (domain.size === 0) {
            return false;
          }
        }
      }
    }
    
    if (iterations >= maxIterations) {
      console.warn("CSP propagation hit iteration limit");
    }
    
    return true;
  }

  /**
   * Find the most constrained variable (smallest domain > 1)
   */
  chooseVariable(): string | null {
    let bestVar: string | null = null;
    let minDomainSize = Infinity;
    
    for (const [varId, domain] of this.domains) {
      if (domain.size > 1 && domain.size < minDomainSize) {
        minDomainSize = domain.size;
        bestVar = varId;
      }
    }
    
    return bestVar;
  }

  /**
   * Create a copy of the current solver state
   */
  clone(): CSPSolver {
    const clone = new CSPSolver();
    
    // Copy domains
    for (const [varId, domain] of this.domains) {
      clone.domains.set(varId, new Set(domain));
    }
    
    // Copy original domains
    for (const [varId, domain] of this.originalDomains) {
      clone.originalDomains.set(varId, [...domain]);
    }
    
    // Copy constraints (they're immutable)
    clone.constraints = [...this.constraints];
    
    return clone;
  }

  /**
   * Check if all variables are assigned
   */
  isComplete(): boolean {
    for (const domain of this.domains.values()) {
      if (domain.size !== 1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get all assigned values as a map
   */
  getAssignments(): Map<string, any> {
    const assignments = new Map<string, any>();
    for (const [varId, domain] of this.domains) {
      if (domain.size === 1) {
        assignments.set(varId, Array.from(domain)[0]);
      }
    }
    return assignments;
  }
}

/**
 * AllDifferent constraint - all variables must have different values
 */
export function allDifferentConstraint(variables: string[], name = "alldiff"): CSPConstraint {
  return {
    variables,
    name,
    propagate: (domains) => {
      const assignments = new Map<string, any>();
      const assignedValues = new Set<any>();
      
      // Collect assigned variables
      for (const varId of variables) {
        const domain = domains.get(varId);
        if (!domain) continue;
        
        if (domain.size === 1) {
          const value = Array.from(domain)[0];
          if (assignedValues.has(value)) {
            return false; // duplicate assignment
          }
          assignments.set(varId, value);
          assignedValues.add(value);
        }
      }
      
      // Remove assigned values from other variables' domains
      for (const varId of variables) {
        if (assignments.has(varId)) continue; // skip assigned variables
        
        const domain = domains.get(varId);
        if (!domain) continue;
        
        for (const assignedValue of assignedValues) {
          domain.delete(assignedValue);
        }
        
        if (domain.size === 0) {
          return false; // domain became empty
        }
      }
      
      return true;
    }
  };
}

/**
 * NotEqual constraint - two variables must have different values
 */
export function notEqualConstraint(var1: string, var2: string, name = "neq"): CSPConstraint {
  return {
    variables: [var1, var2],
    name: `${name}(${var1},${var2})`,
    propagate: (domains) => {
      const domain1 = domains.get(var1);
      const domain2 = domains.get(var2);
      
      if (!domain1 || !domain2) return true;
      
      // If var1 is assigned, remove that value from var2's domain
      if (domain1.size === 1) {
        const value1 = Array.from(domain1)[0];
        domain2.delete(value1);
        if (domain2.size === 0) return false;
      }
      
      // If var2 is assigned, remove that value from var1's domain
      if (domain2.size === 1) {
        const value2 = Array.from(domain2)[0];
        domain1.delete(value2);
        if (domain1.size === 0) return false;
      }
      
      return true;
    }
  };
}

/**
 * Equal constraint - two variables must have the same value
 */
export function equalConstraint(var1: string, var2: string, name = "eq"): CSPConstraint {
  return {
    variables: [var1, var2],
    name: `${name}(${var1},${var2})`,
    propagate: (domains) => {
      const domain1 = domains.get(var1);
      const domain2 = domains.get(var2);
      
      if (!domain1 || !domain2) return true;
      
      // Intersect the domains
      const intersection = new Set<any>();
      for (const value of domain1) {
        if (domain2.has(value)) {
          intersection.add(value);
        }
      }
      
      if (intersection.size === 0) return false;
      
      // Update both domains to the intersection
      domain1.clear();
      domain2.clear();
      for (const value of intersection) {
        domain1.add(value);
        domain2.add(value);
      }
      
      return true;
    }
  };
}

/**
 * Value constraint - assign a specific value to a variable
 */
export function valueConstraint(varId: string, value: any, name = "val"): CSPConstraint {
  return {
    variables: [varId],
    name: `${name}(${varId},${value})`,
    propagate: (domains) => {
      const domain = domains.get(varId);
      if (!domain) return true;
      
      if (!domain.has(value)) {
        return false; // value not in domain
      }
      
      // Set domain to only contain this value
      domain.clear();
      domain.add(value);
      
      return true;
    }
  };
}

/**
 * Not value constraint - exclude a specific value from a variable's domain
 */
export function notValueConstraint(varId: string, value: any, name = "notval"): CSPConstraint {
  return {
    variables: [varId],
    name: `${name}(${varId},${value})`,
    propagate: (domains) => {
      const domain = domains.get(varId);
      if (!domain) return true;
      
      domain.delete(value);
      
      return domain.size > 0; // false if domain becomes empty
    }
  };
}

/**
 * Arithmetic constraint - var1 + offset = var2
 */
export function arithmeticConstraint(var1: string, offset: number, var2: string, name = "arith"): CSPConstraint {
  return {
    variables: [var1, var2],
    name: `${name}(${var1}+${offset}=${var2})`,
    propagate: (domains) => {
      const domain1 = domains.get(var1);
      const domain2 = domains.get(var2);
      
      if (!domain1 || !domain2) return true;
      
      // Remove values from domain1 that don't have corresponding value+offset in domain2
      const validValues1 = new Set<any>();
      for (const value1 of domain1) {
        if (typeof value1 === 'number' && domain2.has(value1 + offset)) {
          validValues1.add(value1);
        }
      }
      
      // Remove values from domain2 that don't have corresponding value-offset in domain1  
      const validValues2 = new Set<any>();
      for (const value2 of domain2) {
        if (typeof value2 === 'number' && domain1.has(value2 - offset)) {
          validValues2.add(value2);
        }
      }
      
      if (validValues1.size === 0 || validValues2.size === 0) {
        return false;
      }
      
      // Update domains
      domain1.clear();
      domain2.clear();
      for (const value of validValues1) {
        domain1.add(value);
      }
      for (const value of validValues2) {
        domain2.add(value);
      }
      
      return true;
    }
  };
}

/**
 * Integration with the existing constraint system
 */
export function cspSolver(
  variables: { id: string; domain: any[]; output?: Term }[],
  constraints: CSPConstraint[]
): Goal {
  return (input$: SimpleObservable<Subst>) => new SimpleObservable<Subst>((observer) => {
    const sub = input$.subscribe({
      next: (subst) => {
        const solver = new CSPSolver();
        
        // Add variables to solver
        for (const { id, domain } of variables) {
          solver.addVariable(id, domain);
        }
        
        // Add constraints
        for (const constraint of constraints) {
          solver.addConstraint(constraint);
        }
        
        // Try to solve with backtracking
        const solutions = solveCSP(solver);
        
        // Emit solutions by unifying with the substitution
        for (const solution of solutions) {
          let resultSubst = subst;
          let valid = true;
          
          for (const { id, output } of variables) {
            if (output) {
              const value = solution.get(id);
              if (value !== undefined) {
                const unifyResult = unify(output, value, resultSubst);
                if (unifyResult === null) {
                  valid = false;
                  break;
                }
                resultSubst = unifyResult;
              }
            }
          }
          
          if (valid) {
            observer.next(resultSubst);
          }
        }
      },
      error: observer.error,
      complete: observer.complete,
    });

    return () => sub.unsubscribe();
  });
}

/**
 * Backtracking search for CSP
 */
function solveCSP(solver: CSPSolver): Map<string, any>[] {
  const solutions: Map<string, any>[] = [];
  
  function backtrack(currentSolver: CSPSolver): void {
    // Propagate constraints
    if (!currentSolver.propagate()) {
      return; // unsatisfiable
    }
    
    // Check if complete
    if (currentSolver.isComplete()) {
      solutions.push(currentSolver.getAssignments());
      return;
    }
    
    // Choose variable to branch on
    const varId = currentSolver.chooseVariable();
    if (!varId) return;
    
    const domain = currentSolver.getDomain(varId);
    if (!domain) return;
    
    // Try each value in the domain
    for (const value of Array.from(domain)) {
      const branchSolver = currentSolver.clone();
      
      // Assign the value
      if (branchSolver.assignValue(varId, value)) {
        backtrack(branchSolver);
      }
      
      // Limit number of solutions to prevent explosion
      if (solutions.length >= 10) {
        return;
      }
    }
  }
  
  backtrack(solver);
  return solutions;
}

/**
 * Helper to find a logic variable by ID in substitution
 */
function findVariableInSubst(subst: Subst, varId: string): Var | null {
  // Look for a variable with the given ID in the substitution
  for (const [key, value] of subst.entries()) {
    if (isVar(key) && (key as any).id === varId) {
      return key as Var;
    }
  }
  return null;
}
