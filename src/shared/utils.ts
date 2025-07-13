import { keyBy } from 'lodash';
import { Term, Subst, Var } from '../core/types.ts';
import { isVar , walk, unify } from "../core/kernel.ts";
import { WhereClause } from './types.ts';

export const queryUtils = {

  /**
   * Walk all keys of an object with a substitution and return a new object
   */
  walkAllKeys<T extends Record<string, Term>>(
    obj: T,
    subst: Subst
  ): Record<string, Term> {
    const result: Record<string, Term> = {};
    const keys = Object.keys(obj);

    for (const key of keys) {
      result[key] = walk(obj[key], subst);
    }

    return result;
  },

  /**
   * Walk all values in an array with a substitution
   */
  walkAllArray(
    arr: Term[],
    subst: Subst
  ): Term[] {
    return arr.map(term => walk(term, subst));
  },

  /**
   * Check if all query parameters are grounded (no variables)
   */
  allParamsGrounded(params: Record<string, Term>): boolean {
    const values = Object.values(params);
    for (let i = 0; i < values.length; i++) {
      if (isVar(values[i])) return false;
    }
    return true;
  },

  /**
   * Check if all array elements are grounded (no variables)
   */
  allArrayGrounded(arr: Term[]): boolean {
    for (let i = 0; i < arr.length; i++) {
      if (isVar(arr[i])) return false;
    }
    return true;
  },

  /**
   * Build query parts from parameters and substitution
   */
  buildQueryParts(params: Record<string, Term>, subst: Subst) {
    const selectCols = Object.keys(params).sort();
    const walkedQ: Record<string, Term> = {};
    const whereClauses: WhereClause[] = [];
    
    for (const col of selectCols) {
      walkedQ[col] = walk(params[col], subst);
      if (!isVar(walkedQ[col])) {
        whereClauses.push({
          column: col,
          value: walkedQ[col] 
        });
      }
    }
    
    return {
      selectCols,
      whereClauses,
      walkedQ 
    };
  },

  onlyGrounded<T>(params: Record<string, Term<T>>) {
    return Object.fromEntries(
      Object.entries(params).filter(
        ([key, value]) => !isVar(value)
      )
    ) as Record<string, T>;
  },

  onlyVars(params: Record<string, Term>) {
    return Object.fromEntries(
      Object.entries(params).filter(
        ([key, value]) => isVar(value)
      )
    ) as Record<string, Var>;
  }
};

export const unificationUtils = {

  /**
   * Unify all selectCols in a row with walkedQ and subst
   */
  unifyRowWithWalkedQ(
    selectCols: string[],
    walkedQ: Record<string, Term>,
    row: Record<string, any>,
    subst: Subst,
  ): Subst | null {
    let s2: Subst = subst;
    let needsClone = true;
    
    for (let i = 0; i < selectCols.length; i++) {
      const col = selectCols[i];
      if (!isVar(walkedQ[col])) {
        if (walkedQ[col] !== row[col]) {
          return null;
        }
      } else {
        if (needsClone) {
          s2 = new Map(subst);
          needsClone = false;
        }
        
        const unified = unify(walkedQ[col], row[col], s2);
        if (unified) {
          s2 = unified;
        } else {
          return null;
        }
      }
    }
    
    return s2;
  },

  /**
   * Unify arrays element by element
   */
  unifyArrays(
    queryArray: Term[],
    factArray: Term[],
    subst: Subst
  ): Subst | null {
    if (queryArray.length !== factArray.length) {
      return null;
    }
    
    return unify(queryArray, factArray, subst);
  }
};

export const patternUtils = {

  /**
   * Check if all select columns are tags (have id property)
   */
  allSelectColsAreTags(cols: Record<string, Term>): boolean {
    const values = Object.values(cols);
    for (let i = 0; i < values.length; i++) {
      if (!(values[i] as any).id) return false;
    }
    return true;
  },

  /**
   * Separate query object into select and where columns
   */
  separateQueryColumns(queryObj: Record<string, Term>) {
    const selectCols: Record<string, Term> = {};
    const whereCols: Record<string, Term> = {};
    const entries = Object.entries(queryObj);

    for (let i = 0; i < entries.length; i++) {
      const [key, value] = entries[i];
      if (isVar(value)) {
        selectCols[key] = value;
      } else {
        whereCols[key] = value;
      }
    }

    return {
      selectCols,
      whereCols 
    };
  },

  /**
   * Separate array query into select and where terms
   */
  separateArrayQuery(queryArray: Term[]) {
    const selectTerms: Term[] = [];
    const whereTerms: Term[] = [];
    const positions: number[] = [];
    
    for (let i = 0; i < queryArray.length; i++) {
      const term = queryArray[i];
      if (isVar(term)) {
        selectTerms.push(term);
        positions.push(i);
      } else {
        whereTerms.push(term);
      }
    }
    
    return {
      selectTerms,
      whereTerms,
      positions
    };
  },

  /**
   * Separate symmetric query values into select and where - optimized
   */
  separateSymmetricColumns(queryObj: Record<string, Term>) {
    const selectCols: Term[] = [];
    const whereCols: Term[] = [];
    const values = Object.values(queryObj);
    
    for (let i = 0; i < values.length; i++) {
      const value = values[i];
      if (isVar(value)) {
        selectCols.push(value);
      } else {
        whereCols.push(value);
      }
    }
    
    return {
      selectCols,
      whereCols 
    };
  }
};

export const indexUtils = {

  /**
   * Returns the intersection of two sets
   */
  intersect<T>(setA: Set<T>, setB: Set<T>): Set<T> {
    const result = new Set<T>();
    setA.forEach(item => {
      if (setB.has(item)) {
        result.add(item);
      }
    });
    return result;
  },

  /**
   * Returns true if a value is indexable (string, number, boolean, or null)
   */
  isIndexable(v: any): boolean {
    return typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null;
  },

  /**
   * Create an index for a specific position/key
   */
  createIndex<T>(): Map<T, Set<number>> {
    return new Map<T, Set<number>>();
  },

  /**
   * Add a value to an index
   */
  addToIndex<T>(index: Map<T, Set<number>>, key: T, factIndex: number): void {
    let set = index.get(key);
    if (!set) {
      set = new Set<number>();
      index.set(key, set);
    }
    set.add(factIndex);
  }
};

// Export individual functions for backward compatibility
export const intersect = indexUtils.intersect;
export const isIndexable = indexUtils.isIndexable;