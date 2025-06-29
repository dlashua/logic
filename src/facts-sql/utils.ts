import {
  Term,
  Subst,
  isVar,
  walk,
  unify
} from '../core.ts'
import { WhereClause } from './types.ts';

export const queryUtils = {
  /**
   * Walk all keys of an object with a substitution and return a new object
   */
  async walkAllKeys<T extends Record<string, Term>>(
    obj: T,
    subst: Subst
  ): Promise<Record<string, Term>> {
    const result: Record<string, Term> = {};
    for (const key of Object.keys(obj)) {
      result[key] = await walk(obj[key], subst);
    }
    return result;
  },

  /**
   * Check if all query parameters are grounded (no variables)
   */
  allParamsGrounded(params: Record<string, Term>): boolean {
    return Object.values(params).every(param => !isVar(param));
  },

  /**
   * Build a cache key for queries
   */
  buildCacheKey(table: string, selectCols: string[], whereClauses: WhereClause[]): string {
    return JSON.stringify({
      table,
      select: [...selectCols].sort(),
      where: [...whereClauses].sort((a, b) => a.column.localeCompare(b.column)),
    });
  },

  /**
   * Build a row cache key for fully grounded queries
   */
  buildRowCacheKey(table: string, params: Record<string, Term>): string {
    const key = Object.keys(params).sort().map(k => `${k}:${params[k]}`).join("|");
    return `${table}|${key}`;
  },

  /**
   * Build query parts from parameters and substitution
   */
  async buildQueryParts(params: Record<string, Term>, subst: Subst) {
    const selectCols = Object.keys(params).sort();
    const walkedQ: Record<string, Term> = {};
    const whereClauses: WhereClause[] = [];
    
    for (const col of selectCols) {
      walkedQ[col] = await walk(params[col], subst);
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
  }
};

export const unificationUtils = {
  /**
   * Unify all selectCols in a row with walkedQ and subst
   */
  async unifyRowWithWalkedQ(
    selectCols: string[],
    walkedQ: Record<string, Term>,
    row: Record<string, any>,
    subst: Subst,
  ): Promise<Subst | null> {
    let s2: Subst = new Map(subst);
    
    for (const col of selectCols) {
      if (!isVar(walkedQ[col])) {
        if (walkedQ[col] === row[col]) {
          continue;
        } else {
          return null;
        }
      } else {
        const unified = await unify(walkedQ[col], row[col], s2);
        if (unified) {
          s2 = unified;
        } else {
          return null;
        }
      }
    }
    
    return s2;
  }
};

export const patternUtils = {
  /**
   * Check if all select columns are tags (have id property)
   */
  allSelectColsAreTags(cols: Record<string, Term>): boolean {
    return Object.values(cols).every((x: Term) => (x as any).id);
  },

  /**
   * Separate query object into select and where columns
   */
  separateQueryColumns(queryObj: Record<string, Term>) {
    const selectCols: Record<string, Term> = {};
    const whereCols: Record<string, Term> = {};

    for (const [key, value] of Object.entries(queryObj)) {
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
   * Separate symmetric query values into select and where
   */
  separateSymmetricColumns(queryObj: Record<string, Term>) {
    const selectCols: Term[] = [];
    const whereCols: Term[] = [];
    
    for (const value of Object.values(queryObj)) {
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