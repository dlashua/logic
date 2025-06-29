// SQL-specific utilities extending shared utilities
import { queryUtils as baseQueryUtils, unificationUtils, patternUtils } from '../shared/utils.ts';
import { Term } from '../core.ts';
import { WhereClause } from './types.ts';

export const queryUtils = {
  ...baseQueryUtils,

  /**
   * Build a cache key for queries - optimized to avoid JSON.stringify
   */
  buildCacheKey(table: string, selectCols: string[], whereClauses: WhereClause[]): string {
    const selectKey = selectCols.slice().sort().join(',');
    const whereKey = whereClauses
      .slice()
      .sort((a, b) => a.column.localeCompare(b.column))
      .map(w => `${w.column}=${w.value}`)
      .join('&');
    return `${table}|${selectKey}|${whereKey}`;
  },

  /**
   * Build a row cache key for fully grounded queries - optimized
   */
  buildRowCacheKey(table: string, params: Record<string, Term>): string {
    const keys = Object.keys(params).sort();
    const parts = new Array(keys.length + 1);
    parts[0] = table;
    
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      parts[i + 1] = `${key}:${params[key]}`;
    }
    
    return parts.join('|');
  }
};

// Re-export shared utilities
export { unificationUtils, patternUtils };