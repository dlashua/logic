import { BaseCache } from '../shared/cache.ts';
import { Logger } from '../shared/logger.ts';
import { CacheType, CacheConfig, Pattern, SymmetricPattern } from './types.ts';
import { PatternComparator } from './pattern-comparator.ts';

export class QueryCache extends BaseCache {
  private patternComparator = new PatternComparator();

  constructor(
    config: CacheConfig,
    logger: Logger
  ) {
    super(config, logger);
  }

  /**
   * Find matching pattern using optimized comparison
   */
  findMatchingPattern(patterns: Pattern[], currentPattern: Pattern, cacheTTL?: number): Pattern | null {
    if (!this.config.patternCacheEnabled) return null;
    
    // Filter out expired patterns if TTL is provided
    let validPatterns = patterns;
    if (cacheTTL !== undefined) {
      const now = Date.now();
      validPatterns = patterns.filter(pattern => {
        if (!pattern.timestamp) return true; // No timestamp means no expiration
        return (now - pattern.timestamp) <= cacheTTL;
      });
    }
    
    return this.patternComparator.findMatchingPattern(validPatterns, currentPattern);
  }

  /**
   * Find matching symmetric pattern using optimized comparison
   */
  findMatchingSymmetricPattern(patterns: SymmetricPattern[], currentPattern: SymmetricPattern): SymmetricPattern | null {
    if (!this.config.patternCacheEnabled) return null;
    return this.patternComparator.findMatchingSymmetricPattern(patterns, currentPattern);
  }

  /**
   * Process cached pattern results
   */
  processCachedPatternResult(matchingPattern: Pattern | SymmetricPattern, targetPattern?: Pattern | SymmetricPattern): any[] {
    if ((matchingPattern.selectCols as any).length === 0) {
      return matchingPattern.rows[0] === true ? ["HERE"] : [];
    } else {
      // If we have a target pattern, filter the results to match the target's additional where conditions
      if (targetPattern && this.needsFiltering(matchingPattern, targetPattern)) {
        return this.filterPatternResults(matchingPattern, targetPattern);
      }
      return matchingPattern.rows;
    }
  }

  /**
   * Check if cached results need filtering for the target pattern
   */
  private needsFiltering(cachedPattern: Pattern | SymmetricPattern, targetPattern: Pattern | SymmetricPattern): boolean {
    // Only handle regular patterns for now
    if (Array.isArray(cachedPattern.whereCols) || Array.isArray(targetPattern.whereCols)) {
      return false;
    }
    
    const cachedWhere = cachedPattern.whereCols as Record<string, any>;
    const targetWhere = targetPattern.whereCols as Record<string, any>;
    
    // Check if target has more where conditions than cached
    return Object.keys(targetWhere).length > Object.keys(cachedWhere).length;
  }

  /**
   * Filter cached pattern results to match target pattern's additional where conditions
   */
  private filterPatternResults(cachedPattern: Pattern | SymmetricPattern, targetPattern: Pattern | SymmetricPattern): any[] {
    if (Array.isArray(cachedPattern.whereCols) || Array.isArray(targetPattern.whereCols)) {
      return cachedPattern.rows;
    }
    
    const cachedWhere = cachedPattern.whereCols as Record<string, any>;
    const targetWhere = targetPattern.whereCols as Record<string, any>;
    const targetSelect = targetPattern.selectCols as Record<string, any>;
    
    // Find additional conditions in target that aren't in cached
    const additionalConditions: Record<string, any> = {};
    for (const [key, value] of Object.entries(targetWhere)) {
      if (!(key in cachedWhere)) {
        additionalConditions[key] = value;
      }
    }
    
    // If target is a confirmation query (no select columns), return boolean result
    if (Object.keys(targetSelect).length === 0) {
      // Filter cached rows to see if any match the additional conditions
      const hasMatch = cachedPattern.rows.some(row => {
        return Object.entries(additionalConditions).every(([key, value]) => row[key] === value);
      });
      return hasMatch ? [true] : [false];
    }
    
    // For select queries, filter and project the results
    return cachedPattern.rows
      .filter(row => {
        return Object.entries(additionalConditions).every(([key, value]) => row[key] === value);
      })
      .map(row => {
        const selectKeys = Object.keys(targetSelect);
        if (selectKeys.length === 0 || selectKeys.includes('*')) {
          return row;
        }
        const projectedRow: any = {};
        for (const key of selectKeys) {
          projectedRow[key] = row[key];
        }
        return projectedRow;
      });
  }

  clear(type?: CacheType): void {
    super.clear(type);
    if (!type || type === 'pattern') {
      this.patternComparator.clearCache();
    }
  }
}