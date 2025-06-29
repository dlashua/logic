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
  findMatchingPattern(patterns: Pattern[], currentPattern: Pattern): Pattern | null {
    if (!this.config.patternCacheEnabled) return null;
    return this.patternComparator.findMatchingPattern(patterns, currentPattern);
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
  processCachedPatternResult(matchingPattern: Pattern | SymmetricPattern): any[] {
    if ((matchingPattern.selectCols as any).length === 0) {
      return matchingPattern.rows[0] === true ? ["HERE"] : [];
    } else {
      return matchingPattern.rows;
    }
  }

  clear(type?: CacheType): void {
    super.clear(type);
    if (!type || type === 'pattern') {
      this.patternComparator.clearCache();
    }
  }
}