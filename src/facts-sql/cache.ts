import { CacheType, CacheConfig, Pattern, SymmetricPattern } from './types.ts';
import { Logger } from './logger.ts';

export class QueryCache {
  private patternCache = new Map<string, any>();
  private rowCache = new Map<string, any>();
  private recordCache = new Map<string, any>();

  constructor(
    private config: CacheConfig,
    private logger: Logger
  ) {}

  get(key: string, type: CacheType): any | null {
    switch (type) {
    case 'pattern':
      return this.config.patternCacheEnabled ? this.patternCache.get(key) || null : null;
    case 'row':
      return this.config.rowCacheEnabled ? this.rowCache.get(key) || null : null;
    case 'query':
      return this.config.recordCacheEnabled ? this.recordCache.get(key) || null : null;
    default:
      return null;
    }
  }

  set(key: string, value: any, type: CacheType): void {
    switch (type) {
    case 'pattern':
      if (this.config.patternCacheEnabled) {
        this.patternCache.set(key, value);
      }
      break;
    case 'row':
      if (this.config.rowCacheEnabled) {
        this.rowCache.set(key, value);
        this.logger.log("ROW_CACHE_SET", {
          key,
          value 
        });
      }
      break;
    case 'query':
      if (this.config.recordCacheEnabled) {
        this.recordCache.set(key, value);
      }
      break;
    }
  }

  clear(type?: CacheType): void {
    if (!type) {
      this.patternCache.clear();
      this.rowCache.clear();
      this.recordCache.clear();
      return;
    }

    switch (type) {
    case 'pattern':
      this.patternCache.clear();
      break;
    case 'row':
      this.rowCache.clear();
      break;
    case 'query':
      this.recordCache.clear();
      break;
    }
  }

  /**
   * Find matching pattern in cache for regular patterns
   */
  findMatchingPattern(patterns: Pattern[], currentPattern: Pattern): Pattern | null {
    if (!this.config.patternCacheEnabled) return null;

    return patterns.find(otherPattern => {
      return (
        otherPattern !== currentPattern &&
        JSON.stringify(otherPattern.whereCols) === JSON.stringify(currentPattern.whereCols) && 
        otherPattern.ran === true
      );
    }) || null;
  }

  /**
   * Find matching pattern in cache for symmetric patterns
   */
  findMatchingSymmetricPattern(patterns: SymmetricPattern[], currentPattern: SymmetricPattern): SymmetricPattern | null {
    if (!this.config.patternCacheEnabled) return null;

    return patterns.find(otherPattern => {
      return (
        otherPattern !== currentPattern &&
        JSON.stringify(otherPattern.whereCols) === JSON.stringify(currentPattern.whereCols) && 
        otherPattern.ran === true
      );
    }) || null;
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
}