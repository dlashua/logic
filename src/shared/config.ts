import { BaseConfig, CacheConfig, LogConfig } from './types.ts';

export class ConfigurationManager {
  static createDefault(): BaseConfig {
    return {
      cache: {
        patternCacheEnabled: true,
      },
      logging: {
        enabled: false,
        ignoredIds: new Set([
          "MULTIPLE_ROWS_SELECTCOLS_UNCHANGED",
          "PATTERN_ROWS_UPDATED",
          "MERGING_PATTERNS",
          "GROUNDING_SELECT_COL",
          "RUN_START",
          "PATTERN_CACHE_HIT",
          "ROW_CACHE_HIT",
          "QUERY_CACHE_HIT",
          "DB_QUERY",
          "ROW_CACHE_SET",
          "CACHE_HIT",
          "MATCHED_PATTERNS",
          "UNMATCHED_QUERYOBJ",
          "MERGE_PATTERNS_START",
          "SKIPPED_PATTERN",
          "MERGE_PATTERNS_END",
          "RUN_END",
          "PATTERNS AFTER",
          "RAN FALSE PATTERNS",
          "PATTERNS BEFORE",
          "FINAL PATTERNS",
          "FINAL PATTERNS SYM",
          "MEMORY_SCAN",
          "INDEX_LOOKUP",
          "FACT_MATCH"
        ]),
        criticalIds: new Set(["SELECTCOLS MISMATCH PATTERNS"])
      }
    };
  }

  static create(overrides: Partial<BaseConfig> = {}): BaseConfig {
    const defaultConfig = ConfigurationManager.createDefault();
    return {
      cache: {
        ...defaultConfig.cache,
        ...overrides.cache 
      },
      logging: { 
        ...defaultConfig.logging, 
        ...overrides.logging,
        ignoredIds: overrides.logging?.ignoredIds || defaultConfig.logging.ignoredIds,
        criticalIds: overrides.logging?.criticalIds || defaultConfig.logging.criticalIds
      }
    };
  }
}