import { BaseConfig, CacheConfig, LogConfig } from './types.ts';

export class ConfigurationManager {
  static createDefault(): BaseConfig {
    return {
      cache: {
        patternCacheEnabled: true,
        maxSize: 10000,
        ttlMs: 30 * 60 * 1000, // 30 minutes
        cleanupIntervalMs: 5 * 60 * 1000, // 5 minutes
      },
      logging: {
        enabled: false,
        ignoredIds: new Set([
          "JOIN_QUERY_EXECUTING",
          "SAME_TABLE_QUERY_EXECUTING",
          "GOAL_COMPLETE",
          "CHECKING_MERGE_CANDIDATES",
          "MERGE_CANDIDATE_CHECK",
          "MERGE_DETECTION_START",
          "RELATION_CREATED", 
          "DB_QUERY_EXECUTED",
          "FACT_ADDED",
          "PATTERN_CREATED",
          "SYMMETRIC_PATTERN_CREATED",
          "PROCESSING_LOOP_START",
          "PROCESSING_LOOP_END",
          "MEMORY_STATS",
        ]),
        criticalIds: new Set([])
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