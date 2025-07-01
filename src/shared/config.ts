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
          // "JOIN_QUERY_EXECUTING",

          // "GOAL_COMPLETE",
          // "CHECKING_MERGE_CANDIDATES",
          // "MERGE_CANDIDATE_CHECK",
          // "MERGE_DETECTION_START",
          // "RELATION_CREATED",

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