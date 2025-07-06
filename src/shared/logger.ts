import util from 'node:util';

const DEFAULT_CONFIG = {
  enabled: true,
  allowedIds: new Set<string>([
    // "FLUSH_BATCH",
    // "FLUSH_BATCH_COMPLETE",
    // "GOAL_NEXT",
    // "UPSTREAM_GOAL_COMPLETE",
    // "GOAL_COMPLETE",
    // "GOAL_CANCELLED",
    // "FLUSH_BATCH_CANCELLED_AFTER_QUERY",
    // "FLUSH_BATCH_CANCELLED_DURING_ROWS",
    // "FLUSH_BATCH_CANCELLED_DURING_SUBST",
    // "DB_QUERY_BATCH",
    // "CACHE_HIT",
    // "CACHE_MISS",
    "UNIFY_SUCCESS",
    "UNIFY_FAILURE",

    // "GOAL_BATCH_KEY_UPDATED",
    // "ABOUT_TO_CALL_CACHE_OR_QUERY",
    // "CACHE_OR_QUERY_START",
    // "COMPATIBLE_GOALS",
    // "ABOUT_TO_PROCESS_GOAL",
    // "GOAL_GROUP_INFO", 
    "DB_ROWS",
    "DB_NO_ROWS",
    // "FLUSH_BATCH",
    // "COMPATIBLE_MERGE_GOALS",
    // "DB_QUERY_MERGED",
    // "DB_ROWS_MERGED",
    // "ABOUT_TO_CALL_CACHE_OR_QUERY",
    // "USING_GOAL_MERGING",
    // "USING_GOAL_CACHING",
    // "USING_SUBSTITUTION_BATCHING",
    // "CACHE_PERFORMANCE",
    // "BATCH_PERFORMANCE",
    "CACHE_HIT_IMMEDIATE",
    "CACHE_MISS_TO_BATCH",
    // "PROCESSING_CACHE_MISSES",
    // "EXECUTING_QUERY_FOR_CACHE_MISSES",
    // "SINGLE_CACHE_MISS_WITH_GOAL_MERGING",
    // "EXECUTING_UNIFIED_QUERY", 
    // "DB_QUERY_UNIFIED",
    // "POPULATING_CACHE_FOR_COMPATIBLE_GOALS",
    // "MERGING_COMPATIBLE_GOALS",
    // "COMPATIBLE_GOALS",
    // "CACHED_FOR_OTHER_GOAL",
    // "CROSS_GROUP_CACHE_CHECK",
    // "OUTER_GROUP_CACHE_POPULATION",
    // "GOAL_STARTED",
    // "FOUND_RELATED_GOALS",
    // "MERGE_COMPATIBILITY_CHECK",
    // "CACHE_COMPATIBILITY_CHECK",
    // "SINGLE_QUERY_COLUMN_SELECTION",
    // "MERGED_QUERY_COLUMN_SELECTION",

  ]), // empty means allow all
  deniedIds: new Set<string>([
    "UNIFY_FAILED",

    "THIS_GOAL_ROWS",
    // "ALL_GOAL_ROWS",
    "COMMON_GOALS",
    "DB_QUERY",

    "GOAL_CREATED",
    "SAW_CACHE",
    // "SHARED_GOALS", // Disabled to reduce noise
    // "DB_QUERY", // Disabled to reduce noise
    // "NO_DB_ROWS",
    // "DB_ROWS",
    // "GOAL_CREATED", // Disabled to reduce noise
    // "MERGEABLE_CHECK", // Disabled to reduce noise  
    // "PENDING_QUERIES_DEBUG", // Disabled to reduce noise
    // "MERGE_DEBUG", // Disabled to reduce noise
    // "PENDING_ADD", // Disabled to reduce noise

    // "CACHE_HIT", // Enabled to see cache hits
    // "SHARED_UNIFY", // Enabled to see shared unification
  ]), // specific ids to deny
};

export interface LoggerConfig {
  enabled: boolean;
  allowedIds: Set<string>;
  deniedIds: Set<string>;
}

export class Logger {
  constructor(private config: LoggerConfig) {}

  log(id: string, data: Record<string, any> | string | (() => Record<string, any> | string)): void {
    if (!this.config.enabled) return;
    if (this.config.deniedIds.has(id)) return;
    if (this.config.allowedIds.size > 0 && !this.config.allowedIds.has(id)) return;

    let out: Record<string, any> | string;
    if (typeof data === "function") {
      out = data();
    } else {
      out = data;
    }

    if(typeof out === "string") {
      console.log(`[${id}] ${out}`);
    } else {
      console.log(`[${id}]`, util.inspect(out, {
        depth: null,
        colors: true 
      }));
    }
    // console.log();
  }
}

let defaultLoggerInstance: Logger | null = null;

export function getDefaultLogger(): Logger {
  if (!defaultLoggerInstance) {
    defaultLoggerInstance = new Logger(DEFAULT_CONFIG);
  }
  return defaultLoggerInstance;
}