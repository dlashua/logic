import util from 'node:util';

const DEFAULT_CONFIG = {
  enabled: false,
  allowedIds: new Set<string>([
  ]), // empty means allow all
  deniedIds: new Set<string>([
    "UNIFY_SUCCESS",
    "UNIFY_FAILED",

    "THIS_GOAL_ROWS",
    // "ALL_GOAL_ROWS",
    // "DB_ROWS",
    "COMMON_GOALS",
    "DB_QUERY",

    "GOAL_STARTED",
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

  log(id: string, data: Record<string, any> | string): void {
    if (!this.config.enabled) return;
    
    if (this.config.deniedIds.has(id)) return;
    
    if (this.config.allowedIds.size > 0 && !this.config.allowedIds.has(id)) return;
    
    if(typeof data === "string") {
      console.log(`[${id}] ${data}`);
    } else {
      console.log(`[${id}]`, util.inspect(data, {
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