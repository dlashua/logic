import util from 'node:util';

const DEFAULT_CONFIG = {
  enabled: true,
  allowedIds: new Set<string>(), // empty means allow all
  deniedIds: new Set<string>([
    "SHARED_GOALS",
    "DB_QUERY",
    "NO_DB_ROWS",
    "UNIFY_SUCCESS",
    "UNIFY_FAILED",
    "DB_ROWS",
    // "GOAL_CREATED", // Enabled to see queryObj values
    // "MERGEABLE_CHECK", // Enabled to see merge detection
    // "PENDING_QUERIES_DEBUG", // Enabled to debug pending queries
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