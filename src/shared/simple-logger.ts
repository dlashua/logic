import util from 'node:util';

const DEFAULT_CONFIG = {
  enabled: false,
  allowedIds: new Set<string>(), // empty means allow all
  deniedIds: new Set<string>([
    "SHARED_GOALS",
    "DB_QUERY",
    "NO_DB_ROWS",
    "CACHE_HIT",
    "UNIFY_SUCCESS",
    "UNIFY_FAILED",
    "STORED_QUERY",
    "DB_ROWS",
    "CACHE_WRITTEN",
    // "CACHE_LOOKUP",
  ]), // specific ids to deny
};

export interface SimpleLoggerConfig {
  enabled: boolean;
  allowedIds: Set<string>;
  deniedIds: Set<string>;
}

export class SimpleLogger {
  constructor(private config: SimpleLoggerConfig) {}

  log(id: string, data: Record<string, any>): void {
    if (!this.config.enabled) return;
    
    if (this.config.deniedIds.has(id)) return;
    
    if (this.config.allowedIds.size > 0 && !this.config.allowedIds.has(id)) return;
    
    console.log(`[${id}]`);
    console.log(util.inspect(data, {
      depth: null,
      colors: true 
    }));
  }
}

let defaultLoggerInstance: SimpleLogger | null = null;

export function getDefaultLogger(): SimpleLogger {
  if (!defaultLoggerInstance) {
    defaultLoggerInstance = new SimpleLogger(DEFAULT_CONFIG);
  }
  return defaultLoggerInstance;
}