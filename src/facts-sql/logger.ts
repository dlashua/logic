import { LogConfig } from './types.ts';

export class Logger {
  constructor(private config: LogConfig) {}

  log(id: string, ...args: Record<string, unknown>[]): void {
    if (!this.config.enabled) return;
    if (this.config.ignoredIds.has(id) && !this.config.criticalIds.has(id)) return;

    if (args.length === 0) {
      console.dir({
        log: id 
      }, {
        depth: null 
      });
    } else if (args.length === 1) {
      console.dir({
        log: id,
        ...args[0] 
      }, {
        depth: null 
      });
    } else {
      console.dir({
        log: id,
        args 
      }, {
        depth: null 
      });
    }
  }

  logError(id: string, error: Error, context?: Record<string, unknown>): void {
    console.error({
      log: id,
      error: error.message,
      stack: error.stack,
      ...context
    });
  }

  logQuery(sql: string, rows: any[]): void {
    this.log("DB_QUERY", {
      sql,
      rows 
    });
  }

  logCacheHit(type: string, description: string): void {
    this.log("CACHE_HIT", {
      type,
      description 
    });
  }
}