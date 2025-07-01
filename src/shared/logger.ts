import { LogConfig } from './types.ts';

export class Logger {
  constructor(private config: LogConfig) {}

  log(id: string, message: string, data?: any): void {
    if (!this.config.enabled) return;
    
    if (this.config.ignoredIds.has(id)) return;
    
    const timestamp = new Date().toISOString();
    const prefix = this.config.criticalIds.has(id) ? '[CRITICAL]' : '[INFO]';
    
    if (data !== undefined) {
      console.log(`${timestamp} ${prefix} [${id}] ${message}`);
      console.dir(data, {
        depth: null 
      });
    } else {
      console.log(`${timestamp} ${prefix} [${id}] ${message}`);
    }
  }

  logCritical(id: string, message: string, data?: any): void {
    if (!this.config.enabled) return;
    
    const timestamp = new Date().toISOString();
    console.log(`${timestamp} [CRITICAL] [${id}] ${message}`, data);
  }

  logPerformance(id: string, operation: string, duration: number, details?: any): void {
    if (!this.config.enabled) return;
    if (this.config.ignoredIds.has(id)) return;
    
    const timestamp = new Date().toISOString();
    const message = `${operation} took ${duration}ms`;
    
    if (details) {
      console.log(`${timestamp} [PERF] [${id}] ${message}`, details);
    } else {
      console.log(`${timestamp} [PERF] [${id}] ${message}`);
    }
  }

  logError(id: string, error: Error, context?: Record<string, unknown>): void {
    const timestamp = new Date().toISOString();
    console.error(`${timestamp} [ERROR] [${id}]`, error.message, context);
  }

  logQuery(sql: string, rows: any[]): void {
    this.log("DB_QUERY", `Query executed: ${sql}`, {
      rowCount: rows.length 
    });
  }

  logCacheHit(type: string, description: string): void {
    this.log("CACHE_HIT", `${type}: ${description}`);
  }
}