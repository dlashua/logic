import { CacheType, CacheConfig } from './types.ts';
import { Logger } from './logger.ts';

export class BaseCache {
  private cache = new Map<string, any>();

  constructor(
    protected config: CacheConfig,
    protected logger: Logger
  ) {}

  get(key: string, type: CacheType): any | null {
    if (type === 'pattern' && !this.config.patternCacheEnabled) {
      return null;
    }
    
    const value = this.cache.get(key);
    if (value !== undefined) {
      this.logger.log(`${type.toUpperCase()}_CACHE_HIT`, `Cache hit for key: ${key}`);
      return value;
    }
    
    return null;
  }

  set(key: string, value: any, type: CacheType): void {
    if (type === 'pattern' && !this.config.patternCacheEnabled) {
      return;
    }
    
    this.cache.set(key, value);
    this.logger.log(`${type.toUpperCase()}_CACHE_SET`, `Cache set for key: ${key}`);
  }

  has(key: string): boolean {
    return this.cache.has(key);
  }

  delete(key: string): boolean {
    return this.cache.delete(key);
  }

  clear(type?: CacheType): void {
    if (!type) {
      this.cache.clear();
      this.logger.log('CACHE_CLEAR', 'All caches cleared');
    } else {
      // For type-specific clearing, we'd need to store type info with keys
      // For now, just clear all
      this.cache.clear();
      this.logger.log('CACHE_CLEAR', `${type} cache cleared`);
    }
  }

  size(): number {
    return this.cache.size;
  }

  keys(): IterableIterator<string> {
    return this.cache.keys();
  }
}