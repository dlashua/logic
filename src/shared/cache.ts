import { CacheType, CacheConfig } from './types.ts';
import { SimpleLogger } from './simple-logger.ts';

interface CacheEntry {
  value: any;
  timestamp: number;
  lastAccessed: number;
  type: CacheType;
}

export class BaseCache {
  private cache = new Map<string, CacheEntry>();
  private cleanupTimer: NodeJS.Timeout | null = null;
  private readonly maxSize: number;
  private readonly ttlMs: number;
  private readonly cleanupIntervalMs: number;

  constructor(
    protected config: CacheConfig,
    protected logger: SimpleLogger
  ) {
    this.maxSize = config.maxSize || 10000;
    this.ttlMs = config.ttlMs || 30 * 60 * 1000; // 30 minutes default
    this.cleanupIntervalMs = config.cleanupIntervalMs || 5 * 60 * 1000; // 5 minutes default
    
    // Start cleanup timer
    this.startCleanupTimer();
  }

  private startCleanupTimer(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
    }
    
    this.cleanupTimer = setInterval(() => {
      this.cleanup();
    }, this.cleanupIntervalMs);
  }

  private cleanup(): void {
    const now = Date.now();
    let expired = 0;
    let evicted = 0;
    
    // Remove expired entries
    for (const [key, entry] of this.cache.entries()) {
      if (now - entry.timestamp > this.ttlMs) {
        this.cache.delete(key);
        expired++;
      }
    }
    
    // If still over size limit, remove oldest entries (LRU eviction)
    if (this.cache.size > this.maxSize) {
      const entries = Array.from(this.cache.entries())
        .sort((a, b) => a[1].lastAccessed - b[1].lastAccessed);
      
      const toRemove = this.cache.size - this.maxSize;
      for (let i = 0; i < toRemove; i++) {
        this.cache.delete(entries[i][0]);
        evicted++;
      }
    }
    
    if (expired > 0 || evicted > 0) {
      this.logger.log('CACHE_CLEANUP', `Cleaned ${expired} expired, ${evicted} evicted entries. Size: ${this.cache.size}/${this.maxSize}`);
    }
  }

  get(key: string, type: CacheType): any | null {
    if (type === 'pattern' && !this.config.patternCacheEnabled) {
      return null;
    }
    
    const entry = this.cache.get(key);
    if (entry !== undefined) {
      // Update last accessed time
      entry.lastAccessed = Date.now();
      
      // Check if expired
      if (Date.now() - entry.timestamp > this.ttlMs) {
        this.cache.delete(key);
        this.logger.log(`${type.toUpperCase()}_CACHE_EXPIRED`, `Cache expired for key: ${key}`);
        return null;
      }
      
      this.logger.log(`${type.toUpperCase()}_CACHE_HIT`, `Cache hit for key: ${key}`);
      return entry.value;
    }
    
    return null;
  }

  set(key: string, value: any, type: CacheType): void {
    if (type === 'pattern' && !this.config.patternCacheEnabled) {
      return;
    }
    
    const now = Date.now();
    const entry: CacheEntry = {
      value,
      timestamp: now,
      lastAccessed: now,
      type
    };
    
    this.cache.set(key, entry);
    this.logger.log(`${type.toUpperCase()}_CACHE_SET`, `Cache set for key: ${key}. Size: ${this.cache.size}/${this.maxSize}`);
    
    // If we're over the size limit, trigger immediate cleanup
    if (this.cache.size > this.maxSize * 1.1) { // 10% over limit triggers immediate cleanup
      this.cleanup();
    }
  }

  has(key: string): boolean {
    const entry = this.cache.get(key);
    if (!entry) return false;
    
    // Check if expired
    if (Date.now() - entry.timestamp > this.ttlMs) {
      this.cache.delete(key);
      return false;
    }
    
    return true;
  }

  delete(key: string): boolean {
    return this.cache.delete(key);
  }

  clear(type?: CacheType): void {
    if (!type) {
      this.cache.clear();
      this.logger.log('CACHE_CLEAR', 'All caches cleared');
    } else {
      // Type-specific clearing
      let cleared = 0;
      for (const [key, entry] of this.cache.entries()) {
        if (entry.type === type) {
          this.cache.delete(key);
          cleared++;
        }
      }
      this.logger.log('CACHE_CLEAR', `${type} cache cleared: ${cleared} entries`);
    }
  }

  size(): number {
    return this.cache.size;
  }

  keys(): IterableIterator<string> {
    return this.cache.keys();
  }

  // Get cache statistics
  getStats(): { size: number; maxSize: number; ttlMs: number } {
    return {
      size: this.cache.size,
      maxSize: this.maxSize,
      ttlMs: this.ttlMs
    };
  }

  // Destroy the cache and cleanup timer
  destroy(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    this.cache.clear();
  }
}