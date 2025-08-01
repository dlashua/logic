import type { Subst } from "@swiftfall/logic";
import type { CacheEntry, CacheManager, DataRow } from "./types.js";

const ROW_CACHE = Symbol.for("abstract-row-cache");

/**
 * Default cache manager that stores cache entries in substitution objects
 * This matches the current SQL implementation behavior
 */
export class DefaultCacheManager implements CacheManager {
  /**
   * Get cached rows for a goal from a substitution
   */
  get(goalId: number, subst: Subst): DataRow[] | null {
    const cache = this.getOrCreateRowCache(subst);
    if (cache.has(goalId)) {
      const entry = cache.get(goalId)!;
      return entry.data;
    }
    return null;
  }

  /**
   * Set cached rows for a goal in a substitution
   */
  set(
    goalId: number,
    subst: Subst,
    rows: DataRow[],
    meta?: Record<string, any>,
  ): void {
    const cache = this.getOrCreateRowCache(subst);
    cache.set(goalId, {
      data: rows,
      timestamp: Date.now(),
      goalId,
      meta,
    });
  }

  /**
   * Clear cache entries
   */
  clear(goalId?: number): void {
    // Note: This implementation clears from a specific substitution
    // For global clearing, you'd need to track all substitutions
    if (goalId !== undefined) {
      // Clear specific goal - would need access to all active substitutions
      // This is a limitation of the current design
    }
    // For now, we rely on substitution-local clearing
  }

  /**
   * Check if cache entry exists
   */
  has(goalId: number, subst: Subst): boolean {
    const cache = this.getOrCreateRowCache(subst);
    return cache.has(goalId);
  }

  /**
   * Remove cache entry for a specific goal from a substitution
   */
  delete(goalId: number, subst: Subst): void {
    const cache = this.getOrCreateRowCache(subst);
    cache.delete(goalId);
  }

  /**
   * Get or create the cache map from a substitution
   */
  private getOrCreateRowCache(subst: Subst): Map<number, CacheEntry> {
    if (!subst.has(ROW_CACHE)) {
      subst.set(ROW_CACHE, new Map<number, CacheEntry>());
    }
    return subst.get(ROW_CACHE) as Map<number, CacheEntry>;
  }

  /**
   * Format cache for logging (matches current implementation)
   */
  formatCacheForLog(subst: Subst): Record<number, any> {
    const result: Record<number, any> = {};
    const cache = subst.get(ROW_CACHE);
    if (!(cache instanceof Map)) return result;

    for (const [goalId, entry] of cache.entries()) {
      if (Array.isArray(entry.data)) {
        if (entry.data.length <= 5) {
          result[goalId] = entry.data;
        } else {
          result[goalId] = {
            count: entry.data.length,
            timestamp: entry.timestamp,
          };
        }
      }
    }
    return result;
  }
}
