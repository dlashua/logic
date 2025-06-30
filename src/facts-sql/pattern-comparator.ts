import { Term } from '../core.ts';
import { Pattern, SymmetricPattern } from './types.ts';

export class PatternComparator {
  // Cache for pattern hash keys to avoid recomputation
  private patternHashCache = new Map<Pattern | SymmetricPattern, string>();

  /**
   * Fast comparison of pattern whereCols without JSON.stringify
   */
  private fastWhereColsEqual(a: Record<string, Term> | Term[], b: Record<string, Term> | Term[]): boolean {
    // Handle symmetric pattern case (Term[])
    if (Array.isArray(a) && Array.isArray(b)) {
      if (a.length !== b.length) return false;
      for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
      }
      return true;
    }
    
    // Handle regular pattern case (Record<string, Term>)
    if (!Array.isArray(a) && !Array.isArray(b)) {
      const keysA = Object.keys(a);
      const keysB = Object.keys(b);
      
      if (keysA.length !== keysB.length) return false;
      
      for (let i = 0; i < keysA.length; i++) {
        const key = keysA[i];
        if (!(key in b) || a[key] !== b[key]) return false;
      }
      return true;
    }
    
    return false;
  }

  /**
   * Generate a fast hash key for pattern comparison
   */
  private getPatternHash(pattern: Pattern | SymmetricPattern): string {
    if (this.patternHashCache.has(pattern)) {
      return this.patternHashCache.get(pattern)!;
    }

    let hash: string;
    if (Array.isArray(pattern.whereCols)) {
      // SymmetricPattern case
      hash = pattern.whereCols.map((term, idx) => `${idx}:${term}`).join('|');
    } else {
      // Pattern case - create sorted key to ensure consistency
      const sortedKeys = Object.keys(pattern.whereCols).sort();
      // @ts-expect-error
      hash = sortedKeys.map(k => `${k}:${pattern.whereCols[k]}`).join('|');
    }
    
    this.patternHashCache.set(pattern, hash);
    return hash;
  }

  /**
   * Fast pattern matching for regular patterns
   */
  findMatchingPattern(patterns: Pattern[], currentPattern: Pattern): Pattern | null {
    const currentHash = this.getPatternHash(currentPattern);
    
    for (let i = 0; i < patterns.length; i++) {
      const otherPattern = patterns[i];
      if (otherPattern !== currentPattern && otherPattern.ran) {
        const otherHash = this.getPatternHash(otherPattern);
        if (currentHash === otherHash) {
          return otherPattern;
        }
        
        // Check if otherPattern can satisfy currentPattern via subset matching
        if (this.canPatternSatisfy(otherPattern, currentPattern)) {
          return otherPattern;
        }
      }
    }
    
    return null;
  }

  /**
   * Check if an existing pattern can satisfy a new pattern's requirements
   * This handles cases where a broader query (fewer where conditions) can answer
   * a more specific query (more where conditions)
   */
  private canPatternSatisfy(existingPattern: Pattern, newPattern: Pattern): boolean {
    // Only handle regular patterns (not symmetric)
    if (Array.isArray(existingPattern.whereCols) || Array.isArray(newPattern.whereCols)) {
      return false;
    }
    
    const existingWhere = existingPattern.whereCols as Record<string, Term>;
    const newWhere = newPattern.whereCols as Record<string, Term>;
    
    // Check if existingPattern's where conditions are a subset of newPattern's where conditions
    // and all matching keys have the same values
    const existingKeys = Object.keys(existingWhere);
    const newKeys = Object.keys(newWhere);
    
    // existingPattern must have fewer or equal where conditions
    if (existingKeys.length > newKeys.length) {
      return false;
    }
    
    // All of existingPattern's where conditions must be present in newPattern with same values
    for (const key of existingKeys) {
      if (!(key in newWhere) || existingWhere[key] !== newWhere[key]) {
        return false;
      }
    }
    
    // For this to be a cache hit, we need to verify the existingPattern's results
    // can answer the newPattern's query by filtering the cached results
    return true;
  }

  /**
   * Fast pattern matching for symmetric patterns
   */
  findMatchingSymmetricPattern(patterns: SymmetricPattern[], currentPattern: SymmetricPattern): SymmetricPattern | null {
    const currentHash = this.getPatternHash(currentPattern);
    
    for (let i = 0; i < patterns.length; i++) {
      const otherPattern = patterns[i];
      if (otherPattern !== currentPattern && otherPattern.ran) {
        const otherHash = this.getPatternHash(otherPattern);
        if (currentHash === otherHash) {
          return otherPattern;
        }
      }
    }
    
    return null;
  }

  /**
   * Clear the hash cache (call when patterns are modified)
   */
  clearCache(): void {
    this.patternHashCache.clear();
  }
}