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
      }
    }
    
    return null;
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