import { Term, isVar } from '../core.ts';
import { Logger } from '../shared/logger.ts';
import { Pattern, SymmetricPattern } from './types.ts';
import { patternUtils, queryUtils } from './utils.ts';

export class PatternManager {
  private patterns: Pattern[] = [];
  private nextGoalId = 1;
  
  // Performance optimization: Multiple indexes
  private patternsByGoal = new Map<number, Pattern[]>();
  private patternsByTable = new Map<string, Pattern[]>();
  private unranPatterns = new Set<Pattern>();
  
  // Cache for expensive operations
  private selectColsKeyCache = new Map<Pattern, string>();

  constructor(private logger: Logger) {}

  generateGoalId(): number {
    return this.nextGoalId++;
  }

  addPattern(pattern: Pattern): void {
    this.patterns.push(pattern);
    this.unranPatterns.add(pattern);
    
    // Update goal index
    for (const goalId of pattern.goalIds) {
      if (!this.patternsByGoal.has(goalId)) {
        this.patternsByGoal.set(goalId, []);
      }
      this.patternsByGoal.get(goalId)!.push(pattern);
    }
    
    // Update table index
    if (!this.patternsByTable.has(pattern.table)) {
      this.patternsByTable.set(pattern.table, []);
    }
    this.patternsByTable.get(pattern.table)!.push(pattern);
  }

  getPatternsForGoal(goalId: number): Pattern[] {
    return this.patternsByGoal.get(goalId) || [];
  }

  getAllPatterns(): Pattern[] {
    return this.patterns;
  }

  markPatternAsRan(pattern: Pattern): void {
    this.unranPatterns.delete(pattern);
  }

  createPattern(
    table: string,
    queryObj: Record<string, Term>,
    goalId: number
  ): Pattern {
    const { selectCols, whereCols } = patternUtils.separateQueryColumns(queryObj);

    return {
      table,
      selectCols,
      whereCols,
      goalIds: [goalId],
      rows: [],
      ran: false,
      last: {
        selectCols: [],
        whereCols: [],
      },
      queries: [],
    };
  }

  // Optimized: Cache selectCols keys to avoid repeated Object.keys() calls
  private getSelectColsKey(pattern: Pattern): string {
    if (!this.selectColsKeyCache.has(pattern)) {
      const key = Object.keys(pattern.selectCols).sort().join('|');
      this.selectColsKeyCache.set(pattern, key);
    }
    return this.selectColsKeyCache.get(pattern)!;
  }

  async mergePatterns(
    queryObj: Record<string, Term>,
    walkedQ: Record<string, Term>,
    goalId: number
  ): Promise<void> {
    const patternsForGoal = this.getPatternsForGoal(goalId);
    if (patternsForGoal.length <= 1) return; // Nothing to merge

    // Group patterns by selectCols key for O(n) merging instead of O(n²)
    const selectColsGroups = new Map<string, Pattern[]>();
    
    for (const pattern of patternsForGoal) {
      if (pattern.ran) continue;
      
      const selectColsKey = this.getSelectColsKey(pattern);
      if (!selectColsGroups.has(selectColsKey)) {
        selectColsGroups.set(selectColsKey, []);
      }
      selectColsGroups.get(selectColsKey)!.push(pattern);
    }

    // Merge within groups - now O(k) where k is group size
    for (const group of selectColsGroups.values()) {
      if (group.length > 1) {
        this.mergePatternGroup(group);
      }
    }
  }

  private mergePatternGroup(patterns: Pattern[]): void {
    const basePattern = patterns[0];
    
    for (let i = 1; i < patterns.length; i++) {
      const pattern = patterns[i];
      
      // Merge selectCols efficiently
      const baseSelectKeys = Object.keys(basePattern.selectCols);
      for (let j = 0; j < baseSelectKeys.length; j++) {
        const key = baseSelectKeys[j];
        if (isVar(basePattern.selectCols[key]) && !isVar(pattern.selectCols[key])) {
          (basePattern.selectCols as any)[key] = pattern.selectCols[key];
        }
      }

      // Merge whereCols efficiently
      const baseWhereKeys = Object.keys(basePattern.whereCols);
      for (let j = 0; j < baseWhereKeys.length; j++) {
        const key = baseWhereKeys[j];
        if (isVar(basePattern.whereCols[key]) && !isVar(pattern.whereCols[key])) {
          (basePattern.whereCols as any)[key] = pattern.whereCols[key];
        }
      }

      // Merge goalIds efficiently
      for (const goalId of pattern.goalIds) {
        if (!basePattern.goalIds.includes(goalId)) {
          (basePattern.goalIds as any).push(goalId);
        }
      }
    }
    
    // Clear cache since pattern was modified
    this.selectColsKeyCache.delete(basePattern);
  }

  updatePatternRows(pattern: Pattern, rows: any[], selectCols: Record<string, Term>): void {
    if (rows.length === 1 && (rows[0] === true || rows[0] === false)) {
      (pattern as any).rows = rows;
    } else {
      if (Object.keys(selectCols).length === 0) {
        (pattern as any).rows = rows.length > 0 ? [true] : [false];
      } else {
        (pattern as any).rows = rows.length > 0 ? rows : [false];
      }
    }
  }

  logFinalDiagnostics(goalId: number): void {
    // Remove setTimeout overhead entirely
    if (goalId === this.nextGoalId - 1) {
      // Only log if explicitly enabled and needed for debugging
      if (this.logger && (this.logger as any).config?.enabled) {
        const ranFalsePatterns = Array.from(this.unranPatterns);
        if (ranFalsePatterns.length > 0) {
          this.logger.log("RAN FALSE PATTERNS", "Pattern diagnostics", {
            ranFalsePatterns 
          });
        }
      }
    }
  }
}

export class SymmetricPatternManager {
  private patterns: SymmetricPattern[] = [];
  private nextGoalId = 1;
  private patternsByGoal = new Map<number, SymmetricPattern[]>();

  constructor(private logger: Logger) {}

  generateGoalId(): number {
    return this.nextGoalId++;
  }

  addPattern(pattern: SymmetricPattern): void {
    this.patterns.push(pattern);
    
    for (const goalId of pattern.goalIds) {
      if (!this.patternsByGoal.has(goalId)) {
        this.patternsByGoal.set(goalId, []);
      }
      this.patternsByGoal.get(goalId)!.push(pattern);
    }
  }

  getPatternsForGoal(goalId: number): SymmetricPattern[] {
    return this.patternsByGoal.get(goalId) || [];
  }

  getAllPatterns(): SymmetricPattern[] {
    return this.patterns;
  }

  createPattern(
    table: string,
    queryObj: Record<string, Term>,
    goalId: number
  ): SymmetricPattern {
    const { selectCols, whereCols } = patternUtils.separateSymmetricColumns(queryObj);

    return {
      table,
      selectCols,
      whereCols,
      goalIds: [goalId],
      rows: [],
      ran: false,
      last: {
        selectCols: [],
        whereCols: [],
      },
      queries: [],
    };
  }

  updatePatternRows(pattern: SymmetricPattern, rows: any[]): void {
    (pattern as any).rows = rows;
  }

  logFinalDiagnostics(goalId: number): void {
    // Remove setTimeout overhead entirely
    return;
  }
}