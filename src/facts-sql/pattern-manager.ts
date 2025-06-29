import { Term, isVar } from '../core.ts';
import { Pattern, SymmetricPattern } from './types.ts';
import { Logger } from './logger.ts';
import { patternUtils, queryUtils } from './utils.ts';

export class PatternManager {
  private patterns: Pattern[] = [];
  private nextGoalId = 1;

  constructor(private logger: Logger) {}

  generateGoalId(): number {
    return this.nextGoalId++;
  }

  addPattern(pattern: Pattern): void {
    this.patterns.push(pattern);
  }

  getPatternsForGoal(goalId: number): Pattern[] {
    return this.patterns.filter(pattern => pattern.goalIds.includes(goalId));
  }

  getAllPatterns(): Pattern[] {
    return [...this.patterns];
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

  async mergePatterns(
    queryObj: Record<string, Term>,
    walkedQ: Record<string, Term>,
    goalId: number
  ): Promise<void> {
    const updatedPatterns: Pattern[] = [];

    for (const pattern of this.patterns) {
      this.logger.log("MERGE_PATTERNS_START", {
        pattern,
        goalId 
      });

      // Skip patterns that do not match the current goalId
      if (!pattern.goalIds.includes(goalId)) {
        this.logger.log("SKIPPED_PATTERN", {
          pattern,
          reason: "GoalId does not match",
          goalId,
        });
        updatedPatterns.push(pattern);
        continue;
      }

      // Skip patterns that have already been run
      if (pattern.ran) {
        this.logger.log("SKIPPED_PATTERN", {
          pattern,
          reason: "Pattern already ran",
        });
        updatedPatterns.push(pattern);
        continue;
      }

      // Find matching patterns with same selectCols
      const matchingPatterns = this.patterns.filter(otherPattern => {
        return (
          otherPattern !== pattern &&
          otherPattern.goalIds.includes(goalId) &&
          JSON.stringify(Object.keys(otherPattern.selectCols).sort()) === 
          JSON.stringify(Object.keys(pattern.selectCols).sort())
        );
      });

      if (matchingPatterns.length > 0) {
        this.logger.log("MERGING_PATTERNS", {
          matchingPatterns 
        });
        this.mergeMatchingPatterns(pattern, matchingPatterns);
      }

      updatedPatterns.push(pattern);
    }

    this.logger.log("MERGE_PATTERNS_END", {
      updatedPatterns 
    });
    this.patterns = updatedPatterns;
  }

  private mergeMatchingPatterns(pattern: Pattern, matchingPatterns: Pattern[]): void {
    for (const match of matchingPatterns) {
      // Merge selectCols
      for (const key of Object.keys(match.selectCols)) {
        if (isVar(pattern.selectCols[key]) && !isVar(match.selectCols[key])) {
          this.logger.log("GROUNDING_SELECT_COL_DURING_MERGE", {
            key,
            currentValue: pattern.selectCols[key],
            newValue: match.selectCols[key],
          });
          (pattern.selectCols as any)[key] = match.selectCols[key];
        }
      }

      // Merge whereCols
      for (const key of Object.keys(match.whereCols)) {
        if (isVar(pattern.whereCols[key]) && !isVar(match.whereCols[key])) {
          this.logger.log("GROUNDING_WHERE_COL_DURING_MERGE", {
            key,
            currentValue: pattern.whereCols[key],
            newValue: match.whereCols[key],
          });
          (pattern.whereCols as any)[key] = match.whereCols[key];
        }
      }

      // Merge goalIds
      match.goalIds.forEach(id => {
        if (!pattern.goalIds.includes(id)) {
          (pattern.goalIds as any).push(id);
        }
      });
    }
  }

  updatePatternRows(pattern: Pattern, rows: any[], selectCols: Record<string, Term>): void {
    if (rows.length === 1 && (rows[0] === true || rows[0] === false)) {
      // Keep boolean results as-is
      (pattern as any).rows = rows;
    } else {
      if (Object.keys(selectCols).length === 0) {
        // Confirmation query
        (pattern as any).rows = rows.length > 0 ? [true] : [false];
      } else {
        (pattern as any).rows = rows.length > 0 ? rows : [false];
      }
    }

    this.logger.log("PATTERN_ROWS_UPDATED", {
      pattern 
    });
  }

  logFinalDiagnostics(goalId: number): void {
    setTimeout(() => {
      if (goalId === this.nextGoalId - 1) {
        this.logger.log("FINAL PATTERNS", {
          patterns: this.patterns,
          goalId 
        });

        const ranFalsePatterns = this.patterns.filter(x => !x.ran);
        if (ranFalsePatterns.length > 0) {
          this.logger.log("RAN FALSE PATTERNS", {
            ranFalsePatterns 
          });
        }

        const selectColsMismatchPatterns = this.patterns.filter(
          x => x.rows.length > 1 && !patternUtils.allSelectColsAreTags(x.selectCols)
        );
        if (selectColsMismatchPatterns.length > 0) {
          this.logger.log("SELECTCOLS MISMATCH PATTERNS", {
            selectColsMismatchPatterns 
          });
        }

        const mergedPatterns = this.patterns.filter(x => x.goalIds.length > 1);
        if (mergedPatterns.length > 0) {
          this.logger.log("MERGED PATTERNS SEEN. GOOD!", {
            mergedPatterns 
          });
        }
      }
    }, 500);
  }
}

export class SymmetricPatternManager {
  private patterns: SymmetricPattern[] = [];
  private nextGoalId = 1;

  constructor(private logger: Logger) {}

  generateGoalId(): number {
    return this.nextGoalId++;
  }

  addPattern(pattern: SymmetricPattern): void {
    this.patterns.push(pattern);
  }

  getPatternsForGoal(goalId: number): SymmetricPattern[] {
    return this.patterns.filter(pattern => pattern.goalIds.includes(goalId));
  }

  getAllPatterns(): SymmetricPattern[] {
    return [...this.patterns];
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
    this.logger.log("PATTERN_ROWS_UPDATED", {
      pattern 
    });
  }

  logFinalDiagnostics(goalId: number): void {
    setTimeout(() => {
      if (goalId === this.nextGoalId - 1) {
        this.logger.log("FINAL PATTERNS SYM", {
          patterns: this.patterns,
          goalId 
        });
      }
    }, 500);
  }
}