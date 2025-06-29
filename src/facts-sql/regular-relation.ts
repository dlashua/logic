import { Term, Subst, walk } from "../core.ts";
import { Logger } from "../shared/logger.ts";
import { QueryCache } from "./cache.ts";
import { PatternManager } from "./pattern-manager.ts";
import { QueryBuilder } from "./query-builder.ts";
import { queryUtils, unificationUtils } from "./utils.ts";
import { Pattern, GoalFunction } from "./types.ts";

export class RegularRelation {
  constructor(
    private table: string,
    private patternManager: PatternManager,
    private logger: Logger,
    private cache: QueryCache,
    private queryBuilder: QueryBuilder,
    private queries: string[],
    private realQueries: string[],
  ) {}

  createGoal(queryObj: Record<string, Term>): GoalFunction {
    const goalId = this.patternManager.generateGoalId();
    
    // Create and add pattern
    const pattern = this.patternManager.createPattern(this.table, queryObj, goalId);
    this.patternManager.addPattern(pattern);

    return async function* factsSql(this: RegularRelation, s: Subst) {
      const patterns = this.patternManager.getPatternsForGoal(goalId);
      if (patterns.length === 0) {
        return;
      }

      // Walk queryObj terms
      const walkedQ = await queryUtils.walkAllKeys(queryObj, s);
      await this.patternManager.mergePatterns(queryObj, walkedQ, goalId);

      for (const pattern of this.patternManager.getPatternsForGoal(goalId)) {
        let s2 = s;
        for await (const s3 of this.runPattern(s2, queryObj, pattern, walkedQ)) {
          yield s3;
          if(s3) s2 = s3;
        }
      }

      // Optimized diagnostics (no setTimeout)
      this.patternManager.logFinalDiagnostics(goalId);
    }.bind(this);
  }

  private async* runPattern(
    s: Subst,
    queryObj: Record<string, Term>,
    pattern: Pattern,
    walkedQ: Record<string, Term>
  ): AsyncGenerator<Subst, void, unknown> {
    if (pattern.ran && pattern.rows.length === 0) {
      return;
    }

    const { rows, cacheInfo } = await this.getPatternRows(pattern, queryObj, s, walkedQ);
    
    this.patternManager.updatePatternRows(pattern, rows, pattern.selectCols);

    // Optimized row processing with better loop structure
    const patternRows = pattern.rows;
    const patternRowsLength = patternRows.length;
    
    for (let i = 0; i < patternRowsLength; i++) {
      const row = patternRows[i];
      
      if (row === false) {
        continue;
      } else if (row === true) {
        // Confirmation query: unify queryObj with whereCols
        const whereColsKeys = Object.keys(pattern.whereCols);
        const unifiedSubst = await unificationUtils.unifyRowWithWalkedQ(
          whereColsKeys,
          pattern.whereCols,
          queryObj,
          s
        );
        if (unifiedSubst) {
          yield unifiedSubst;
        }
      } else {
        // Regular row processing
        const selectColsKeys = Object.keys(pattern.selectCols);
        const unifiedSubst = await unificationUtils.unifyRowWithWalkedQ(
          selectColsKeys,
          walkedQ,
          row,
          s
        );
        if (unifiedSubst) {
          // Update patterns with newly grounded terms
          const updatedWalkedQ = await queryUtils.walkAllKeys(queryObj, unifiedSubst);
          await this.patternManager.mergePatterns(queryObj, updatedWalkedQ, pattern.goalIds[0]);
          yield unifiedSubst;
        }
      }
    }
  }

  private async getPatternRows(
    pattern: Pattern,
    queryObj: Record<string, Term>,
    s: Subst,
    walkedQ: Record<string, Term>
  ): Promise<{ rows: any[], cacheInfo: any }> {
    // Try pattern cache first
    if (pattern.ran) {
      return {
        rows: pattern.rows,
        cacheInfo: {
          type: 'pattern' 
        }
      };
    }

    // Check for matching patterns using optimized comparison
    const matchingPattern = this.cache.findMatchingPattern(
      this.patternManager.getAllPatterns(),
      pattern
    );
    
    if (matchingPattern) {
      const rows = this.cache.processCachedPatternResult(matchingPattern);
      return {
        rows,
        cacheInfo: {
          type: 'pattern',
          matchingGoals: matchingPattern.goalIds 
        }
      };
    }

    // Execute database query
    return await this.executeQuery(pattern, queryObj, s);
  }

  private async executeQuery(
    pattern: Pattern,
    queryObj: Record<string, Term>,
    s: Subst
  ): Promise<{ rows: any[], cacheInfo: any }> {
    // Build query parts once
    const { whereClauses } = await queryUtils.buildQueryParts(queryObj, s);
    const selectColsKeys = Object.keys(pattern.selectCols);
    
    let query;
    if (selectColsKeys.length === 0) {
      // Confirmation query
      query = this.queryBuilder.buildConfirmationQuery(this.table, whereClauses);
    } else {
      // Regular select query
      query = this.queryBuilder.buildSelectQuery(this.table, selectColsKeys, whereClauses);
    }

    const { rows, sql } = await this.queryBuilder.executeQuery(query);
    
    // Mark pattern as ran and update indexes
    (pattern as any).ran = true;
    this.patternManager.markPatternAsRan(pattern);

    // Minimal logging
    if (this.logger) {
      this.logger.logQuery(sql, rows);
    }
    this.queries.push(sql);
    this.realQueries.push(sql);
    (pattern as any).queries.push(sql);

    return {
      rows,
      cacheInfo: {
        type: 'database',
        sql 
      }
    };
  }
}