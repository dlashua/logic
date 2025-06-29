import { Term, Subst, walk } from "../core.ts";
import { Logger } from "./logger.ts";
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
    private cacheQueries: string[]
  ) {}

  createGoal(queryObj: Record<string, Term>): GoalFunction {
    const goalId = this.patternManager.generateGoalId();
    
    // Create and add pattern
    const pattern = this.patternManager.createPattern(this.table, queryObj, goalId);
    this.patternManager.addPattern(pattern);

    return async function* factsSql(this: RegularRelation, s: Subst) {
      const patterns = this.patternManager.getPatternsForGoal(goalId);
      if (patterns.length === 0) {
        console.log("NO PATTERNS");
        return;
      }

      // Walk queryObj terms and merge patterns
      const walkedQ = await queryUtils.walkAllKeys(queryObj, s);
      await this.patternManager.mergePatterns(queryObj, walkedQ, goalId);

      this.logger.log("PATTERNS BEFORE", {
        patterns: this.patternManager.getAllPatterns() 
      });

      for (const pattern of this.patternManager.getPatternsForGoal(goalId)) {
        let s2 = s;
        for await (const s3 of this.runPattern(s2, queryObj, pattern, walkedQ)) {
          yield s3;
          if(s3) s2 = s3
        }
      }

      this.logger.log("PATTERNS AFTER", {
        patterns: this.patternManager.getAllPatterns() 
      });
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

    this.logger.log("RUN_START", {
      pattern,
      queryObj,
      walkedQ 
    });

    const { rows, cacheInfo } = await this.getPatternRows(pattern, queryObj, s, walkedQ);
    
    this.patternManager.updatePatternRows(pattern, rows, pattern.selectCols);

    // Process rows and yield substitutions
    for (const row of pattern.rows) {
      if (row === false) {
        continue;
      } else if (row === true) {
        // Confirmation query: unify queryObj with whereCols
        const unifiedSubst = await unificationUtils.unifyRowWithWalkedQ(
          Object.keys(pattern.whereCols),
          pattern.whereCols,
          queryObj,
          s
        );
        if (unifiedSubst) {
          yield unifiedSubst;
        }
      } else {
        // Regular row processing
        const unifiedSubst = await unificationUtils.unifyRowWithWalkedQ(
          Object.keys(pattern.selectCols),
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

    this.logger.log("RUN_END", {
      pattern 
    });
  }

  private async getPatternRows(
    pattern: Pattern,
    queryObj: Record<string, Term>,
    s: Subst,
    walkedQ: Record<string, Term>
  ): Promise<{ rows: any[], cacheInfo: any }> {
    // Try pattern cache first
    if (pattern.ran) {
      this.logger.log("PATTERN_CACHE_HIT", {
        pattern,
        rows: pattern.rows 
      });
      return {
        rows: pattern.rows,
        cacheInfo: {
          type: 'pattern' 
        }
      };
    }

    // Check for matching patterns
    const matchingPattern = this.cache.findMatchingPattern(
      this.patternManager.getAllPatterns(),
      pattern
    );
    
    if (matchingPattern) {
      const rows = this.cache.processCachedPatternResult(matchingPattern);
      this.logger.log("PATTERN_CACHE_HIT", {
        pattern,
        matchingPattern,
        rows,
      });
      return {
        rows,
        cacheInfo: {
          type: 'pattern',
          matchingGoals: matchingPattern.goalIds 
        }
      };
    }

    // Try row cache for fully grounded queries
    if (queryUtils.allParamsGrounded(walkedQ)) {
      const rowKey = queryUtils.buildRowCacheKey(this.table, walkedQ);
      const cachedRow = this.cache.get(rowKey, 'row');
      if (cachedRow) {
        this.logger.log("ROW_CACHE_HIT", {
          rowKey,
          rows: [cachedRow]
        });
        return {
          rows: [cachedRow],
          cacheInfo: {
            type: 'row',
            key: rowKey 
          }
        };
      }
    }

    // Try query cache
    const { whereClauses } = await queryUtils.buildQueryParts(queryObj, s);
    const cacheKey = queryUtils.buildCacheKey(this.table, Object.keys(pattern.selectCols), whereClauses);
    const cachedResult = this.cache.get(cacheKey, 'query');
    
    if (cachedResult) {
      this.logger.log("QUERY_CACHE_HIT", {
        cacheKey,
        rows: cachedResult 
      });
      return {
        rows: cachedResult,
        cacheInfo: {
          type: 'query',
          key: cacheKey 
        }
      };
    }

    // Execute database query
    return await this.executeQuery(pattern, whereClauses, walkedQ, cacheKey);
  }

  private async executeQuery(
    pattern: Pattern,
    whereClauses: any[],
    walkedQ: Record<string, Term>,
    cacheKey: string
  ): Promise<{ rows: any[], cacheInfo: any }> {
    let query;
    
    if (Object.keys(pattern.selectCols).length === 0) {
      // Confirmation query
      query = this.queryBuilder.buildConfirmationQuery(this.table, whereClauses);
    } else {
      // Regular select query
      query = this.queryBuilder.buildSelectQuery(
        this.table,
        Object.keys(pattern.selectCols),
        whereClauses
      );
    }

    const { rows, sql } = await this.queryBuilder.executeQuery(query);
    (pattern as any).ran = true;

    // Cache the result
    this.cache.set(cacheKey, rows, 'query');

    // If fully grounded and single row, cache in row cache
    if (queryUtils.allParamsGrounded(walkedQ) && rows.length === 1) {
      const rowKey = queryUtils.buildRowCacheKey(this.table, walkedQ);
      this.cache.set(rowKey, rows[0], 'row');
    }

    // Log query
    this.logger.logQuery(sql, rows);
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