import { Term, Subst, walk } from "../core.ts";
import { Logger } from "../shared/logger.ts";
import { QueryCache } from "./cache.ts";
import { PatternManager } from "./pattern-manager.ts";
import { QueryBuilder } from "./query-builder.ts";
import { queryUtils, unificationUtils } from "./utils.ts";
import { 
  Pattern, 
  GoalFunction, 
  RelationOptions, 
  FullScanCache, 
  CacheEntry 
} from "./types.ts";

export class RegularRelation {
  private fullScanCache: FullScanCache = {};
  private fullScanKeys: Set<string>;
  private cacheTTL: number;

  constructor(
    private table: string,
    private patternManager: PatternManager,
    private logger: Logger,
    private cache: QueryCache,
    private queryBuilder: QueryBuilder,
    private queries: string[],
    private realQueries: string[],
    options?: RelationOptions,
  ) {
    this.fullScanKeys = new Set(options?.fullScanKeys || []);
    this.cacheTTL = options?.cacheTTL ?? 10000; // Default 3 seconds
  }

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
      const rows = this.cache.processCachedPatternResult(matchingPattern, pattern);
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
    
    // Check if any where clause uses a fullScanKey
    const fullScanKey = whereClauses.find(clause => this.fullScanKeys.has(clause.column));
    
    if (fullScanKey) {
      return await this.executeFullScanQuery(pattern, queryObj, s, fullScanKey, selectColsKeys);
    }
    
    // Regular query execution
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

  private async executeFullScanQuery(
    pattern: Pattern,
    queryObj: Record<string, Term>,
    s: Subst,
    fullScanKey: { column: string; value: any },
    selectColsKeys: string[]
  ): Promise<{ rows: any[], cacheInfo: any }> {
    const { column, value } = fullScanKey;
    
    // Check if we already have valid (non-expired) full scan results cached
    let allRows = this.getCachedData(column, value);
    
    if (!allRows) {
      // Perform full scan for this key-value pair
      const fullScanQuery = this.queryBuilder.buildSelectQuery(
        this.table, 
        ['*'], // Select all columns
        [{ 
          column, 
          value 
        }]
      );
      
      const { rows, sql } = await this.queryBuilder.executeQuery(fullScanQuery);
      
      // Cache the full scan results using TTL-aware method
      this.setCachedData(column, value, rows);
      allRows = rows;
      
      this.logger.log("FULL_SCAN_EXECUTED", `Full scan executed for ${column}=${value}`, {
        table: this.table,
        column,
        value,
        rowCount: rows.length
      });
      
      this.queries.push(sql);
      this.realQueries.push(sql);
      (pattern as any).queries.push(sql);
    } else {
      this.logger.log("FULL_SCAN_CACHE_HIT", `Full scan cache hit for ${column}=${value}`, {
        table: this.table,
        column,
        value,
        rowCount: allRows.length
      });
    }
    
    // Filter rows based on other where clauses (if any)
    const { whereClauses } = await queryUtils.buildQueryParts(queryObj, s);
    const otherWhereClauses = whereClauses.filter(clause => clause.column !== column);
    
    let filteredRows = allRows;
    if (otherWhereClauses.length > 0) {
      filteredRows = allRows.filter(row => {
        return otherWhereClauses.every(clause => row[clause.column] === clause.value);
      });
    }
    
    // Project only the requested columns if not selecting all
    let resultRows = filteredRows;
    if (selectColsKeys.length > 0 && !selectColsKeys.includes('*')) {
      resultRows = filteredRows.map(row => {
        const projectedRow: any = {};
        for (const key of selectColsKeys) {
          projectedRow[key] = row[key];
        }
        return projectedRow;
      });
    }
    
    // Mark pattern as ran and store results for future cache hits
    (pattern as any).ran = true;
    (pattern as any).rows = resultRows;
    this.patternManager.markPatternAsRan(pattern);
    
    // Create additional patterns for cache hits
    this.createFullScanCachePatterns(column, value, allRows);
    
    return {
      rows: resultRows,
      cacheInfo: {
        type: 'fullScan',
        column,
        value,
        totalRows: allRows.length,
        filteredRows: resultRows.length
      }
    };
  }
  
  private createFullScanCachePatterns(column: string, value: any, allRows: any[]): void {
    // Create a "master" pattern for this fullScan key-value pair
    // This pattern will be used to match future queries that have the same where clause
    const masterPattern: Pattern = {
      table: this.table,
      goalIds: [-1], // Special goal ID for fullScan patterns
      rows: allRows,
      ran: true,
      selectCols: {}, // Empty - this pattern can match any select columns
      whereCols: { 
        [column]: value 
      },
      queries: [`FULL_SCAN:${this.table}:${column}=${value}`],
      last: {
        selectCols: [],
        whereCols: []
      }
    };
    
    // Add this pattern to the pattern manager so future queries can match against it
    this.patternManager.addPattern(masterPattern);
    
    this.logger.log("FULL_SCAN_PATTERNS_CREATED", `Master pattern created for future cache hits`, {
      table: this.table,
      column,
      value,
      rowCount: allRows.length,
      patternId: masterPattern.goalIds[0]
    });
  }

  private isCacheEntryExpired(entry: CacheEntry): boolean {
    return Date.now() - entry.timestamp > this.cacheTTL;
  }

  private getCachedData(column: string, value: any): any[] | null {
    const entry = this.fullScanCache[column]?.[value];
    if (!entry) {
      return null;
    }
    
    if (this.isCacheEntryExpired(entry)) {
      this.logger.log("FULL_SCAN_CACHE_EXPIRED", `Cache entry expired for ${column}=${value}`, {
        table: this.table,
        column,
        value,
        age: Date.now() - entry.timestamp,
        ttl: this.cacheTTL
      });
      
      // Clean up expired entry
      delete this.fullScanCache[column][value];
      return null;
    }
    
    return entry.data;
  }

  private setCachedData(column: string, value: any, data: any[]): void {
    if (!this.fullScanCache[column]) {
      this.fullScanCache[column] = {};
    }
    
    this.fullScanCache[column][value] = {
      data,
      timestamp: Date.now()
    };
    
    this.logger.log("FULL_SCAN_CACHE_SET", `Cache entry created for ${column}=${value}`, {
      table: this.table,
      column,
      value,
      rowCount: data.length,
      ttl: this.cacheTTL
    });
  }
}