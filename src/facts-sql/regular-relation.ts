import type { Term, Subst, Goal } from "../core/types.ts";
import { walk, isVar } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";
import { QueryCache } from "./cache.ts";
import { QueryBuilder } from "./query-builder.ts";
import { queryUtils, unificationUtils, patternUtils } from "./utils.ts";
import { Pattern, RelationOptions, FullScanCache, CacheEntry } from "./types.ts"

export class RegularRelation {
  private fullScanCache: FullScanCache = {};
  private fullScanKeys: Set<string>;
  private cacheTTL: number;

  // Minimal pattern management for full scan cache
  private nextGoalId = 1;
  private patternsByGoal = new Map<number, Pattern[]>();
  
  // New substitution-aware query cache
  private queryCache = new Map<string, { rows: any[], timestamp: number }>();

  constructor(
    private table: string,
    private logger: Logger,
    private cache: QueryCache,
    private queryBuilder: QueryBuilder,
    private queries: string[],
    private realQueries: string[],
    options?: RelationOptions,
  ) {
    this.fullScanKeys = new Set(options?.fullScanKeys || []);
    this.cacheTTL = options?.cacheTTL ?? 1000; // Default 3 seconds
  }

  createGoal(queryObj: Record<string, Term>): Goal {
    const goalId = this.generateGoalId();
    
    // Create and add pattern
    const pattern = this.createPattern(this.table, queryObj, goalId);
    this.addPattern(pattern);

    return async function* factsSql(this: RegularRelation, s: Subst) {
      const patterns = this.getPatternsForGoal(goalId);
      if (patterns.length === 0) {
        return;
      }

      // Walk queryObj terms
      const walkedQ = await queryUtils.walkAllKeys(queryObj, s);
      


      for (const pattern of this.getPatternsForGoal(goalId)) {
        for await (const s3 of this.runPattern(s, queryObj, pattern, walkedQ)) {
          yield s3;
        }
      }

      // Optimized diagnostics (no setTimeout)
      this.logFinalDiagnostics(goalId);
    }.bind(this);
  }

  private async* runPattern(
    s: Subst,
    queryObj: Record<string, Term>,
    pattern: Pattern,
    walkedQ: Record<string, Term>
  ): AsyncGenerator<Subst, void, unknown> {

    const { rows, cacheInfo } = await this.getPatternRows(pattern, queryObj, s, walkedQ);
    
    this.updatePatternRows(pattern, rows, pattern.selectCols);

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
    // Check new substitution-aware cache first
    const cacheKey = this.createQueryCacheKey(queryObj, walkedQ);
    const cachedRows = this.getCachedQuery(cacheKey);
    
    if (cachedRows !== null) {
      return {
        rows: cachedRows,
        cacheInfo: {
          type: 'substitution-aware',
          cacheKey
        }
      };
    }

    // Execute database query
    const result = await this.executeQuery(pattern, queryObj, s);
    
    // Cache the results with the substitution-aware key
    this.setCachedQuery(cacheKey, result.rows);
    
    return result;
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
    
    // Mark pattern as ran  
    (pattern as any).ran = true;

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
    this.addPattern(masterPattern);
    
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

  // Pattern management methods (moved from PatternManager)
  private generateGoalId(): number {
    return this.nextGoalId++;
  }

  private addPattern(pattern: Pattern): void {
    // Minimal pattern tracking for goal execution
    for (const goalId of pattern.goalIds) {
      if (!this.patternsByGoal.has(goalId)) {
        this.patternsByGoal.set(goalId, []);
      }
      this.patternsByGoal.get(goalId)!.push(pattern);
    }
  }

  private getPatternsForGoal(goalId: number): Pattern[] {
    return this.patternsByGoal.get(goalId) || [];
  }

  private createPattern(
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
      timestamp: Date.now(),
      last: {
        selectCols: [],
        whereCols: [],
      },
      queries: [],
    };
  }


  private updatePatternRows(pattern: Pattern, rows: any[], selectCols: Record<string, Term>): void {
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

  private logFinalDiagnostics(_goalId: number): void {
    // Simplified diagnostics - pattern tracking removed
    return;
  }

  // New substitution-aware caching methods
  private createQueryCacheKey(
    queryObj: Record<string, Term>, 
    walkedQ: Record<string, Term>
  ): string {
    // Create a cache key based on the table and resolved query parameters
    const resolvedParams: Record<string, any> = {};
    
    for (const [key, value] of Object.entries(walkedQ)) {
      // Only include non-variable values in the cache key
      if (!isVar(value)) {
        resolvedParams[key] = value;
      }
    }
    
    // Sort keys for consistent cache keys
    const sortedKeys = Object.keys(resolvedParams).sort();
    const keyParts = sortedKeys.map(key => `${key}:${resolvedParams[key]}`);
    
    return `${this.table}:${keyParts.join(',')}`;
  }

  private getCachedQuery(cacheKey: string): any[] | null {
    const cached = this.queryCache.get(cacheKey);
    
    if (!cached) {
      return null;
    }
    
    // Check if cache entry is expired
    if (Date.now() - cached.timestamp > this.cacheTTL) {
      this.queryCache.delete(cacheKey);
      return null;
    }
    
    return cached.rows;
  }

  private setCachedQuery(cacheKey: string, rows: any[]): void {
    this.queryCache.set(cacheKey, {
      rows,
      timestamp: Date.now()
    });
  }
}