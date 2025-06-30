import { Term, Subst, walk, isVar } from "../core.ts";
import { Logger } from "../shared/logger.ts";
import { QueryCache } from "./cache.ts";
import { QueryBuilder } from "./query-builder.ts";
import { queryUtils, unificationUtils, patternUtils } from "./utils.ts";
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

  // Pattern management fields (moved from PatternManager)
  private patterns: Pattern[] = [];
  private nextGoalId = 1;
  private patternsByGoal = new Map<number, Pattern[]>();
  private patternsByTable = new Map<string, Pattern[]>();
  private unranPatterns = new Set<Pattern>();
  private selectColsKeyCache = new Map<Pattern, string>();

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
    this.cacheTTL = options?.cacheTTL ?? 5000; // Default 3 seconds
  }

  createGoal(queryObj: Record<string, Term>): GoalFunction {
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
      await this.mergePatterns(queryObj, walkedQ, goalId);

      for (const pattern of this.getPatternsForGoal(goalId)) {
        let s2 = s;
        for await (const s3 of this.runPattern(s2, queryObj, pattern, walkedQ)) {
          yield s3;
          if(s3) s2 = s3;
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
    if (pattern.ran && pattern.rows.length === 0) {
      return;
    }

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
          // Update patterns with newly grounded terms
          const updatedWalkedQ = await queryUtils.walkAllKeys(queryObj, unifiedSubst);
          await this.mergePatterns(queryObj, updatedWalkedQ, pattern.goalIds[0]);
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
      this.getAllPatterns(),
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
    this.markPatternAsRan(pattern);

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
    this.markPatternAsRan(pattern);
    
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

  private getPatternsForGoal(goalId: number): Pattern[] {
    return this.patternsByGoal.get(goalId) || [];
  }

  private getAllPatterns(): Pattern[] {
    return this.patterns;
  }

  private markPatternAsRan(pattern: Pattern): void {
    this.unranPatterns.delete(pattern);
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

  private getSelectColsKey(pattern: Pattern): string {
    if (!this.selectColsKeyCache.has(pattern)) {
      const key = Object.keys(pattern.selectCols).sort().join('|');
      this.selectColsKeyCache.set(pattern, key);
    }
    return this.selectColsKeyCache.get(pattern)!;
  }

  private async mergePatterns(
    _queryObj: Record<string, Term>,
    _walkedQ: Record<string, Term>,
    goalId: number
  ): Promise<void> {
    const patternsForGoal = this.getPatternsForGoal(goalId);
    if (patternsForGoal.length <= 1) return;

    const selectColsGroups = new Map<string, Pattern[]>();
    
    for (const pattern of patternsForGoal) {
      if (pattern.ran) continue;
      
      const selectColsKey = this.getSelectColsKey(pattern);
      if (!selectColsGroups.has(selectColsKey)) {
        selectColsGroups.set(selectColsKey, []);
      }
      selectColsGroups.get(selectColsKey)!.push(pattern);
    }

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
      
      const baseSelectKeys = Object.keys(basePattern.selectCols);
      for (let j = 0; j < baseSelectKeys.length; j++) {
        const key = baseSelectKeys[j];
        if (isVar(basePattern.selectCols[key]) && !isVar(pattern.selectCols[key])) {
          (basePattern.selectCols as any)[key] = pattern.selectCols[key];
        }
      }

      const baseWhereKeys = Object.keys(basePattern.whereCols);
      for (let j = 0; j < baseWhereKeys.length; j++) {
        const key = baseWhereKeys[j];
        if (isVar(basePattern.whereCols[key]) && !isVar(pattern.whereCols[key])) {
          (basePattern.whereCols as any)[key] = pattern.whereCols[key];
        }
      }

      for (const goalId of pattern.goalIds) {
        if (!basePattern.goalIds.includes(goalId)) {
          (basePattern.goalIds as any).push(goalId);
        }
      }
    }
    
    this.selectColsKeyCache.delete(basePattern);
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

  private logFinalDiagnostics(goalId: number): void {
    if (goalId === this.nextGoalId - 1) {
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