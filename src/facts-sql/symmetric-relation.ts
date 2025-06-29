import {
  Term,
  Subst,
  walk,
  unify,
  isVar
} from "../core.ts"
import { Logger } from "../shared/logger.ts";
import { QueryCache } from "./cache.ts";
import { SymmetricPatternManager } from "./pattern-manager.ts";
import { QueryBuilder } from "./query-builder.ts";
import { 
  SymmetricPattern, 
  GoalFunction, 
  RelationOptions, 
  FullScanCache, 
  CacheEntry 
} from "./types.ts";

export class SymmetricRelation {
  private fullScanCache: FullScanCache = {};
  private fullScanKeys: Set<string>;
  private cacheTTL: number;

  constructor(
    private table: string,
    private keys: [string, string],
    private patternManager: SymmetricPatternManager,
    private logger: Logger,
    private cache: QueryCache,
    private queryBuilder: QueryBuilder,
    private queries: string[],
    private realQueries: string[],
    private options?: RelationOptions,
  ) {
    this.fullScanKeys = new Set(options?.fullScanKeys || []);
    this.cacheTTL = options?.cacheTTL ?? 10000; // Default 3 seconds
  }

  createGoal(queryObj: Record<string, Term<string | number>>): GoalFunction {
    const goalId = this.patternManager.generateGoalId();
    
    // Create and add pattern
    const pattern = this.patternManager.createPattern(this.table, queryObj, goalId);
    this.patternManager.addPattern(pattern);

    return async function* factsSqlSym(this: SymmetricRelation, s: Subst) {
      const patterns = this.patternManager.getPatternsForGoal(goalId);
      if (patterns.length === 0) {
        return;
      }

      for (const pattern of patterns) {
        const s2 = s;
        for await (const result of this.runPattern(s2, queryObj, pattern)) {
          if (result !== null) {
            yield result;
          }
        }
      }

      this.patternManager.logFinalDiagnostics(goalId);
    }.bind(this);
  }

  private async* runPattern(
    s: Subst,
    queryObj: Record<string, Term<string | number>>,
    pattern: SymmetricPattern
  ): AsyncGenerator<Subst, void, unknown> {
    if (pattern.ran && pattern.rows.length === 0) {
      return;
    }

    const values = Object.values(queryObj);
    if (values.length > 2) return;

    const walkedValues: Term[] = await Promise.all(values.map(x => walk(x, s)));
    if (walkedValues[0] === walkedValues[1]) return;

    this.logger.log("RUN_START", "Starting symmetric relation", {
      pattern,
      queryObj,
      walkedValues 
    });

    const { rows, cacheInfo } = await this.getPatternRows(pattern, walkedValues);
    this.patternManager.updatePatternRows(pattern, rows);

    // Process rows and yield unified substitutions
    for (const row of pattern.rows) {
      // Try first orientation
      const s2 = new Map(s);
      const unified1 = await unify(walkedValues[0], row[this.keys[0]], s2);
      if (unified1) {
        const unified2 = await unify(walkedValues[1], row[this.keys[1]], unified1);
        if (unified2) {
          yield unified2;
          continue;
        }
      }

      // Try second orientation (symmetric)
      const s3 = new Map(s);
      const unified3 = await unify(walkedValues[1], row[this.keys[0]], s3);
      if (unified3) {
        const unified4 = await unify(walkedValues[0], row[this.keys[1]], unified3);
        if (unified4) {
          yield unified4;
        }
      }
    }

    this.logger.log("RUN_END", "Completed symmetric relation", {
      pattern 
    });
  }

  private async getPatternRows(
    pattern: SymmetricPattern,
    walkedValues: Term[]
  ): Promise<{ rows: any[], cacheInfo: any }> {
    // Try pattern cache first
    if (pattern.ran) {
      this.logger.log("PATTERN_CACHE_HIT", "Pattern cache hit", {
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
    const matchingPattern = this.cache.findMatchingSymmetricPattern(
      this.patternManager.getAllPatterns(),
      pattern
    );
    
    if (matchingPattern) {
      const rows = this.cache.processCachedPatternResult(matchingPattern);
      this.logger.log("PATTERN_CACHE_HIT", "Matching pattern found", {
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

    // Execute database query
    return await this.executeQuery(pattern, walkedValues);
  }

  private async executeQuery(
    pattern: SymmetricPattern,
    walkedValues: Term[]
  ): Promise<{ rows: any[], cacheInfo: any }> {
    const groundedValues = walkedValues.filter(x => !isVar(x)) as (string | number)[];
    
    // Check if any of the grounded values match fullScanKeys
    const fullScanKey = this.keys.find(key => this.fullScanKeys.has(key));
    
    if (fullScanKey && groundedValues.length > 0) {
      return await this.executeFullScanQuery(pattern, fullScanKey, groundedValues[0]);
    }
    
    // Regular symmetric query execution
    const query = this.queryBuilder.buildSymmetricQuery(
      this.table,
      this.keys,
      groundedValues
    );

    const { rows, sql } = await this.queryBuilder.executeQuery(query);
    (pattern as any).ran = true;

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

  private async executeFullScanQuery(
    pattern: SymmetricPattern,
    fullScanKey: string,
    value: string | number
  ): Promise<{ rows: any[], cacheInfo: any }> {
    // Check if we already have valid (non-expired) full scan results cached for this value
    let allRows = this.getCachedData(fullScanKey, value);
    
    if (!allRows) {
      // Perform optimized single-query full scan using OR condition
      // This replaces two separate queries with one efficient OR query
      const query = this.queryBuilder.buildSymmetricQuery(
        this.table,
        this.keys,
        [value] // Single value triggers OR condition in buildSymmetricQuery
      );
      
      // Execute the single optimized query
      const result = await this.queryBuilder.executeQuery(query);
      const uniqueRows = this.deduplicateRows(result.rows);
      
      // Cache the full scan results for the primary value using TTL-aware method
      this.setCachedData(fullScanKey, value, uniqueRows);
      allRows = uniqueRows;
      
      // For symmetric relations, also cache for all connected values to enable bidirectional cache hits
      this.cacheConnectedValues(fullScanKey, value, uniqueRows);
      
      this.logger.log("FULL_SCAN_EXECUTED", `Optimized symmetric full scan executed for ${fullScanKey}=${value}`, {
        table: this.table,
        key: fullScanKey,
        value,
        rowCount: uniqueRows.length,
        sql: result.sql,
        optimization: 'single-OR-query'
      });
      
      this.queries.push(result.sql);
      this.realQueries.push(result.sql);
      (pattern as any).queries.push(result.sql);
    } else {
      this.logger.log("FULL_SCAN_CACHE_HIT", `Symmetric full scan cache hit for ${fullScanKey}=${value}`, {
        table: this.table,
        key: fullScanKey,
        value,
        rowCount: allRows.length
      });
    }
    
    // Mark pattern as ran and store results for future cache hits
    (pattern as any).ran = true;
    (pattern as any).rows = allRows;
    
    // Create additional patterns for cache hits
    this.createFullScanCachePatterns(fullScanKey, value, allRows);
    
    return {
      rows: allRows,
      cacheInfo: {
        type: 'symmetricFullScan',
        key: fullScanKey,
        value,
        totalRows: allRows.length
      }
    };
  }

  private cacheConnectedValues(fullScanKey: string, primaryValue: string | number, rows: any[]): void {
    // For symmetric relations, cache the results for all connected values
    // This enables cache hits when querying for any of the connected entities
    const [key1, key2] = this.keys;
    const connectedValues = new Set<string | number>();
    
    // Extract all unique values that are connected to the primary value
    rows.forEach(row => {
      const val1 = row[key1];
      const val2 = row[key2];
      if (val1 !== primaryValue) connectedValues.add(val1);
      if (val2 !== primaryValue) connectedValues.add(val2);
    });
    
    // Cache a subset of results for each connected value (only rows involving that value)
    connectedValues.forEach(connectedValue => {
      if (!this.getCachedData(fullScanKey, connectedValue)) {
        const relevantRows = rows.filter(row => 
          row[key1] === connectedValue || row[key2] === connectedValue
        );
        this.setCachedData(fullScanKey, connectedValue, relevantRows);
        
        this.logger.log("BIDIRECTIONAL_CACHE_SET", `Cached results for connected value ${connectedValue}`, {
          connectedValue,
          primaryValue,
          rowCount: relevantRows.length
        });
      }
    });
  }

  private deduplicateRows(rows: any[]): any[] {
    const seen = new Set<string>();
    return rows.filter(row => {
      const key = JSON.stringify(row);
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
  }

  private createFullScanCachePatterns(key: string, value: string | number, allRows: any[]): void {
    // Create a "master" pattern for this symmetric fullScan key-value pair
    const masterPattern: SymmetricPattern = {
      table: this.table,
      goalIds: [-1], // Special goal ID for fullScan patterns
      rows: allRows,
      ran: true,
      selectCols: [], // Empty - this pattern can match any select columns
      whereCols: [value], // The grounded value
      queries: [`FULL_SCAN_SYM:${this.table}:${key}=${value}`],
      last: {
        selectCols: [],
        whereCols: []
      }
    };
    
    // Add this pattern to the pattern manager so future queries can match against it
    this.patternManager.addPattern(masterPattern);
    
    this.logger.log("FULL_SCAN_PATTERNS_CREATED", `Symmetric master pattern created for future cache hits`, {
      table: this.table,
      key,
      value,
      rowCount: allRows.length,
      patternId: masterPattern.goalIds[0]
    });
  }

  private isCacheEntryExpired(entry: CacheEntry): boolean {
    return Date.now() - entry.timestamp > this.cacheTTL;
  }

  private getCachedData(key: string, value: any): any[] | null {
    const entry = this.fullScanCache[key]?.[value];
    if (!entry) {
      return null;
    }
    
    if (this.isCacheEntryExpired(entry)) {
      this.logger.log("FULL_SCAN_CACHE_EXPIRED", `Cache entry expired for ${key}=${value}`, {
        table: this.table,
        key,
        value,
        age: Date.now() - entry.timestamp,
        ttl: this.cacheTTL
      });
      
      // Clean up expired entry
      delete this.fullScanCache[key][value];
      return null;
    }
    
    return entry.data;
  }

  private setCachedData(key: string, value: any, data: any[]): void {
    if (!this.fullScanCache[key]) {
      this.fullScanCache[key] = {};
    }
    
    this.fullScanCache[key][value] = {
      data,
      timestamp: Date.now()
    };
    
    this.logger.log("FULL_SCAN_CACHE_SET", `Cache entry created for ${key}=${value}`, {
      table: this.table,
      key,
      value,
      rowCount: data.length,
      ttl: this.cacheTTL
    });
  }
}