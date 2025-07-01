import type { Knex } from "knex";
import { isVar, walk } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";
import type { Term, Subst, Var } from "../core/types.ts";
import { QueryBuilder } from './query-builder.ts';
import { PatternProcessor } from "./pattern-processor.ts";

export interface QueryPattern {
  goalId: number;
  table: string;
  queryObj: Record<string, Term>;
  selectCols: Record<string, Term>;
  whereCols: Record<string, Term>;
  varIds: Set<string>;
  locked: boolean;
  timestamp: number;
  isSymmetric?: boolean;
  symmetricKeys?: [string, string];
}

export interface MergedQuery {
  id: string;
  patterns: QueryPattern[];
  table: string;
  combinedSelectCols: string[];
  combinedWhereCols: Record<string, any>;
  sharedVarIds: Set<string>;
  locked: boolean;
  results?: any[];
}

export class QueryMerger {
  private pendingPatterns = new Map<number, QueryPattern>();
  private mergedQueries = new Map<string, MergedQuery>();
  private nextMergedQueryId = 1;
  private readonly mergeDelayMs: number;
  private mergeTimer: NodeJS.Timeout | null = null;
  private processedRows = new Map<number, Set<number>>(); // goalId -> Set of processed row indices
  private static globalGoalId = 1; // Global goal ID counter across all relations
  private patternReadyCallbacks = new Map<number, () => void>(); // goalId -> callback
  private patternProcessor: PatternProcessor;

  private queries: string[] = [];
  
  // Memory management settings
  private cleanupTimer: NodeJS.Timeout | null = null;
  private readonly maxGoalAge = 5 * 60 * 1000; // 5 minutes (goals can be reused but will restart)
  private readonly cleanupIntervalMs = 2 * 60 * 1000; // 2 minutes
  private readonly MAX_GOAL_ID = 1000000; // Reset after 1M goals
  private memoryMonitorTimer: NodeJS.Timeout | null = null;

  constructor(
    private logger: Logger,
    private db: Knex,
    mergeDelayMs = 50
  ) {
    this.mergeDelayMs = mergeDelayMs;
    this.patternProcessor = new PatternProcessor(logger);
    this.startCleanupTimer();
    this.startMemoryMonitor();
  }

  public getQueries(): string[] {
    return [...this.queries];
  }

  public clearQueries(): void {
    this.queries.length = 0;
  }

  public getQueryCount(): number {
    return this.queries.length;
  }

  public getNextGoalId(): number {
    const id = QueryMerger.globalGoalId++;
    
    // Reset goal ID if it gets too large to prevent overflow
    if (QueryMerger.globalGoalId > this.MAX_GOAL_ID) {
      QueryMerger.globalGoalId = 1;
      this.logger.log('GOAL_ID_RESET', 'Goal ID counter reset to prevent overflow');
    }
    
    return id;
  }

  private startCleanupTimer(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
    }
    
    this.cleanupTimer = setInterval(() => {
      this.cleanupOldGoals();
    }, this.cleanupIntervalMs);
  }

  private startMemoryMonitor(): void {
    if (this.memoryMonitorTimer) {
      clearInterval(this.memoryMonitorTimer);
    }
    
    this.memoryMonitorTimer = setInterval(() => {
      this.logMemoryStats();
    }, 60 * 1000); // Every minute
  }

  private cleanupOldGoals(): void {
    const now = Date.now();
    let cleaned = 0;
    
    // Clean up old patterns
    for (const [goalId, pattern] of this.pendingPatterns.entries()) {
      if (now - pattern.timestamp > this.maxGoalAge) {
        this.pendingPatterns.delete(goalId);
        cleaned++;
      }
    }
    
    // Clean up old processed rows
    for (const [goalId, _] of this.processedRows.entries()) {
      const pattern = this.pendingPatterns.get(goalId);
      if (!pattern || now - pattern.timestamp > this.maxGoalAge) {
        this.processedRows.delete(goalId);
        cleaned++;
      }
    }
    
    // Clean up old callbacks
    for (const goalId of this.patternReadyCallbacks.keys()) {
      const pattern = this.pendingPatterns.get(goalId);
      if (!pattern || now - pattern.timestamp > this.maxGoalAge) {
        this.patternReadyCallbacks.delete(goalId);
        cleaned++;
      }
    }
    
    // Clean up old merged queries (check if any patterns reference them)
    for (const [queryId, mergedQuery] of this.mergedQueries.entries()) {
      const hasActivePatterns = mergedQuery.patterns.some(p => 
        this.pendingPatterns.has(p.goalId)
      );
      if (!hasActivePatterns) {
        this.mergedQueries.delete(queryId);
        cleaned++;
      }
    }
    
    if (cleaned > 0) {
      this.logger.log('QUERY_MERGER_CLEANUP', `Cleaned ${cleaned} old goal entries`);
    }
  }

  private logMemoryStats(): void {
    const memUsage = process.memoryUsage();
    const stats = {
      heapUsed: `${Math.round(memUsage.heapUsed / 1024 / 1024)}MB`,
      heapTotal: `${Math.round(memUsage.heapTotal / 1024 / 1024)}MB`,
      pendingPatterns: this.pendingPatterns.size,
      mergedQueries: this.mergedQueries.size,
      processedRows: this.processedRows.size,
      patternCallbacks: this.patternReadyCallbacks.size,
      currentGoalId: QueryMerger.globalGoalId
    };
    
    this.logger.log('MEMORY_STATS', 'Memory usage', stats);
    
    // Warning for high memory usage
    if (memUsage.heapUsed > 500 * 1024 * 1024) { // 500MB warning
      this.logger.log('MEMORY_WARNING', `High memory usage detected: ${stats.heapUsed}`);
    }
    
    // Warning for high goal counts
    if (this.pendingPatterns.size > 1000) {
      this.logger.log('GOAL_COUNT_WARNING', `High pending pattern count: ${this.pendingPatterns.size}`);
    }
  }

  // Note: Goals are not marked as "completed" since async generators can be resumed
  // multiple times. Instead, we rely on time-based cleanup to remove old goal data.

  // Force cleanup now (useful for testing or manual cleanup)
  public forceCleanup(): void {
    this.cleanupOldGoals();
  }

  // Get current state statistics
  public getStats(): {
    pendingPatterns: number;
    mergedQueries: number;
    processedRows: number;
    patternCallbacks: number;
    currentGoalId: number;
    } {
    return {
      pendingPatterns: this.pendingPatterns.size,
      mergedQueries: this.mergedQueries.size,
      processedRows: this.processedRows.size,
      patternCallbacks: this.patternReadyCallbacks.size,
      currentGoalId: QueryMerger.globalGoalId
    };
  }

  // Destroy the query merger and cleanup timers
  public destroy(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    
    if (this.memoryMonitorTimer) {
      clearInterval(this.memoryMonitorTimer);
      this.memoryMonitorTimer = null;
    }
    
    if (this.mergeTimer) {
      clearTimeout(this.mergeTimer);
      this.mergeTimer = null;
    }
    
    // Clear all data
    this.pendingPatterns.clear();
    this.mergedQueries.clear();
    this.processedRows.clear();
    this.patternReadyCallbacks.clear();
    this.queries.length = 0;
    
    this.logger.log('QUERY_MERGER_DESTROYED', 'QueryMerger destroyed and cleaned up');
  }

  /**
   * Add a new query pattern for potential merging
   */
  async addPattern(
    goalId: number,
    table: string,
    queryObj: Record<string, Term>
  ): Promise<void> {
    const { selectCols, whereCols } = this.patternProcessor.separateQueryColumns(queryObj);
    const varIds = this.patternProcessor.extractVarIds(queryObj);
    
    const pattern: QueryPattern = {
      goalId,
      table,
      queryObj,
      selectCols,
      whereCols,
      varIds,
      locked: false,
      timestamp: Date.now()
    };

    this.pendingPatterns.set(goalId, pattern);
    this.logger.log("PATTERN_CREATED", `[Goal ${goalId}] Pattern created for table ${table}`, {
      goalId,
      table,
      varIds: Array.from(varIds),
      selectCols: Object.keys(selectCols),
      whereCols: Object.keys(whereCols)
    });

    // Schedule pattern processing with short delay to allow batching
    this.scheduleMergeProcessing();
  }

  /**
   * Add a new query pattern with explicit grounding information (for execution-time grounding)
   */
  async addPatternWithGrounding(
    goalId: number,
    table: string,
    originalQueryObj: Record<string, Term>,
    selectCols: Record<string, Term>,
    whereCols: Record<string, Term>
  ): Promise<void> {
    const varIds = this.patternProcessor.extractVarIds(originalQueryObj);
    
    const pattern: QueryPattern = {
      goalId,
      table,
      queryObj: originalQueryObj, // Keep original query with variables
      selectCols,
      whereCols,
      varIds,
      locked: false,
      timestamp: Date.now()
    };

    this.pendingPatterns.set(goalId, pattern);
    
    this.logger.log("PATTERN_CREATED", `[Goal ${goalId}] Pattern created for table ${table}`, {
      goalId,
      table,
      varIds: Array.from(varIds),
      selectCols: Object.keys(selectCols),
      whereCols: Object.keys(whereCols)
    });

    // Schedule pattern processing with short delay to allow batching
    this.scheduleMergeProcessing();
  }

  /**
   * Add a symmetric query pattern with explicit grounding information (for execution-time grounding)
   */
  async addSymmetricPatternWithGrounding(
    goalId: number,
    table: string,
    keys: [string, string],
    originalQueryObj: Record<string, Term>,
    selectCols: Record<string, Term>,
    whereCols: Record<string, Term>
  ): Promise<void> {
    const varIds = this.patternProcessor.extractVarIds(originalQueryObj);
    
    const pattern: QueryPattern = {
      goalId,
      table,
      queryObj: originalQueryObj, // Keep original query with variables
      selectCols,
      whereCols,
      varIds,
      locked: false,
      timestamp: Date.now(),
      isSymmetric: true,
      symmetricKeys: keys
    };

    this.pendingPatterns.set(goalId, pattern);
    
    this.logger.log("SYMMETRIC_PATTERN_CREATED", `[Goal ${goalId}] Symmetric pattern created for table ${table}`, {
      goalId,
      table,
      keys,
      varIds: Array.from(varIds),
      selectCols: Object.keys(selectCols),
      whereCols: Object.keys(whereCols)
    });

    // Schedule pattern processing with short delay to allow batching
    this.scheduleMergeProcessing();
  }

  

  /**
   * Get merged query results for a specific goal
   */
  async getResultsForGoal(goalId: number, s: Subst): Promise<any[] | null> {
    // Find which merged query contains this goal
    for (const mergedQuery of this.mergedQueries.values()) {
      const pattern = mergedQuery.patterns.find(p => p.goalId === goalId);
      if (pattern && mergedQuery.results) {
        // Simply return the results - grounding is handled at execution time now
        return mergedQuery.results;
      }
    }
    
    // Check if pattern is still pending
    const pattern = this.pendingPatterns.get(goalId);
    if (pattern && !pattern.locked) {
      // Force processing of pending patterns
      await this.processPendingPatterns();
      return this.getResultsForGoal(goalId, s);
    }
    
    return null;
  }

  /**
   * Check if a goal's pattern is ready (either merged or individual)
   */
  isPatternReady(goalId: number): boolean {
    // Check if it's in a completed merged query
    for (const mergedQuery of this.mergedQueries.values()) {
      if (mergedQuery.patterns.some(p => p.goalId === goalId) && mergedQuery.results) {
        return true;
      }
    }
    
    // Check if it's locked (being processed)
    const pattern = this.pendingPatterns.get(goalId);
    return pattern ? pattern.locked : false;
  }

  /**
   * Wait for a pattern to be ready using async event instead of polling
   */
  async waitForPatternReady(goalId: number, timeoutMs = 5000): Promise<boolean> {
    if (this.isPatternReady(goalId)) {
      return true;
    }

    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        this.patternReadyCallbacks.delete(goalId);
        resolve(false);
      }, timeoutMs);

      this.patternReadyCallbacks.set(goalId, () => {
        clearTimeout(timeout);
        this.patternReadyCallbacks.delete(goalId);
        resolve(true);
      });
    });
  }

  /**
   * Notify that a pattern is ready
   */
  private notifyPatternReady(goalId: number): void {
    const callback = this.patternReadyCallbacks.get(goalId);
    if (callback) {
      callback();
    }
  }

  

  private scheduleMergeProcessing(): void {
    if (this.mergeTimer) {
      return; // Already scheduled
    }
    
    this.mergeTimer = setTimeout(() => {
      this.mergeTimer = null;
      this.processPendingPatterns();
    }, this.mergeDelayMs);
  }

  /**
   * Force immediate processing of pending patterns without delay
   */
  private forceProcessPendingPatterns(): Promise<void> {
    if (this.mergeTimer) {
      clearTimeout(this.mergeTimer);
      this.mergeTimer = null;
    }
    return this.processPendingPatterns();
  }

  

  private async processPendingPatterns(): Promise<void> {
    const patterns = Array.from(this.pendingPatterns.values())
      .filter(p => !p.locked);
    
    if (patterns.length === 0) {
      return;
    }

    // Find merge groups across ALL patterns (not just by table)
    const mergeGroups = this.patternProcessor.findMergeGroups(patterns);
    
    // Process each group of patterns
    for (const group of mergeGroups) {
      await this.processPatternGroup(group);
    }
  }

  /**
   * Processes a group of one or more patterns, handling single, same-table,
   * and cross-table (JOIN) scenarios. This method consolidates the logic
   * from the previous processSinglePattern, processSameTableMergedPatterns,
   * and processCrossTableMergedPatterns methods.
   */
  private async processPatternGroup(patterns: QueryPattern[]): Promise<void> {
    // 1. Lock all patterns in the group to prevent reprocessing.
    patterns.forEach(p => p.locked = true);

    const goalIds = patterns.map(p => p.goalId);
    const tables = new Set(patterns.map(p => p.table));
    const isSinglePattern = patterns.length === 1;
    const isJoin = tables.size > 1;

    let mergedQuery: MergedQuery;
    let results: any[];

    if (isJoin) {
      // --- Logic for Cross-Table JOIN ---
      const tableNames = Array.from(tables);
      this.logger.log("PATTERN_MERGED", `[Goals ${goalIds.join(',')}] Cross-table patterns merged for JOIN`, {
        goalIds,
        tables: tableNames,
        sharedVarIds: Array.from(new Set(patterns.flatMap(p => Array.from(p.varIds))))
      });

      const joinVars = this.patternProcessor.findJoinVariables(patterns);
      if (joinVars.length === 0) {
        this.logger.log("JOIN_NO_SHARED_VARS", `No shared variables for JOIN [Goals ${goalIds.join(',')}], processing individually`);
        // Fallback: process each pattern in the group individually.
        for (const pattern of patterns) {
          await this.processPatternGroup([pattern]);
        }
        return;
      }

      mergedQuery = {
        id: `join_${this.nextMergedQueryId++}`,
        patterns,
        table: tableNames.join('_JOIN_'),
        combinedSelectCols: this.patternProcessor.buildJoinSelectColumns(patterns),
        combinedWhereCols: this.patternProcessor.buildJoinWhereConditions(patterns),
        sharedVarIds: new Set(patterns.flatMap(p => Array.from(p.varIds))),
        locked: true
      };
      results = await this.executeJoinQuery(mergedQuery, joinVars);
    } else {
      // --- Logic for Single Pattern or Same-Table Merge ---
      const table = patterns[0].table;
      if (!isSinglePattern) {
        this.logger.log("PATTERN_MERGED", `[Goals ${goalIds.join(',')}] Same-table patterns merged for table ${table}`, {
          goalIds,
          table,
          sharedVarIds: Array.from(new Set(patterns.flatMap(p => Array.from(p.varIds))))
        });
      }

      const combinedSelectCols = new Set<string>();
      const combinedWhereCols: Record<string, any> = {};
      const sharedVarIds = new Set<string>();

      for (const pattern of patterns) {
        Object.keys(pattern.selectCols).forEach(col => combinedSelectCols.add(col));
        for (const [col, value] of Object.entries(pattern.whereCols)) {
          if (!isVar(value)) {
            combinedWhereCols[col] = value;
          }
        }
        pattern.varIds.forEach(id => sharedVarIds.add(id));
      }

      mergedQuery = {
        id: isSinglePattern ? `single_${this.nextMergedQueryId++}` : `merged_${this.nextMergedQueryId++}`,
        patterns,
        table,
        combinedSelectCols: Array.from(combinedSelectCols),
        combinedWhereCols,
        sharedVarIds,
        locked: true
      };
      results = await this.executeQuery(mergedQuery);
    }

    // 4. Store results and add to the main merged queries map.
    mergedQuery.results = results;
    this.mergedQueries.set(mergedQuery.id, mergedQuery);

    // 5. Remove processed patterns from pending and notify any listeners.
    for (const pattern of patterns) {
      this.pendingPatterns.delete(pattern.goalId);
      this.notifyPatternReady(pattern.goalId);
    }
  }


  private async executeQuery(mergedQuery: MergedQuery): Promise<any[]> {
    const queryBuilder = new QueryBuilder(this.db).build(mergedQuery);
    const sql = queryBuilder.toSQL().toNative();
    this.logger.log("SAME_TABLE_QUERY_EXECUTING", `Executing same-table query with aliases`, {
      sql: sql.sql,
      bindings: sql.bindings
    });
    
    const results = await queryBuilder;
    this.queries.push(sql.sql);
    
    this.logger.log("DB_QUERY_EXECUTED", `[Goals ${mergedQuery.patterns.map(p => p.goalId).join(',')}] Database query executed`, {
      goalIds: mergedQuery.patterns.map(p => p.goalId),
      table: mergedQuery.table,
      sql: sql.sql,
      rowCount: results.length,
      rows: results
    });
    
    return results;
  }

  private async executeJoinQuery(
    mergedQuery: MergedQuery, 
    joinVars: { varId: string; columns: { table: string; column: string; goalId: number; type: 'select' | 'where' }[] }[]
  ): Promise<any[]> {
    const queryBuilder = new QueryBuilder(this.db).build(mergedQuery, joinVars);
    const sql = queryBuilder.toSQL().toNative();
    
    this.logger.log("JOIN_QUERY_EXECUTING", `Executing JOIN query`, {
      sql: sql.sql,
      bindings: sql.bindings
    });
    
    const results = await queryBuilder;
    this.queries.push(sql.sql);
    
    this.logger.log("DB_QUERY_EXECUTED", `[Goals ${mergedQuery.patterns.map(p => p.goalId).join(',')}] JOIN query executed`, {
      goalIds: mergedQuery.patterns.map(p => p.goalId),
      table: mergedQuery.table,
      sql: sql.sql,
      rowCount: results.length,
      rows: results
    });
    
    return results;
  }

  /**
   * Get processed row indices for a specific goal
   */
  async getProcessedRowsForGoal(goalId: number): Promise<Set<number>> {
    if (!this.processedRows.has(goalId)) {
      this.processedRows.set(goalId, new Set());
    }
    return this.processedRows.get(goalId)!;
  }

  /**
   * Mark a row as processed for a specific goal
   */
  markRowProcessedForGoal(goalId: number, rowIndex: number): void {
    if (!this.processedRows.has(goalId)) {
      this.processedRows.set(goalId, new Set());
    }
    this.processedRows.get(goalId)!.add(rowIndex);
  }

  /**
   * Debug method to print patterns and their cached rows for a specific goal
   */
  debugPrintPatternsAndRows(goalId: number): void {    
    // Find the merged query that contains this goal
    for (const [mergedQueryId, mergedQuery] of this.mergedQueries.entries()) {
      const pattern = mergedQuery.patterns.find(p => p.goalId === goalId);
      if (pattern) {
        // Use console.dir for detailed debugging output
        this.logger.log("DEBUG_PATTERNS_AND_ROWS", String(goalId), {
          goalId,
          mergedQueryId,
          mergedQuery,
          patternsInQuery: mergedQuery.patterns.map(p => ({
            goalId: p.goalId,
            table: p.table,
            varIds: Array.from(p.varIds),
            selectCols: Object.keys(p.selectCols),
            whereCols: Object.keys(p.whereCols),
            fullSelectCols: p.selectCols,
            fullWhereCols: p.whereCols
          })),
          cachedRowCount: mergedQuery.results?.length || 0,
          cachedRows: mergedQuery.results || [],
          processedRowsForThisGoal: Array.from(this.processedRows.get(goalId) || [])
        });
        break;
      }
    }
  }  
}
