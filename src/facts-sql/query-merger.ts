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

type JoinVars = { varId: string; columns: { table: string; column: string; goalId: number; type: 'select' | 'where' }[] }[];
type ProcessingState = 'IDLE' | 'SCHEDULED' | 'RUNNING';

export class QueryMerger {
  private pendingPatterns = new Map<number, QueryPattern>();
  private mergedQueries = new Map<string, MergedQuery>();
  private nextMergedQueryId = 1;
  private readonly mergeDelayMs: number;
  private mergeTimer: NodeJS.Timeout | null = null;
  private processingState: ProcessingState = 'IDLE';

  private processedRows = new Map<number, Set<number>>(); // goalId -> Set of processed row indices
  private static globalGoalId = 1; // Global goal ID counter across all relations
  private patternReadyCallbacks = new Map<number, () => void>(); // goalId -> callback
  private patternProcessor: PatternProcessor;

  private queries: string[] = [];
  
  // SQL-based result cache for identical queries
  private sqlResultCache = new Map<string, any[]>();
  
  /**
   * Creates a normalized version of SQL for caching purposes.
   * This removes alias differences while preserving the essential query structure.
   */
  private normalizeSqlForCache(sql: string): string {
    // For now, create a simple normalization:
    // Replace all AS aliases with a generic pattern
    return sql.replace(/AS `[^`]+`/g, 'AS alias')
      .replace(/AS [^\s,]+/g, 'AS alias');
  }
  
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
    this.sqlResultCache.clear();
    
    this.logger.log('QUERY_MERGER_DESTROYED', 'QueryMerger destroyed and cleaned up');
  }

  /**
   * Private helper to create, store, and log a new pattern.
   */
  private _createAndAddPattern(
    goalId: number,
    table: string,
    queryObj: Record<string, Term>,
    selectCols: Record<string, Term>,
    whereCols: Record<string, Term>,
    options: { isSymmetric?: boolean; symmetricKeys?: [string, string] } = {}
  ): void {
    const varIds = this.patternProcessor.extractVarIds(queryObj);

    const pattern: QueryPattern = {
      goalId,
      table,
      queryObj,
      selectCols,
      whereCols,
      varIds,
      locked: false,
      timestamp: Date.now(),
      ...options,
    };

    this.pendingPatterns.set(goalId, pattern);

    const logType = options.isSymmetric ? "SYMMETRIC_PATTERN_CREATED" : "PATTERN_CREATED";
    const logPayload: any = {
      goalId,
      table,
      varIds: Array.from(varIds),
      selectCols: Object.keys(selectCols),
      whereCols: Object.keys(whereCols),
    };
    if (options.symmetricKeys) {
      logPayload.keys = options.symmetricKeys;
    }
    this.logger.log(logType, `[Goal ${goalId}] Pattern created for table ${table}`, logPayload);

    this._scheduleProcessing();
  }

  /**
   * Add a new query pattern for potential merging.
   */
  addPattern(
    goalId: number,
    table: string,
    queryObj: Record<string, Term>
  ): void {
    const { selectCols, whereCols } = this.patternProcessor.separateQueryColumns(queryObj);
    this._createAndAddPattern(goalId, table, queryObj, selectCols, whereCols);
  }

  /**
   * Add a new query pattern with explicit grounding information.
   */
  addPatternWithGrounding(
    goalId: number,
    table: string,
    originalQueryObj: Record<string, Term>,
    selectCols: Record<string, Term>,
    whereCols: Record<string, Term>
  ): void {
    this._createAndAddPattern(goalId, table, originalQueryObj, selectCols, whereCols);
  }

  /**
   * Add a symmetric query pattern with explicit grounding information.
   */
  addSymmetricPatternWithGrounding(
    goalId: number,
    table: string,
    keys: [string, string],
    originalQueryObj: Record<string, Term>,
    selectCols: Record<string, Term>,
    whereCols: Record<string, Term>
  ): void {
    this._createAndAddPattern(goalId, table, originalQueryObj, selectCols, whereCols, {
      isSymmetric: true,
      symmetricKeys: keys,
    });
  }
  
  /**
   * Retrieves results for a goal, waiting for them to be processed if necessary.
   * This version includes a "fast path" to trigger immediate processing if needed.
   */
  async getResultsForGoal(goalId: number): Promise<any[] | null> {
    // 1. Check if results are already available.
    for (const mergedQuery of this.mergedQueries.values()) {
      if (mergedQuery.patterns.some(p => p.goalId === goalId)) {
        return mergedQuery.results || null;
      }
    }

    // 2. Check if the pattern is pending and can be processed immediately.
    const pattern = this.pendingPatterns.get(goalId);
    if (pattern && !pattern.locked) {
      // If idle or only scheduled for a delayed run, we can run immediately.
      if (this.processingState === 'IDLE' || this.processingState === 'SCHEDULED') {
        await this._runProcessingLoop(true); // `true` indicates an immediate, forced run.
      }
    }

    // 3. Now that we've potentially triggered a run, wait for it to complete.
    const isReady = await this.waitForPatternReady(goalId);
    if (!isReady) {
      this.logger.log("GET_RESULTS_TIMEOUT", `[Goal ${goalId}] Timed out waiting for pattern to be ready.`);
      return null;
    }

    // 4. After waiting, the results should be available.
    for (const mergedQuery of this.mergedQueries.values()) {
      if (mergedQuery.patterns.some(p => p.goalId === goalId)) {
        return mergedQuery.results || null;
      }
    }
    
    this.logger.log("GET_RESULTS_NOT_FOUND", `[Goal ${goalId}] Results not found after pattern was ready.`);
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
    
    // If pattern is not in pending patterns, it was processed and removed
    return !this.pendingPatterns.has(goalId);
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

  /**
   * Schedules a processing run if one is not already scheduled.
   */
  private _scheduleProcessing(): void {
    if (this.processingState !== 'IDLE') {
      return; // A run is already scheduled or in progress.
    }
    
    this.processingState = 'SCHEDULED';
    this.mergeTimer = setTimeout(() => {
      this.mergeTimer = null;
      this._runProcessingLoop(false); // `false` indicates a normal, timed run.
    }, this.mergeDelayMs);
  }
  
  /**
   * The main processing loop, protected by the state machine.
   */
  private async _runProcessingLoop(isForced: boolean): Promise<void> {
    // If forced, cancel any pending timed run.
    if (isForced && this.mergeTimer) {
      clearTimeout(this.mergeTimer);
      this.mergeTimer = null;
    }
    
    // Do not run if another process is already running.
    if (this.processingState === 'RUNNING') {
      return;
    }

    this.processingState = 'RUNNING';
    this.logger.log("PROCESSING_LOOP_START", "Starting pattern processing loop.", {
      isForced 
    });

    const patternsToProcess = Array.from(this.pendingPatterns.values())
      .filter(p => !p.locked);
    
    if (patternsToProcess.length > 0) {
      const mergeGroups = this.patternProcessor.findMergeGroups(patternsToProcess);
      
      for (const group of mergeGroups) {
        await this._processPatternGroup(group);
      }
    }

    this.logger.log("PROCESSING_LOOP_END", "Finished pattern processing loop.");
    this.processingState = 'IDLE';

    // If new patterns arrived while we were busy, schedule a new run.
    if (this.pendingPatterns.size > 0) {
      this._scheduleProcessing();
    }
  }

  /**
   * Processes a group of one or more patterns.
   */
  private async _processPatternGroup(patterns: QueryPattern[]): Promise<void> {
    patterns.forEach(p => p.locked = true);

    const goalIds = patterns.map(p => p.goalId);
    const tables = new Set(patterns.map(p => p.table));
    const isJoin = tables.size > 1;

    let mergedQuery: MergedQuery;
    let joinVars: JoinVars | undefined;

    if (isJoin) {
      const tableNames = Array.from(tables);
      this.logger.log("PATTERN_MERGED", `[Goals ${goalIds.join(',')}] Cross-table patterns merged for JOIN`, {
        goalIds,
        tables: tableNames,
      });

      joinVars = this.patternProcessor.findJoinVariables(patterns);
      if (joinVars.length === 0) {
        this.logger.log("JOIN_NO_SHARED_VARS", `No shared variables for JOIN [Goals ${goalIds.join(',')}], processing individually`);
        for (const pattern of patterns) {
          await this._processPatternGroup([pattern]);
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
        locked: true,
      };
    } else {
      const table = patterns[0].table;
      if (patterns.length > 1) {
        this.logger.log("PATTERN_MERGED", `[Goals ${goalIds.join(',')}] Same-table patterns merged for table ${table}`, {
          goalIds,
          table,
        });
      }
  
      // --- New Same-Table Merge Logic ---
      let mostGeneralPattern = patterns[0];
      if (patterns.length > 1) {
        // Find the pattern with the fewest WHERE conditions as a candidate for the most general.
        for (let i = 1; i < patterns.length; i++) {
          if (Object.keys(patterns[i].whereCols).length < Object.keys(mostGeneralPattern.whereCols).length) {
            mostGeneralPattern = patterns[i];
          }
        }
      }
  
      // Verify that the chosen general pattern's WHERE clauses are a true subset of all other patterns.
      let isMergeValid = true;
      const generalWhereEntries = Object.entries(mostGeneralPattern.whereCols);
      for (const pattern of patterns) {
        if (pattern === mostGeneralPattern) continue;
        for (const [key, value] of generalWhereEntries) {
          if (pattern.whereCols[key] !== value) {
            isMergeValid = false;
            break;
          }
        }
        if (!isMergeValid) break;
      }
  
      if (!isMergeValid) {
        this.logger.log("INCOMPATIBLE_MERGE", `Patterns for table ${table} are not compatible for subset merge. Processing individually.`, {
          goalIds 
        });
        for (const pattern of patterns) {
          await this._processPatternGroup([pattern]);
        }
        return;
      }
  
      // If the merge is valid, use the WHERE clause from the most general pattern.
      const combinedWhereCols = mostGeneralPattern.whereCols;
  
      mergedQuery = {
        id: patterns.length === 1 ? `single_${this.nextMergedQueryId++}` : `merged_${this.nextMergedQueryId++}`,
        patterns, // Pass ALL patterns to the query builder
        table,
        combinedSelectCols: [], // Let the builder construct this
        combinedWhereCols, // Use the general WHERE clause
        sharedVarIds: new Set(patterns.flatMap(p => Array.from(p.varIds))),
        locked: true,
      };
    }

    const results = await this._executeQuery(mergedQuery, joinVars);
    mergedQuery.results = results;
    this.mergedQueries.set(mergedQuery.id, mergedQuery);

    patterns.forEach(p => {
      this.pendingPatterns.delete(p.goalId);
      this.notifyPatternReady(p.goalId);
    });
  }

  /**
   * Builds and executes a query.
   */
  private async _executeQuery(mergedQuery: MergedQuery, joinVars?: JoinVars): Promise<any[]> {
    const queryBuilder = new QueryBuilder(this.db).build(mergedQuery, joinVars);
    const sql = queryBuilder.toString();
    const isJoin = !!joinVars;

    // Check SQL cache first using normalized SQL
    const normalizedSql = this.normalizeSqlForCache(sql);
    if (this.sqlResultCache.has(normalizedSql)) {
      const cachedResults = this.sqlResultCache.get(normalizedSql)!;
      this.logger.log("SQL_CACHE_HIT", `Cache hit for SQL query`, {
        originalSql: sql,
        normalizedSql,
        goalIds: mergedQuery.patterns.map(p => p.goalId),
        rowCount: cachedResults.length
      });
      return cachedResults;
    }

    this.logger.log(
      isJoin ? "JOIN_QUERY_EXECUTING" : "SAME_TABLE_QUERY_EXECUTING",
      `Executing ${isJoin ? 'JOIN' : 'same-table'} query`,
      {
        sql,
      }
    );

    const results = await queryBuilder;
    this.queries.push(sql);

    // Cache the results using normalized SQL
    this.sqlResultCache.set(normalizedSql, results);

    this.logger.log("DB_QUERY_EXECUTED", `[Goals ${mergedQuery.patterns.map(p => p.goalId).join(',')}] Query executed`, {
      goalIds: mergedQuery.patterns.map(p => p.goalId),
      table: mergedQuery.table,
      sql,
      rowCount: results.length,
      rows: results,
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
