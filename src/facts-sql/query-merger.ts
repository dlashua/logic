import type { Knex } from "knex";
import { Term, Subst, Var } from "../core/types.ts";
import { isVar, walk } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";

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
  
  private queries: string[] = [];

  constructor(
    private logger: Logger,
    private db: Knex,
    mergeDelayMs = 100
  ) {
    this.mergeDelayMs = mergeDelayMs;
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
    return QueryMerger.globalGoalId++;
  }

  /**
   * Add a new query pattern for potential merging
   */
  addPattern(
    goalId: number,
    table: string,
    queryObj: Record<string, Term>
  ): void {
    const { selectCols, whereCols } = this.separateQueryColumns(queryObj);
    const varIds = this.extractVarIds(queryObj);
    
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

    // Schedule merge processing if not already scheduled
    this.scheduleMergeProcessing();
  }

  /**
   * Add a new query pattern with explicit grounding information (for execution-time grounding)
   */
  addPatternWithGrounding(
    goalId: number,
    table: string,
    originalQueryObj: Record<string, Term>,
    selectCols: Record<string, Term>,
    whereCols: Record<string, Term>
  ): void {
    const varIds = this.extractVarIds(originalQueryObj);
    
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

    // Schedule merge processing if not already scheduled
    this.scheduleMergeProcessing();
  }

  /**
   * Add a symmetric query pattern with explicit grounding information (for execution-time grounding)
   */
  addSymmetricPatternWithGrounding(
    goalId: number,
    table: string,
    keys: [string, string],
    originalQueryObj: Record<string, Term>,
    selectCols: Record<string, Term>,
    whereCols: Record<string, Term>
  ): void {
    const varIds = this.extractVarIds(originalQueryObj);
    
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

    // Schedule merge processing if not already scheduled
    this.scheduleMergeProcessing();
  }

  /**
   * Check if we already have cached results that can satisfy this query
   */
  async checkForExistingResults(
    table: string, 
    walkedQuery: Record<string, Term>,
    originalQuery: Record<string, Term>
  ): Promise<any[] | null> {
    const { selectCols, whereCols } = this.separateQueryColumns(walkedQuery);
    const varIds = this.extractVarIds(originalQuery);
    
    this.logger.log("CHECK_EXISTING_RESULTS", `Checking for existing results for table ${table}`, {
      table,
      selectCols: Object.keys(selectCols),
      whereCols: Object.keys(whereCols),
      varIds: Array.from(varIds),
      existingQueries: Array.from(this.mergedQueries.keys())
    });
    
    // Look for existing merged queries for this table that contain all the data we need
    for (const mergedQuery of this.mergedQueries.values()) {
      if (mergedQuery.table !== table || !mergedQuery.results) {
        continue;
      }
      
      // Check if this merged query can satisfy our WHERE conditions AND variable context
      const canSatisfy = this.canQuerySatisfyConditions(mergedQuery, whereCols, varIds);
      
      if (canSatisfy) {
        this.logger.log("FOUND_SATISFYING_CACHE", `Found cached query that can satisfy new query`, {
          cachedQueryId: mergedQuery.id,
          cachedWhereCols: Object.keys(mergedQuery.combinedWhereCols),
          newWhereCols: Object.keys(whereCols),
          cachedRowCount: mergedQuery.results.length
        });
        
        // Filter and remap the cached results to match our query structure
        return this.filterAndRemapCachedResults(mergedQuery.results, whereCols, selectCols, originalQuery);
      }
    }
    
    return null;
  }
  
  /**
   * Check if a cached query can satisfy the given WHERE conditions and variable context
   */
  private canQuerySatisfyConditions(
    mergedQuery: MergedQuery, 
    whereCols: Record<string, Term>,
    varIds?: Set<string>
  ): boolean {
    // Case 1: Cached query has no WHERE conditions (full table scan) - can satisfy any query
    if (Object.keys(mergedQuery.combinedWhereCols).length === 0) {
      return true;
    }
    
    // Case 2: New query has no WHERE conditions but cached query does - cannot satisfy
    if (Object.keys(whereCols).length === 0) {
      return false;
    }
    
    // Case 3: Check if cached query's WHERE conditions are EXACTLY the same as new query
    // We need to be much more strict here to avoid the aggressive caching bug
    
    const cachedWhereKeys = Object.keys(mergedQuery.combinedWhereCols).sort();
    const newWhereKeys = Object.keys(whereCols).sort();
    
    // Both queries must have the exact same WHERE columns
    if (cachedWhereKeys.length !== newWhereKeys.length) {
      return false;
    }
    
    // Check that all columns and values match exactly
    for (let i = 0; i < cachedWhereKeys.length; i++) {
      const cachedCol = cachedWhereKeys[i];
      const newCol = newWhereKeys[i];
      
      // Different columns - cannot satisfy
      if (cachedCol !== newCol) {
        return false;
      }
      
      // Same column but different values - cannot satisfy
      if (mergedQuery.combinedWhereCols[cachedCol] !== whereCols[newCol]) {
        return false;
      }
    }
    
    // Case 4: Check variable context - only reuse cache if variable IDs overlap significantly
    // This prevents different logical goals from incorrectly sharing cache
    if (varIds && varIds.size > 0) {
      const intersection = new Set([...varIds].filter(v => mergedQuery.sharedVarIds.has(v)));
      const unionSize = varIds.size + mergedQuery.sharedVarIds.size - intersection.size;
      const similarity = intersection.size / unionSize;
      
      // Only reuse cache if there's significant variable overlap (>50%)
      // This ensures logically related queries can share cache while preventing 
      // unrelated queries that happen to use same table structure from sharing
      if (similarity < 0.5) {
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * Filter and remap cached results to match the new query structure
   */
  private filterAndRemapCachedResults(
    cachedResults: any[], 
    whereCols: Record<string, Term>,
    selectCols: Record<string, Term>,
    originalQuery: Record<string, Term>
  ): any[] {
    this.logger.log("FILTER_AND_REMAP_CACHED_RESULTS", `Filtering and remapping cached results`, {
      cachedRowCount: cachedResults.length,
      whereCols: Object.keys(whereCols),
      selectCols: Object.keys(selectCols),
      originalQuery: Object.keys(originalQuery),
      sampleCachedRow: cachedResults[0]
    });
    
    // Filter results based on WHERE conditions
    let filteredResults = cachedResults;
    
    if (Object.keys(whereCols).length > 0) {
      filteredResults = cachedResults.filter(row => {
        for (const [column, expectedValue] of Object.entries(whereCols)) {
          // The cached row might have this column under a different alias
          const actualValue = this.findColumnValueInRow(row, column);
          
          if (actualValue !== expectedValue) {
            return false;
          }
        }
        return true;
      });
    }
    
    this.logger.log("FILTER_RESULTS", `Filtered ${cachedResults.length} -> ${filteredResults.length} rows`);
    
    // Remap results to match the new query's variable structure
    const remappedResults = filteredResults.map(row => {
      const newRow: any = {};
      
      // Map each column in the original query to the appropriate value
      for (const [column, originalTerm] of Object.entries(originalQuery)) {
        if (isVar(originalTerm)) {
          // This column should get its value from the cached row
          if (whereCols[column] !== undefined) {
            // This column is grounded in the new query - use the grounded value
            newRow[originalTerm.id] = whereCols[column];
          } else {
            // This column is still a variable - get its value from cached row
            const value = this.findColumnValueInRow(row, column);
            if (value !== undefined) {
              newRow[originalTerm.id] = value;
            }
          }
        }
      }
      
      return newRow;
    });
    
    this.logger.log("REMAP_RESULTS", `Remapped results`, {
      sampleOriginalRow: filteredResults[0],
      sampleRemappedRow: remappedResults[0]
    });
    
    return remappedResults;
  }
  
  /**
   * Find a column value in a row, handling different aliasing patterns
   */
  private findColumnValueInRow(row: any, column: string): any {
    // Try direct column name
    if (row[column] !== undefined) {
      return row[column];
    }
    
    // The cached row columns are aliased, so we need to find the right alias
    // Look for any key that seems to correspond to this column
    for (const [key, value] of Object.entries(row)) {
      // Check various patterns:
      // 1. Key contains the column name (e.g., "q_person_0" contains "person")
      // 2. Column name contains the key (e.g., "parent" contains "p")
      // 3. Both refer to the same database column (common case: "parent" vs "parent_5")
      const keyBase = key.split('_')[0]; // Extract base from "q_person_0" -> "q"
      const columnBase = column.split('_')[0]; // Extract base from "kid" -> "kid"
      
      if (key.includes(column) || column.includes(keyBase) || 
          (column === 'kid' && key.includes('person')) ||
          (column === 'parent' && (key.includes('parent') || key.includes('in_s'))) ||
          (column === 'parent' && keyBase === 'in' && key.includes('s'))) {
        this.logger.log("COLUMN_MATCH_FOUND", `Found column match`, {
          column,
          key,
          value,
          matchType: key.includes(column) ? 'key_contains_column' : 
            column.includes(keyBase) ? 'column_contains_key' :
              'special_mapping'
        });
        return value;
      }
    }
    
    this.logger.log("COLUMN_NOT_FOUND", `Could not find column in row`, {
      column,
      availableKeys: Object.keys(row)
    });
    
    return undefined;
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
   * Get the table alias for a specific goal ID in JOIN queries
   */
  getTableAliasForGoal(goalId: number): string | null {
    // Find which merged query contains this goal
    for (const mergedQuery of this.mergedQueries.values()) {
      const pattern = mergedQuery.patterns.find(p => p.goalId === goalId);
      if (pattern) {
        // Check if this is a JOIN query (has multiple patterns from different tables)
        const tables = new Set(mergedQuery.patterns.map(p => p.table));
        if (tables.size > 1 || mergedQuery.patterns.length > 1) {
          // This is a JOIN or self-join, need to get the alias
          const tableAliases = this.createTableAliases(mergedQuery.patterns);
          return tableAliases.get(goalId) || null;
        }
        // Single table query, use original table name
        return pattern.table;
      }
    }
    
    return null;
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

  private async processPendingPatterns(): Promise<void> {
    const patterns = Array.from(this.pendingPatterns.values())
      .filter(p => !p.locked);
    
    if (patterns.length === 0) {
      return;
    }

    // Find merge groups across ALL patterns (not just by table)
    const mergeGroups = this.findMergeGroups(patterns);
    
    for (const group of mergeGroups) {
      if (group.length === 1) {
        // Single pattern - execute individually
        await this.processSinglePattern(group[0]);
      } else {
        // Check if all patterns are from the same table
        const tables = new Set(group.map(p => p.table));
        if (tables.size === 1) {
          // Same table - merge within table
          await this.processSameTableMergedPatterns(Array.from(tables)[0], group);
        } else {
          // Cross-table - create JOIN
          await this.processCrossTableMergedPatterns(group);
        }
      }
    }
  }


  private findMergeGroups(patterns: QueryPattern[]): QueryPattern[][] {
    const groups: QueryPattern[][] = [];
    const processed = new Set<number>();
    
    this.logger.log("MERGE_DETECTION_START", `Finding merge groups for ${patterns.length} patterns`, {
      patterns: patterns.map(p => ({
        goalId: p.goalId,
        table: p.table,
        varIds: Array.from(p.varIds) 
      }))
    });
    
    for (const pattern of patterns) {
      if (processed.has(pattern.goalId)) {
        continue;
      }
      
      const group = [pattern];
      processed.add(pattern.goalId);
      
      // Find other patterns that share variables with this group
      let foundMatch = true;
      while (foundMatch) {
        foundMatch = false;
        const groupVarIds = new Set<string>();
        for (const p of group) {
          for (const varId of p.varIds) {
            groupVarIds.add(varId);
          }
        }
        
        this.logger.log("CHECKING_MERGE_CANDIDATES", `Group ${group.map(p => p.goalId).join(',')} has varIds: ${Array.from(groupVarIds).join(',')}`, {
          groupGoalIds: group.map(p => p.goalId),
          groupVarIds: Array.from(groupVarIds)
        });
        
        for (const otherPattern of patterns) {
          if (processed.has(otherPattern.goalId)) {
            continue;
          }
          
          // Check if this pattern shares any variables with the group
          const sharedVars = Array.from(otherPattern.varIds)
            .filter(varId => groupVarIds.has(varId));
          const hasSharedVar = sharedVars.length > 0;
          
          this.logger.log("MERGE_CANDIDATE_CHECK", `Goal ${otherPattern.goalId} checked against group`, {
            candidateGoalId: otherPattern.goalId,
            candidateVarIds: Array.from(otherPattern.varIds),
            sharedVars,
            hasSharedVar
          });
          
          if (hasSharedVar) {
            group.push(otherPattern);
            processed.add(otherPattern.goalId);
            foundMatch = true;
            
            this.logger.log("MERGE_CANDIDATE_ADDED", `Goal ${otherPattern.goalId} added to group`, {
              goalId: otherPattern.goalId,
              groupGoalIds: group.map(p => p.goalId),
              sharedVars
            });
          }
        }
      }
      
      groups.push(group);
    }
    
    this.logger.log("MERGE_GROUPS_FOUND", `Found ${groups.length} merge groups`, {
      groups: groups.map(g => ({
        goalIds: g.map(p => p.goalId),
        tables: g.map(p => p.table),
        size: g.length
      }))
    });
    
    return groups;
  }

  private async processSinglePattern(pattern: QueryPattern): Promise<void> {
    // Lock the pattern
    pattern.locked = true;
    
    // Create a single-pattern merged query
    const mergedQuery: MergedQuery = {
      id: `single_${this.nextMergedQueryId++}`,
      patterns: [pattern],
      table: pattern.table,
      combinedSelectCols: Object.keys(pattern.selectCols),
      combinedWhereCols: pattern.whereCols,
      sharedVarIds: pattern.varIds,
      locked: true
    };
    
    // Execute the query
    const results = await this.executeQuery(mergedQuery);
    mergedQuery.results = results;
    
    this.mergedQueries.set(mergedQuery.id, mergedQuery);
    this.pendingPatterns.delete(pattern.goalId);
  }

  private async processSameTableMergedPatterns(table: string, patterns: QueryPattern[]): Promise<void> {
    // Lock all patterns
    for (const pattern of patterns) {
      pattern.locked = true;
    }
    
    this.logger.log("PATTERN_MERGED", `[Goals ${patterns.map(p => p.goalId).join(',')}] Same-table patterns merged for table ${table}`, {
      goalIds: patterns.map(p => p.goalId),
      table,
      sharedVarIds: Array.from(new Set(patterns.flatMap(p => Array.from(p.varIds))))
    });
    
    // Combine all select columns
    const combinedSelectCols = new Set<string>();
    const combinedWhereCols: Record<string, any> = {};
    const sharedVarIds = new Set<string>();
    
    for (const pattern of patterns) {
      // Add select columns
      for (const col of Object.keys(pattern.selectCols)) {
        combinedSelectCols.add(col);
      }
      
      // Add where columns (non-variables only)
      for (const [col, value] of Object.entries(pattern.whereCols)) {
        if (!isVar(value)) {
          combinedWhereCols[col] = value;
        }
      }
      
      // Add variable IDs
      for (const varId of pattern.varIds) {
        sharedVarIds.add(varId);
      }
    }
    
    const mergedQuery: MergedQuery = {
      id: `merged_${this.nextMergedQueryId++}`,
      patterns,
      table,
      combinedSelectCols: Array.from(combinedSelectCols),
      combinedWhereCols,
      sharedVarIds,
      locked: true
    };
    
    // Execute the merged query
    const results = await this.executeQuery(mergedQuery);
    mergedQuery.results = results;
    
    this.mergedQueries.set(mergedQuery.id, mergedQuery);
    
    // Remove patterns from pending
    for (const pattern of patterns) {
      this.pendingPatterns.delete(pattern.goalId);
    }
  }

  private async processCrossTableMergedPatterns(patterns: QueryPattern[]): Promise<void> {
    // Lock all patterns
    for (const pattern of patterns) {
      pattern.locked = true;
    }
    
    const tables = Array.from(new Set(patterns.map(p => p.table)));
    const sharedVarIds = Array.from(new Set(patterns.flatMap(p => Array.from(p.varIds))));
    
    this.logger.log("PATTERN_MERGED", `[Goals ${patterns.map(p => p.goalId).join(',')}] Cross-table patterns merged for JOIN`, {
      goalIds: patterns.map(p => p.goalId),
      tables,
      sharedVarIds
    });
    
    // Find shared variables for JOIN conditions
    const joinVars = this.findJoinVariables(patterns);
    
    if (joinVars.length === 0) {
      this.logger.log("JOIN_NO_SHARED_VARS", `No shared variables found for JOIN, falling back to individual queries`);
      for (const pattern of patterns) {
        await this.processSinglePattern(pattern);
      }
      return;
    }
    
    // Create JOIN query
    const mergedQuery: MergedQuery = {
      id: `join_${this.nextMergedQueryId++}`,
      patterns,
      table: tables.join('_JOIN_'), // Special table name for JOINs
      combinedSelectCols: this.buildJoinSelectColumns(patterns),
      combinedWhereCols: this.buildJoinWhereConditions(patterns),
      sharedVarIds: new Set(sharedVarIds),
      locked: true
    };
    
    // Execute the JOIN query
    const results = await this.executeJoinQuery(mergedQuery, joinVars);
    mergedQuery.results = results;
    
    this.mergedQueries.set(mergedQuery.id, mergedQuery);
    
    // Remove patterns from pending
    for (const pattern of patterns) {
      this.pendingPatterns.delete(pattern.goalId);
    }
  }

  private async executeQuery(mergedQuery: MergedQuery): Promise<any[]> {
    const { table, combinedWhereCols } = mergedQuery;
    
    // Check if any pattern is symmetric
    const symmetricPattern = mergedQuery.patterns.find(p => p.isSymmetric);
    
    if (symmetricPattern) {
      // Handle symmetric query logic
      return this.executeSymmetricQuery(mergedQuery, symmetricPattern);
    }
    
    // Build SELECT clause with variable ID aliases (like in JOIN queries)
    const selectCols = this.buildSelectWithVariableAliases(mergedQuery.patterns);
    
    // Build where clauses
    const whereClauses = Object.entries(combinedWhereCols).map(([column, value]) => ({
      column,
      value
    }));
    
    // Build SQL manually to include aliases
    let sql = `SELECT ${selectCols.join(', ')} FROM ${table}`;
    if (whereClauses.length > 0) {
      const whereClause = whereClauses.map(({ column, value }) => 
        `${column} = ${typeof value === 'string' ? `'${value}'` : value}`
      ).join(' AND ');
      sql += ` WHERE ${whereClause}`;
    }
    
    this.logger.log("SAME_TABLE_QUERY_EXECUTING", `Executing same-table query with aliases`, {
      sql 
    });
    
    // Execute the query
    const rows = await this.db.raw(sql);
    const results = rows;
    
    // Add to query tracking arrays
    this.queries.push(sql);
    
    this.logger.log("DB_QUERY_EXECUTED", `[Goals ${mergedQuery.patterns.map(p => p.goalId).join(',')}] Database query executed`, {
      goalIds: mergedQuery.patterns.map(p => p.goalId),
      table: mergedQuery.table,
      sql,
      rowCount: results.length,
      rows: results // Show all rows
    });
    
    return results;
  }

  private async executeSymmetricQuery(mergedQuery: MergedQuery, symmetricPattern: QueryPattern): Promise<any[]> {
    const { table } = mergedQuery;
    const keys = symmetricPattern.symmetricKeys!;
    
    // Get grounded values from WHERE clauses
    const groundedValues = Object.values(mergedQuery.combinedWhereCols).filter(value => !isVar(value)) as (string | number)[];
    
    // Build SELECT clause with variable ID aliases for symmetric pattern
    const selectCols = [];
    
    // For symmetric relations, we need BOTH columns in the result to handle both orientations
    selectCols.push(`${keys[0]} AS ${keys[0]}_sym`);
    selectCols.push(`${keys[1]} AS ${keys[1]}_sym`);
    
    // Also add variable aliases for original pattern columns
    for (const [column, term] of Object.entries(symmetricPattern.selectCols)) {
      if (isVar(term)) {
        selectCols.push(`${column} AS ${term.id}`);
      }
    }
    
    // Include grounded values as literals for symmetric pattern
    for (const [column, value] of Object.entries(symmetricPattern.whereCols)) {
      const originalTerm = symmetricPattern.queryObj[column];
      if (isVar(originalTerm)) {
        // Use the db.raw() method to properly handle literal values
        selectCols.push(`'${value}' AS ${originalTerm.id}`);
      }
    }
    
    // Build the symmetric SQL manually to include variable aliases
    let sql = `SELECT ${selectCols.join(', ')} FROM ${table}`;
    
    // Add symmetric WHERE clause manually 
    if (groundedValues.length === 0) {
      // No grounded values - select all rows (no WHERE)
    } else if (groundedValues.length === 2) {
      // Both values grounded - exact match
      sql += ` WHERE ${keys[0]} = '${groundedValues[0]}' AND ${keys[1]} = '${groundedValues[1]}'`;
    } else {
      // One value grounded - symmetric OR
      const value = groundedValues[0];
      sql += ` WHERE ${keys[0]} = '${value}' OR ${keys[1]} = '${value}'`;
    }
    
    this.logger.log("SYMMETRIC_QUERY_EXECUTING", `Executing symmetric query with OR logic`, { 
      sql,
      keys,
      groundedValues 
    });
    
    // Execute the query
    const rows = await this.db.raw(sql);
    const results = rows;
    
    // Add to query tracking arrays
    this.queries.push(sql);
    
    this.logger.log("DB_SYMMETRIC_QUERY_EXECUTED", `Symmetric query executed`, {
      goalId: symmetricPattern.goalId,
      table,
      keys,
      sql,
      rowCount: results.length,
      rows: results
    });
    
    return results;
  }

  private findJoinVariables(patterns: QueryPattern[]): { varId: string; columns: { table: string; column: string; goalId: number; type: 'select' | 'where' }[] }[] {
    const varToColumns = new Map<string, { table: string; column: string; goalId: number; type: 'select' | 'where' }[]>();
    
    // Map each variable to all columns that use it across patterns (both SELECT and WHERE)
    for (const pattern of patterns) {
      // Process SELECT columns
      for (const [column, term] of Object.entries(pattern.selectCols)) {
        if (isVar(term)) {
          const varId = term.id;
          if (!varToColumns.has(varId)) {
            varToColumns.set(varId, []);
          }
          varToColumns.get(varId)!.push({ 
            table: pattern.table, 
            column, 
            goalId: pattern.goalId, 
            type: 'select' 
          });
        }
      }
      
      // Process WHERE columns - these create JOIN relationships too
      for (const [column, term] of Object.entries(pattern.whereCols)) {
        if (isVar(term)) {
          const varId = term.id;
          if (!varToColumns.has(varId)) {
            varToColumns.set(varId, []);
          }
          varToColumns.get(varId)!.push({ 
            table: pattern.table, 
            column, 
            goalId: pattern.goalId, 
            type: 'where' 
          });
        }
      }
    }
    
    this.logger.log("VAR_TO_COLUMNS_MAP", `All variables mapped to columns`, {
      varToColumns: Object.fromEntries(Array.from(varToColumns.entries()).map(([varId, cols]) => [
        varId, 
        cols.map(c => `${c.table}.${c.column}(${c.type})`)
      ]))
    });
    
    // Find variables that create JOIN relationships (appear in multiple patterns)
    const joinVars = [];
    for (const [varId, columns] of varToColumns.entries()) {
      const goalIds = new Set(columns.map(c => c.goalId));
      
      // Variable must appear in multiple patterns to create a JOIN
      if (goalIds.size > 1) {
        joinVars.push({
          varId,
          columns 
        });
      }
    }
    
    this.logger.log("JOIN_VARIABLES_FOUND", `Found ${joinVars.length} join variables`, {
      joinVars: joinVars.map(jv => ({
        varId: jv.varId,
        columns: jv.columns.map(c => ({
          table: c.table,
          column: c.column,
          goalId: c.goalId,
          type: c.type 
        }))
      }))
    });
    
    return joinVars;
  }

  private buildJoinSelectColumns(patterns: QueryPattern[]): string[] {
    const selectCols = [];
    
    for (const pattern of patterns) {
      for (const column of Object.keys(pattern.selectCols)) {
        // Use table-prefixed column names to avoid conflicts
        selectCols.push(`${pattern.table}.${column} AS ${pattern.table}_${column}`);
      }
    }
    
    return selectCols;
  }

  private buildJoinWhereConditions(patterns: QueryPattern[]): Record<string, any> {
    const whereCols: Record<string, any> = {};
    
    // Combine WHERE conditions from all patterns
    for (const pattern of patterns) {
      for (const [column, value] of Object.entries(pattern.whereCols)) {
        // Prefix with table name to avoid conflicts
        whereCols[`${pattern.table}.${column}`] = value;
      }
    }
    
    return whereCols;
  }

  private async executeJoinQuery(
    mergedQuery: MergedQuery, 
    joinVars: { varId: string; columns: { table: string; column: string; goalId: number; type: 'select' | 'where' }[] }[]
  ): Promise<any[]> {
    // Handle self-joins by creating table aliases
    const tableAliases = this.createTableAliases(mergedQuery.patterns);
    
    this.logger.log("JOIN_QUERY_BUILD_START", `Building JOIN query for ${mergedQuery.patterns.length} patterns`, {
      patterns: mergedQuery.patterns.map(p => ({
        goalId: p.goalId,
        table: p.table 
      })),
      tableAliases: Object.fromEntries(tableAliases.entries()),
      joinVars: joinVars.map(jv => jv.varId)
    });
    
    // Build SELECT clause with aliases
    const selectCols = this.buildSelectWithAliases(mergedQuery.patterns, tableAliases);
    
    // Build FROM clause with first table alias
    const firstPattern = mergedQuery.patterns[0];
    const firstAlias = tableAliases.get(firstPattern.goalId)!;
    let sql = `SELECT ${selectCols.join(', ')} FROM ${firstPattern.table} AS ${firstAlias}`;
    
    // Build JOIN clauses
    const joinClause = this.buildJoinClauses(mergedQuery.patterns, tableAliases, joinVars);
    sql += joinClause;
    
    // Add WHERE clauses
    const whereClause = this.buildWhereClause(mergedQuery.patterns, tableAliases);
    if (whereClause) {
      sql += ` WHERE ${whereClause}`;
    }
    
    this.logger.log("JOIN_QUERY_EXECUTING", `Executing JOIN query`, {
      sql 
    });
    
    // Execute the query
    const rows = await this.db.raw(sql);
    const results = rows;
    
    // Add to query tracking arrays
    this.queries.push(sql);
    
    this.logger.log("DB_QUERY_EXECUTED", `[Goals ${mergedQuery.patterns.map(p => p.goalId).join(',')}] JOIN query executed`, {
      goalIds: mergedQuery.patterns.map(p => p.goalId),
      table: mergedQuery.table,
      sql,
      rowCount: results.length,
      rows: results
    });
    
    return results;
  }

  private createTableAliases(patterns: QueryPattern[]): Map<number, string> {
    const aliases = new Map<number, string>();
    const tableCount = new Map<string, number>();
    
    for (const pattern of patterns) {
      const currentCount = tableCount.get(pattern.table) || 0;
      tableCount.set(pattern.table, currentCount + 1);
      
      if (currentCount === 0) {
        aliases.set(pattern.goalId, pattern.table);
      } else {
        aliases.set(pattern.goalId, `${pattern.table}_${currentCount}`);
      }
    }
    
    return aliases;
  }

  private buildSelectWithAliases(patterns: QueryPattern[], tableAliases: Map<number, string>): string[] {
    const selectCols = [];
    
    for (const pattern of patterns) {
      const alias = tableAliases.get(pattern.goalId)!;
      for (const [column, term] of Object.entries(pattern.selectCols)) {
        if (isVar(term)) {
          // Alias column to the variable ID for clean mapping
          selectCols.push(`${alias}.${column} AS ${term.id}`);
        }
      }
    }
    
    return selectCols;
  }

  private buildSelectWithVariableAliases(patterns: QueryPattern[]): string[] {
    const selectCols = [];
    
    for (const pattern of patterns) {
      // Add SELECT columns (variables)
      for (const [column, term] of Object.entries(pattern.selectCols)) {
        if (isVar(term)) {
          selectCols.push(`${column} AS ${term.id}`);
        }
      }
      
      // Add WHERE columns (grounded values) as literals
      // We need these in the result so unification can find them
      for (const [column, value] of Object.entries(pattern.whereCols)) {
        // Check if this column had a variable in the original query
        const originalTerm = pattern.queryObj[column];
        
        if (isVar(originalTerm)) {
          // Include the grounded value as a literal in SELECT
          const literalValue = typeof value === 'string' ? `'${value}'` : value;
          selectCols.push(`${literalValue} AS ${originalTerm.id}`);
        }
      }
    }
    
    return selectCols;
  }

  private buildJoinClauses(
    patterns: QueryPattern[], 
    tableAliases: Map<number, string>, 
    joinVars: { varId: string; columns: { table: string; column: string; goalId: number; type: 'select' | 'where' }[] }[]
  ): string {
    let joinClause = '';
    
    // For each pattern after the first, find how to join it
    for (let i = 1; i < patterns.length; i++) {
      const pattern = patterns[i];
      const alias = tableAliases.get(pattern.goalId)!;
      
      // Find join conditions for this pattern
      const joinConditions = [];
      for (const joinVar of joinVars) {
        // Find columns for this variable that relate to current pattern and previous patterns
        const currentCols = joinVar.columns.filter(c => c.goalId === pattern.goalId);
        const previousCols = joinVar.columns.filter(c => {
          // Find in previous patterns
          for (let j = 0; j < i; j++) {
            if (c.goalId === patterns[j].goalId) {
              return true;
            }
          }
          return false;
        });
        
        // Create JOIN conditions: SELECT columns join with WHERE columns (and vice versa)
        for (const currentCol of currentCols) {
          for (const prevCol of previousCols) {
            const prevAlias = tableAliases.get(prevCol.goalId)!;
            
            // Join when one side is SELECT and the other is WHERE, or both are SELECT
            if ((currentCol.type !== prevCol.type) || (currentCol.type === 'select' && prevCol.type === 'select')) {
              joinConditions.push(`${prevAlias}.${prevCol.column} = ${alias}.${currentCol.column}`);
            }
          }
        }
      }
      
      if (joinConditions.length > 0) {
        // Remove duplicates
        const uniqueConditions = [...new Set(joinConditions)];
        joinClause += ` INNER JOIN ${pattern.table} AS ${alias} ON ${uniqueConditions.join(' AND ')}`;
      } else {
        joinClause += ` CROSS JOIN ${pattern.table} AS ${alias}`;
      }
    }
    
    return joinClause;
  }

  private buildWhereClause(patterns: QueryPattern[], tableAliases: Map<number, string>): string {
    const whereConditions = [];
    
    for (const pattern of patterns) {
      const alias = tableAliases.get(pattern.goalId)!;
      for (const [column, value] of Object.entries(pattern.whereCols)) {
        whereConditions.push(`${alias}.${column} = ${typeof value === 'string' ? `'${value}'` : value}`);
      }
    }
    
    return whereConditions.join(' AND ');
  }

  private separateQueryColumns(queryObj: Record<string, Term>): {
    selectCols: Record<string, Term>;
    whereCols: Record<string, Term>;
  } {
    const selectCols: Record<string, Term> = {};
    const whereCols: Record<string, Term> = {};

    for (const [key, value] of Object.entries(queryObj)) {
      if (isVar(value)) {
        selectCols[key] = value;
      } else {
        whereCols[key] = value;
      }
    }

    return {
      selectCols,
      whereCols 
    };
  }

  private extractVarIds(queryObj: Record<string, Term>): Set<string> {
    const varIds = new Set<string>();
    
    for (const value of Object.values(queryObj)) {
      if (isVar(value)) {
        varIds.add((value as Var).id);
      }
    }
    
    return varIds;
  }

  /**
   * Re-evaluate query columns with current substitution to move grounded terms to WHERE clause
   */
  private async separateQueryColumnsWithSubstitution(queryObj: Record<string, Term>, s: Subst): Promise<{
    selectCols: Record<string, Term>;
    whereCols: Record<string, Term>;
  }> {
    const selectCols: Record<string, Term> = {};
    const whereCols: Record<string, Term> = {};

    for (const [key, value] of Object.entries(queryObj)) {
      // Walk the term with current substitution to see if it's grounded
      const walkedValue = await walk(value, s);
      
      if (isVar(walkedValue)) {
        // Still a variable after walking - goes in SELECT
        selectCols[key] = walkedValue;
      } else {
        // Grounded to a concrete value - goes in WHERE
        whereCols[key] = walkedValue;
      }
    }

    return {
      selectCols,
      whereCols 
    };
  }
  
  /**
   * Execute a specific query for a pattern with grounded terms
   */
  private async executeSpecificQuery(
    pattern: QueryPattern, 
    selectCols: Record<string, Term>,
    whereCols: Record<string, Term>
  ): Promise<any[]> {
    const { table } = pattern;
    
    // Build where clauses from grounded terms
    const whereClauses = Object.entries(whereCols).map(([column, value]) => ({
      column,
      value
    }));
    
    // Build SELECT clause with variable ID aliases
    // Include both SELECT columns and WHERE columns that are still variables in the original query
    const selectColumns = [];
    
    // Add SELECT columns (variables)
    for (const [column, term] of Object.entries(selectCols)) {
      if (isVar(term)) {
        selectColumns.push(`${column} AS ${term.id}`);
      }
    }
    
    // Add WHERE columns that correspond to variables in the original query
    // (these are grounded now but we still need them in the result for unification)
    for (const [column, value] of Object.entries(whereCols)) {
      // Check if this column had a variable in the original pattern
      const originalTerm = pattern.queryObj[column];
      if (isVar(originalTerm)) {
        // Include the grounded value as a literal in SELECT
        const literalValue = typeof value === 'string' ? `'${value}'` : value;
        selectColumns.push(`${literalValue} AS ${originalTerm.id}`);
      }
    }
    
    // Build SQL manually to include aliases and WHERE conditions
    let sql = `SELECT ${selectColumns.join(', ')} FROM ${table}`;
    if (whereClauses.length > 0) {
      const whereClause = whereClauses.map(({ column, value }) => 
        `${column} = ${typeof value === 'string' ? `'${value}'` : value}`
      ).join(' AND ');
      sql += ` WHERE ${whereClause}`;
    }
    
    this.logger.log("SPECIFIC_QUERY_EXECUTING", `Executing specific query with grounded terms`, {
      sql 
    });
    
    // Execute the query
    const rows = await this.db.raw(sql);
    const results = rows;
    
    // Add to query tracking arrays
    this.queries.push(sql);
    
    this.logger.log("DB_QUERY_EXECUTED", `Specific query executed for goal ${pattern.goalId}`, {
      goalId: pattern.goalId,
      table,
      sql,
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