import type { Goal, Term, Subst } from "../core/types.ts";
import { unify, isVar, walk } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";
import { QueryMerger } from "./query-merger.ts";
import { RelationOptions } from "./types.ts";

export class SymmetricRelationWithMerger {
  
  constructor(
    private table: string,
    private keys: [string, string],
    private logger: Logger,
    private queryMerger: QueryMerger,
    private options?: RelationOptions,
  ) {
    this.logger.log("SYMMETRIC_RELATION_CREATED", `Created SymmetricRelationWithMerger for table: ${table} with keys: ${keys.join(', ')}`);
  }

  createGoal(queryObj: Record<string, Term>): Goal {
    const baseGoalId = this.queryMerger.getNextGoalId();
    
    // Validate that we only have the two symmetric keys
    const queryKeys = Object.keys(queryObj);
    if (queryKeys.length !== 2 || !queryKeys.every(key => this.keys.includes(key))) {
      throw new Error(`Symmetric relation ${this.table} must query exactly the two keys: ${this.keys.join(', ')}`);
    }
    
    this.logger.log("SYMMETRIC_GOAL_CREATED", `[Goal ${baseGoalId}] Symmetric logic query created for table ${this.table}`, {
      goalId: baseGoalId,
      table: this.table,
      keys: this.keys,
      queryObj: JSON.stringify(queryObj, (key, value) => {
        if (typeof value === 'object' && value?.tag === 'var') {
          return `VAR(${value.id})`;
        }
        return value;
      })
    });

    return async function* symmetricFactsSql(this: SymmetricRelationWithMerger, s: Subst) {
      // Generate unique execution ID for this specific execution of the goal  
      const executionId = this.queryMerger.getNextGoalId();
      
      // STEP 1: Walk all variables in queryObj to see what's actually grounded
      const walkedQuery: Record<string, Term> = {};
      const selectCols: Record<string, Term> = {};
      const whereCols: Record<string, Term> = {};
      
      for (const [column, term] of Object.entries(queryObj)) {
        const walkedTerm = await walk(term, s);
        walkedQuery[column] = walkedTerm;
        
        if (isVar(walkedTerm)) {
          selectCols[column] = walkedTerm;
        } else {
          whereCols[column] = walkedTerm;
        }
      }
      
      this.logger.log("SYMMETRIC_EXECUTION_START", `[Goal ${baseGoalId}] Execution ${executionId} with walked terms`, {
        goalId: baseGoalId,
        executionId,
        originalQuery: queryObj,
        walkedQuery,
        selectCols: Object.keys(selectCols),
        whereCols: Object.keys(whereCols),
        hasWhereConditions: Object.keys(whereCols).length > 0
      });
      
      // STEP 2: Handle symmetric relation logic
      // Check for same-value case (a person can't be related to themselves in most cases)
      const values = Object.values(walkedQuery);
      if (values.length === 2 && !isVar(values[0]) && !isVar(values[1]) && values[0] === values[1]) {
        this.logger.log("SYMMETRIC_SELF_RELATION_SKIPPED", `[Goal ${baseGoalId}] Execution ${executionId} skipped (self-relation)`, {
          goalId: baseGoalId,
          executionId,
          value: values[0]
        });
        return; // Don't yield anything for self-relations
      }
      
      // STEP 3: Check if we can reuse existing cached results from query merger
      const existingResults = await this.queryMerger.checkForExistingResults(this.table, walkedQuery, queryObj);
      if (existingResults) {
        this.logger.log("SYMMETRIC_REUSING_CACHED_RESULTS", `[Goal ${baseGoalId}] Execution ${executionId} reusing cached results`, {
          goalId: baseGoalId,
          executionId,
          cachedRowCount: existingResults.length
        });
        
        let yielded = 0;
        for (const row of existingResults) {
          const unifiedSubst = await this.unifyRowWithSymmetricQuery(row, queryObj, s);
          if (unifiedSubst) {
            yielded++;
            this.logger.log("SYMMETRIC_SUBST_YIELDED", `[Goal ${baseGoalId}] Execution ${executionId} yielded substitution from cache`, {
              goalId: baseGoalId,
              executionId,
              row,
              substitution: Object.fromEntries(unifiedSubst.entries())
            });
            yield unifiedSubst;
          }
        }
        
        this.logger.log("SYMMETRIC_GOAL_EXECUTION_FINISHED", `[Goal ${baseGoalId}] Execution ${executionId} finished using cache, yielded ${yielded} results`, {
          goalId: baseGoalId,
          executionId,
          yieldedCount: yielded
        });
        return;
      }
      
      // STEP 4: Execute new symmetric query with current grounding information
      // Convert symmetric query to a regular relation pattern for the query merger
      // But mark it as symmetric so the query builder knows to use OR logic
      await this.queryMerger.addSymmetricPatternWithGrounding(executionId, this.table, this.keys, queryObj, selectCols, whereCols);
      
      // Pattern processing is now synchronous - no need to wait

      // Get results from query merger
      let mergedResults = await this.queryMerger.getResultsForGoal(executionId, s);
      
      if (!mergedResults) {
        // Goal may have been cleaned up - re-add pattern and try again
        this.logger.log("SYMMETRIC_GOAL_RESTARTING", `[Goal ${baseGoalId}] Execution ${executionId} restarting (likely cleaned up)`);
        await this.queryMerger.addSymmetricPatternWithGrounding(executionId, this.table, this.keys, queryObj, selectCols, whereCols);
        mergedResults = await this.queryMerger.getResultsForGoal(executionId, s);
        
        if (!mergedResults) {
          this.logger.log("SYMMETRIC_GOAL_NO_RESULTS", `[Goal ${baseGoalId}] Execution ${executionId} no results after restart`);
          return;
        }
      }

      // Process results and yield matching substitutions with symmetric logic
      const processedRows = await this.queryMerger.getProcessedRowsForGoal(executionId);
      
      let yielded = 0;
      for (let i = 0; i < mergedResults.length; i++) {
        // Skip rows that have already been processed by this goal
        if (processedRows.has(i)) {
          continue;
        }
        
        const row = mergedResults[i];
        const unifiedSubst = await this.unifyRowWithSymmetricQuery(row, queryObj, s);
        if (unifiedSubst) {
          // Mark this row as processed for this goal
          this.queryMerger.markRowProcessedForGoal(executionId, i);
          
          yielded++;
          this.logger.log("SYMMETRIC_SUBST_YIELDED", `[Goal ${baseGoalId}] Execution ${executionId} yielded substitution`, {
            goalId: baseGoalId,
            executionId,
            row,
            substitution: Object.fromEntries(unifiedSubst.entries())
          });
          yield unifiedSubst;
        }
      }
      
      this.logger.log("SYMMETRIC_GOAL_EXECUTION_FINISHED", `[Goal ${baseGoalId}] Execution ${executionId} finished, yielded ${yielded} results`, {
        goalId: baseGoalId,
        executionId,
        yieldedCount: yielded
      });
    }.bind(this);
  }

  /**
   * Unify a database row with a symmetric query, trying both orientations
   */
  private async unifyRowWithSymmetricQuery(
    row: any,
    queryObj: Record<string, Term>,
    s: Subst
  ): Promise<Subst | null> {
    const [key1, key2] = this.keys;
    const queryValues = [queryObj[key1], queryObj[key2]];
    
    // Get symmetric column values from the row (added by the query builder)
    const rowValues = [row[`${key1}_sym`], row[`${key2}_sym`]];
    
    // Try first orientation: query[key1] <-> row[key1], query[key2] <-> row[key2]
    let resultSubst = s;
    const unified1 = await unify(queryValues[0], rowValues[0], resultSubst);
    if (unified1) {
      const unified2 = await unify(queryValues[1], rowValues[1], unified1);
      if (unified2) {
        return unified2;
      }
    }
    
    // Try second orientation (symmetric): query[key1] <-> row[key2], query[key2] <-> row[key1]  
    resultSubst = s;
    const unified3 = await unify(queryValues[0], rowValues[1], resultSubst);
    if (unified3) {
      const unified4 = await unify(queryValues[1], rowValues[0], unified3);
      if (unified4) {
        return unified4;
      }
    }
    
    return null; // Neither orientation worked
  }
}