import type { Term, Subst, Goal } from "../core/types.ts";
import { unify, isVar, walk } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";
import type { QueryMerger } from "./query-merger.ts";
import { patternUtils } from "./utils.ts";
import { RelationOptions } from "./types.ts";

export class RegularRelationWithMerger {
  
  constructor(
    private table: string,
    private logger: Logger,
    private queryMerger: QueryMerger,
    options?: RelationOptions,
  ) {
    this.logger.log("RELATION_CREATED", `Created RegularRelationWithMerger for table: ${table}`);
  }

  createGoal(queryObj: Record<string, Term>): Goal {
    const baseGoalId = this.queryMerger.getNextGoalId();
    
    this.logger.log("GOAL_CREATED", `[Goal ${baseGoalId}] Logic query created for table ${this.table}`, {
      goalId: baseGoalId,
      table: this.table,
      queryObj: JSON.stringify(queryObj, (key, value) => {
        if (typeof value === 'object' && value?.tag === 'var') {
          return `VAR(${value.id})`;
        }
        return value;
      })
    });
    
    // DON'T add pattern to merger yet - wait until execution time to check grounding

    return async function* factsSql(this: RegularRelationWithMerger, s: Subst) {
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
      
      this.logger.log("GOAL_EXECUTION_START", `[Goal ${baseGoalId}] Execution ${executionId} with walked terms`, {
        goalId: baseGoalId,
        executionId,
        originalQuery: queryObj,
        walkedQuery,
        selectCols: Object.keys(selectCols),
        whereCols: Object.keys(whereCols),
        hasWhereConditions: Object.keys(whereCols).length > 0
      });
      
      // STEP 2: (Cache check removed)
      
      // STEP 3: Execute new query with current grounding information
      // Add pattern to merger with original query structure but current grounding info
      await this.queryMerger.addPatternWithGrounding(executionId, this.table, queryObj, selectCols, whereCols);
      
      // Pattern processing is now synchronous - no need to wait

      // Get results from query merger
      let mergedResults = await this.queryMerger.getResultsForGoal(executionId);
      
      if (!mergedResults) {
        // Goal may have been cleaned up - re-add pattern and try again
        this.logger.log("GOAL_RESTARTING", `[Goal ${baseGoalId}] Execution ${executionId} restarting (likely cleaned up)`);
        await this.queryMerger.addPatternWithGrounding(executionId, this.table, queryObj, selectCols, whereCols);
        mergedResults = await this.queryMerger.getResultsForGoal(executionId);
        
        if (!mergedResults) {
          this.logger.log("GOAL_NO_RESULTS", `[Goal ${baseGoalId}] Execution ${executionId} no results after restart`);
          return;
        }
      }

      // Process results and yield matching substitutions
      const processedRows = await this.queryMerger.getProcessedRowsForGoal(executionId);
      
      this.logger.log("PROCESSING_RESULTS", `[Goal ${baseGoalId}] Execution ${executionId} processing ${mergedResults.length} rows`, {
        goalId: baseGoalId,
        executionId,
        totalRows: mergedResults.length,
        sampleRow: mergedResults[0],
        queryObj
      });
      
      let yielded = 0;
      for (let i = 0; i < mergedResults.length; i++) {
        // Skip rows that have already been processed by this goal
        if (processedRows.has(i)) {
          continue;
        }
        
        const row = mergedResults[i];
        const unifiedSubst = await this.unifyRowWithQuery(row, queryObj, s);
        if (unifiedSubst) {
          // Mark this row as processed for this goal
          this.queryMerger.markRowProcessedForGoal(executionId, i);
          
          yielded++;
          this.logger.log("SUBST_YIELDED", `[Goal ${baseGoalId}] Execution ${executionId} yielded substitution`, {
            goalId: baseGoalId,
            executionId,
            row,
            substitution: Object.fromEntries(unifiedSubst.entries())
          });
          yield unifiedSubst;
        } else {
          this.logger.log("UNIFICATION_FAILED", `[Goal ${baseGoalId}] Execution ${executionId} unification failed`, {
            goalId: baseGoalId,
            executionId,
            row,
            queryObj
          });
        }
      }
      
      this.logger.log("GOAL_EXECUTION_FINISHED", `[Goal ${baseGoalId}] Execution ${executionId} finished, yielded ${yielded} results`, {
        goalId: baseGoalId,
        executionId,
        yieldedCount: yielded
      });
    }.bind(this);
  }

  private async unifyRowWithQuery(
    row: any,
    queryObj: Record<string, Term>,
    s: Subst
  ): Promise<Subst | null> {
    // Optimized unification - directly match variable IDs with row values
    let resultSubst = s;
    
    for (const [col, term] of Object.entries(queryObj)) {
      if (isVar(term)) {
        // Direct lookup using variable ID (columns are aliased to variable IDs in SQL)
        const rowValue = row[term.id];
        
        if (rowValue === undefined) {
          return null; // Variable not found in result set
        }
        
        const unified = await unify(term, rowValue, resultSubst);
        if (!unified) {
          return null; // Unification failed
        }
        resultSubst = unified;
      }
    }
    
    return resultSubst;
  }

}