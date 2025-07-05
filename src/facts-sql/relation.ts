import { nextTick } from "process";
import { log } from "console";
import type { Term, Subst, Goal, Observable } from "../core/types.ts";
import { unify, isVar, walk } from "../core/kernel.ts";
import { Logger, getDefaultLogger } from "../shared/logger.ts";
import { queryUtils } from "../shared/utils.ts";
import { SimpleObservable } from "../core/observable.ts";
import { SQL_GROUP_ID, SQL_GROUP_PATH, SQL_GROUP_GOALS } from "../core/combinators.ts";
import { RelationOptions } from "./types.ts";
import type { DBManager, GoalRecord } from "./index.ts";

const ROW_CACHE = Symbol.for("sql-row-cache");

// WeakMap to link observables to their goal IDs
const observableToGoalId = new WeakMap<Observable<any>, number>();

// Global registry to track goals by group ID
const goalsByGroupId = new Map<number, Set<number>>();

// Adjustable batch size for IN queries
const BATCH_SIZE = 100;
// Adjustable debounce window for batching (ms)
const BATCH_DEBOUNCE_MS = 50;

// --- Observable/Goal Registration Utilities ---

/**
 * Register a goalId in a group.
 */
function registerGoalInGroup(
  goalsByGroupId: Map<number, Set<number>>,
  groupId: number,
  goalId: number
): void {
  if (!goalsByGroupId.has(groupId)) {
    goalsByGroupId.set(groupId, new Set());
  }
  goalsByGroupId.get(groupId)!.add(goalId);
}


// --- Batching & Debounce Utilities ---

/**
 * Create a batch processor for streaming input, with batch size and debounce window.
 * Calls flushFn(batch) when batch is full or debounce window elapses.
 * Returns a handler for input and a cancel function.
 */
function createBatchProcessor<T>(options: {
  batchSize: number,
  debounceMs: number,
  onFlush: (batch: T[]) => Promise<void> | void,
}): {
  addItem: (item: T) => void,
  complete: () => Promise<void>,
  cancel: () => void,
} {
  let batch: T[] = [];
  let debounceTimer: NodeJS.Timeout | null = null;
  let flushingPromise: Promise<void> | null = null;
  let cancelled = false;

  const clearDebounce = (): void => {
    if (debounceTimer) {
      clearTimeout(debounceTimer);
      debounceTimer = null;
    }
  };

  const flushBatch = async (): Promise<void> => {
    clearDebounce();
    if (flushingPromise) return flushingPromise;
    if (batch.length === 0 || cancelled) return Promise.resolve();
    const toFlush = batch;
    batch = [];
    flushingPromise = Promise.resolve(options.onFlush(toFlush)).finally(() => {
      flushingPromise = null;
    });
    return flushingPromise;
  };

  const addItem = (item: T): void => {
    if (cancelled) return;
    batch.push(item);
    if (shouldFlushBatch(batch, options.batchSize)) {
      flushBatch();
    } else {
      clearDebounce();
      debounceTimer = setTimeout(() => flushBatch(), options.debounceMs);
    }
  };

  const complete = async (): Promise<void> => {
    await flushBatch();
  };

  const cancel = (): void => {
    cancelled = true;
    clearDebounce();
    batch = [];
  };

  return {
    addItem,
    complete,
    cancel 
  };
}

// --- Helper for batch flush condition (task #7) ---
function shouldFlushBatch<T>(batch: T[], batchSize: number): boolean {
  return batch.length >= batchSize;
}

// --- Cache Management Utilities ---

/**
 * Get or create the ROW_CACHE map from a substitution.
 */
function getOrCreateRowCache(s: Map<string | Symbol, any>): Map<number, Record<string, any>[]> {
  if (!s.has(ROW_CACHE)) {
    s.set(ROW_CACHE, new Map());
  }
  return s.get(ROW_CACHE) as Map<number, Record<string, any>[]>;
}

/**
 * Get and remove the cache for a goalId from the substitution's ROW_CACHE.
 */
function takeCache(goalId: number, s: Subst): Record<string, any>[] | null {
  const cache = getOrCreateRowCache(s);
  if (cache.has(goalId)) {
    const rows = cache.get(goalId) as Record<string, any>[];
    cache.delete(goalId);
    return rows;
  }
  return null;
}

/**
 * Peek at the cache for a goalId in the substitution's ROW_CACHE (does not remove).
 */
function peekCache(goalId: number, s: Subst): Record<string, any>[] | null {
  const cache = getOrCreateRowCache(s);
  if (cache.has(goalId)) {
    return cache.get(goalId) as Record<string, any>[];
  }
  return null;
}

/**
 * Create a new substitution with an updated ROW_CACHE, removing a goal's cache and/or adding new caches.
 */
function createUpdatedSubst(s: Subst, goalIdToRemove: number, newCaches?: Record<number, Record<string, any>[]>): Subst {
  const newSubst = new Map(s);
  const originalCache = getOrCreateRowCache(s);
  const newCache = new Map(originalCache);
  newCache.delete(goalIdToRemove);
  if (newCaches) {
    for (const [goalId, rows] of Object.entries(newCaches)) {
      newCache.set(Number(goalId), rows);
    }
  }
  newSubst.set(ROW_CACHE, newCache);
  return newSubst;
}

/**
 * Create a new substitution with cache for other compatible goals in the same batch.
 */
function createUpdatedSubstWithCacheForOtherGoals(dbObj: DBManager, s: Subst, myGoalId: number, currentRow: any, logger: Logger): Subst {
  const newSubst = new Map(s);
  const originalCache = getOrCreateRowCache(s);
  const newCache = new Map(originalCache);
  newCache.delete(myGoalId);
  const myGoal = dbObj.getGoalById(myGoalId);
  if (myGoal) {
    const compatibleGoals = dbObj.getGoals().filter(g =>
      g.goalId !== myGoalId &&
      g.batchKey === myGoal.batchKey &&
      g.batchKey !== undefined &&
      g.table === myGoal.table
    );
    for (const otherGoal of compatibleGoals) {
      newCache.set(otherGoal.goalId, [currentRow]);
      logger.log("ADDED_CACHE_FOR_OTHER_GOAL", {
        myGoalId,
        otherGoalId: otherGoal.goalId,
        rowCount: 1,
        cachedRow: currentRow
      });
    }
  }
  newSubst.set(ROW_CACHE, newCache);
  return newSubst;
}

// --- Goal Compatibility & Merging Utilities ---

/**
 * Find goals compatible for merging: same table and at least one shared variable.
 */
function findCompatibleGoals(myGoal: GoalRecord, commonGoals: { goal: GoalRecord, matchingIds: string[] }[]): GoalRecord[] {
  return commonGoals
    .filter(g =>
      g.goal.table === myGoal.table &&
      g.matchingIds.length > 0
    )
    .map(g => g.goal);
}

/**
 * Collect all WHERE clause values from a set of goals for merging.
 * Only collect grounded values that are constants in the original goal definitions,
 * not variables that happen to be bound in the current substitution.
 */
async function collectAllWhereClauses(goals: GoalRecord[], s: Subst): Promise<Record<string, Set<any>>> {
  const allWhereClauses: Record<string, Set<any>> = {};
  for (const goal of goals) {
    // Only collect WHERE clauses from values that are already grounded constants
    // in the original goal definition, not from bound variables
    const whereCols = queryUtils.onlyGrounded(goal.queryObj);
    for (const [col, value] of Object.entries(whereCols)) {
      if (!allWhereClauses[col]) allWhereClauses[col] = new Set();
      allWhereClauses[col].add(value);
    }
  }
  return allWhereClauses;
}

// --- Query Building Utilities ---

/**
 * Build a select query for a table with given where clauses and columns.
 * whereClauses: { col: Set<any> } (values in set will be used with whereIn if >1, else where)
 * selectColumns: array of column names to select (never '*')
 */
function buildSelectQuery(dbObj: DBManager, table: string, whereClauses: Record<string, Set<any>>, selectColumns: string[]) {
  let query = dbObj.db(table);
  for (const [col, values] of Object.entries(whereClauses)) {
    if (values.size === 1) {
      query = query.where(col, Array.from(values)[0]);
    } else {
      query = query.whereIn(col, Array.from(values));
    }
  }
  query = query.select(selectColumns);
  return query;
}


export class RegularRelationWithMerger {
  private logger: Logger;
  private primaryKey?: string;

  constructor(
    private dbObj: DBManager,
    private table: string,
    logger?: Logger,
    private options?: RelationOptions,
  ) {
    this.logger = logger ?? getDefaultLogger();
    this.primaryKey = options?.primaryKey;
  }

  haveAtLeastOneMatchingVar(a: GoalRecord, b: GoalRecord) {
    const aVarIds = Object.values(queryUtils.onlyVars(a.queryObj)).map(x => x.id);
    const bVarIds = Object.values(queryUtils.onlyVars(b.queryObj)).map(x => x.id);
    const matchingIds = aVarIds.filter(av => bVarIds.includes(av));
    
    if(matchingIds.length === 0) {
      return null
    }
    return {
      goal: b,
      matchingIds,
    }
  }

  async processGoal(myGoal: GoalRecord, s: Subst) {
    // Get the group goals from the substitution (set by conj/disj)
    const groupGoals = s.get(SQL_GROUP_GOALS) as Goal[] || [];
    
    if (groupGoals.length === 0) {
      return [];
    }
    
    // Look up goal IDs for each goal function using the WeakMap
    const otherGoalIds = groupGoals
      .map(goalFn => observableToGoalId.get(goalFn as unknown as Observable<any>))
      .filter(goalId => goalId !== undefined && goalId !== myGoal.goalId) as number[];
    
    // Get the goal records
    const otherGoals = otherGoalIds
      .map(goalId => this.dbObj.getGoalById(goalId))
      .filter(goal => goal !== undefined) as GoalRecord[];
    
    this.logger.log("COMPATIBLE_GOALS", {
      myGoalId: myGoal.goalId,
      groupGoalsCount: groupGoals.length,
      foundOtherGoalIds: otherGoalIds,
      compatibleGoalIds: otherGoals.map(g => g.goalId),
      allGoals: otherGoals.map(g => ({
        goalId: g.goalId,
        table: g.table 
      }))
    });
    
    return otherGoals.map(x => this.haveAtLeastOneMatchingVar(myGoal, x)).filter(x => x !== null);
  }


  createGoal(queryObj: Record<string, Term>): Goal {
    const goalId = this.dbObj.getNextGoalId();
    this.logger.log("GOAL_CREATED", {
      goalId,
      table: this.table,
      queryObj
    });
    // Register goal immediately when the goal function is called, before any processing
    this.dbObj.addGoal(goalId, this.table, queryObj, undefined);
    this.logger.log("GOAL_REGISTERED_EARLY", {
      goalId,
      table: this.table,
      queryObj
    });
    // Streaming protocol: always accept Observable<Subst> as input
    const mySubstHandler = (input$: any) => {
      const resultObservable = new SimpleObservable<Subst>((observer) => {
        let cancelled = false;
        let batchIndex = 0;
        let input_complete = false;
        let batchKeyUpdated = false;
        this.logger.log("GOAL_STARTED", {
          goalId,
          table: this.table,
          queryObj,
        });
        // Use the batch processor utility
        const batchProcessor = createBatchProcessor<Subst>({
          batchSize: BATCH_SIZE,
          debounceMs: BATCH_DEBOUNCE_MS,
          onFlush: async (batch) => {
            if (cancelled) return;
            this.logger.log("FLUSH_BATCH", {
              goalId,
              batchIndex,
              batchSize: batch.length,
            });

            // All substitutions in this batch are cache misses
            // (cache hits are processed immediately above)
            
            this.logger.log("PROCESSING_CACHE_MISSES", {
              goalId,
              cacheMissCount: batch.length
            });
            
            const rows = await this.executeQueryForSubstitutions(goalId, queryObj, batch);
            
            // Process SQL results for all cache misses
            for (const subst of batch) {
              if (cancelled) return;
              
              for (const row of rows) {
                if (cancelled) return;
                
                const unifiedSubst = this.unifyRowWithQuery(row, queryObj, new Map(subst));
                if (unifiedSubst && !cancelled) {
                  // Cache results for other compatible goals
                  const updatedSubst = createUpdatedSubstWithCacheForOtherGoals(
                    this.dbObj, 
                    unifiedSubst, 
                    goalId, 
                    row, 
                    this.logger
                  );
                    
                  this.logger.log("UNIFY_SUCCESS", {
                    goalId,
                    queryObj,
                    row,
                    wasFromCache: false
                  });
                  observer.next(new Map(updatedSubst));
                  await new Promise(resolve => nextTick(resolve));
                }
              }
            }
            batchIndex++;
            this.logger.log("FLUSH_BATCH_COMPLETE", {
              goalId,
              batchIndex,
              batchSize: 0,
            });
          }
        });
        const subscription = input$.subscribe({
          next: async (subst: Subst) => {
            if (cancelled) return;
            if (!batchKeyUpdated) {
              const groupId = subst.get(SQL_GROUP_ID) as number | undefined;
              batchKeyUpdated = true;
              if (groupId !== undefined) {
                registerGoalInGroup(goalsByGroupId, groupId, goalId);
                this.logger.log("GOAL_GROUP_INFO", {
                  goalId,
                  table: this.table,
                  groupId,
                  registeredInGroup: groupId !== undefined,
                  queryObj
                });
              }
            }
            
            // Check cache first - if hit, process immediately without batching
            const cache = takeCache(goalId, subst);
            if (cache) {
              this.logger.log("CACHE_HIT_IMMEDIATE", {
                goalId,
                rowCount: cache.length,
                table: this.table
              });
              
              // Process cached results immediately
              for (const row of cache) {
                if (cancelled) return;
                
                const unifiedSubst = this.unifyRowWithQuery(row, queryObj, new Map(subst));
                if (unifiedSubst && !cancelled) {
                  this.logger.log("UNIFY_SUCCESS", {
                    goalId,
                    queryObj,
                    row,
                    wasFromCache: true
                  });
                  observer.next(new Map(unifiedSubst));
                  await new Promise(resolve => nextTick(resolve));
                }
              }
            } else {
              // Cache miss - add to batch for SQL processing
              batchProcessor.addItem(subst);
              this.logger.log("CACHE_MISS_TO_BATCH", {
                goalId,
                input_complete,
                subst,
              });
            }
          },
          error: (err: any) => {
            if (!cancelled) observer.error?.(err);
          },
          complete: () => {
            this.logger.log("UPSTREAM_GOAL_COMPLETE", {
              goalId,
              batchIndex,
              input_complete,
              cancelled,
            });
            input_complete = true;
            batchProcessor.complete().then(() => {
              this.logger.log("GOAL_COMPLETE", {
                goalId,
                batchIndex,
                input_complete,
                cancelled,
              });
              observer.complete?.();
            });
          }
        });
        return () => {
          this.logger.log("GOAL_CANCELLED", {
            goalId,
            batchIndex,
            input_complete,
            cancelled,
          });
          cancelled = true;
          batchProcessor.cancel();
          subscription.unsubscribe?.();
        };
      });
      return resultObservable;
    };
    // Inline registerGoalHandler here since it's only used once
    observableToGoalId.set(mySubstHandler as unknown as Observable<any>, goalId);
    return mySubstHandler;
  }

  private async executeQueryForSubstitutions(goalId: number, queryObj: Record<string, Term>, substitutions: Subst[]): Promise<any[]> {
    if (substitutions.length === 0) return [];
    
    this.logger.log("EXECUTING_UNIFIED_QUERY", {
      goalId,
      substitutionCount: substitutions.length,
      table: this.table
    });
    
    // Always check for goal merging opportunities (regardless of batch size)
    const myGoal = this.dbObj.getGoalById(goalId);
    if (myGoal && substitutions.length > 0) {
      const representativeSubst = substitutions[0];
      const commonGoals = await this.processGoal(myGoal, representativeSubst);
      const compatibleGoals = findCompatibleGoals(myGoal, commonGoals);
      
      if (compatibleGoals.length > 0) {
        this.logger.log("MERGING_COMPATIBLE_GOALS", {
          myGoalId: goalId,
          compatibleGoalIds: compatibleGoals.map(g => g.goalId),
          substitutionCount: substitutions.length
        });
        
        // Merge WHERE clauses from all compatible goals AND from all substitutions
        const allGoalsToMerge = [myGoal, ...compatibleGoals];
        
        // Collect WHERE clauses from compatible goals
        const goalWhereClauses = await collectAllWhereClauses(allGoalsToMerge, representativeSubst);
        
        // Also collect WHERE clauses from all substitutions (for batching)
        const substWhereClauses: Record<string, Set<any>> = {};
        for (const subst of substitutions) {
          const walked = await queryUtils.walkAllKeys(queryObj, subst);
          const whereCols = queryUtils.onlyGrounded(walked);
          for (const [col, value] of Object.entries(whereCols)) {
            if (!substWhereClauses[col]) substWhereClauses[col] = new Set();
            substWhereClauses[col].add(value);
          }
        }
        
        // Merge both sets of WHERE clauses
        const mergedWhereClauses: Record<string, Set<any>> = {
          ...goalWhereClauses 
        };
        for (const [col, values] of Object.entries(substWhereClauses)) {
          if (mergedWhereClauses[col]) {
            // Combine values from both sources
            for (const value of values) {
              mergedWhereClauses[col].add(value);
            }
          } else {
            mergedWhereClauses[col] = new Set(values);
          }
        }
        
        // Collect all columns needed by all goals
        const allGoalColumns = new Set<string>();
        for (const goal of allGoalsToMerge) {
          Object.keys(goal.queryObj).forEach(col => allGoalColumns.add(col));
        }
        const additionalColumns = this.options?.selectColumns || [];
        const allColumns = [...new Set([...allGoalColumns, ...additionalColumns])];
        
        // Execute merged query
        const query = buildSelectQuery(this.dbObj, this.table, mergedWhereClauses, allColumns);
        const sqlString = query.toString();
        this.dbObj.addQuery(sqlString);
        this.logger.log("DB_QUERY_MERGED", {
          table: this.table,
          sql: sqlString,
          goalId,
          mergedGoalIds: allGoalsToMerge.map(g => g.goalId),
          substitutionCount: substitutions.length
        });
        
        const rows = await query;
        if (rows.length) {
          this.logger.log("DB_ROWS_MERGED", {
            table: this.table,
            sql: sqlString,
            goalId,
            rows,
          });
        }
        
        // Cache results for all compatible goals in all substitutions
        for (const subst of substitutions) {
          const passed = getOrCreateRowCache(subst);
          for (const otherGoal of compatibleGoals) {
            if (otherGoal.goalId !== goalId && otherGoal.table === myGoal.table) {
              passed.set(otherGoal.goalId, rows);
              this.logger.log("CACHED_FOR_OTHER_GOAL", {
                myGoalId: goalId,
                otherGoalId: otherGoal.goalId,
                rowCount: rows.length
              });
            }
          }
          
          // Also populate cache for my goal in case other goals need it
          passed.set(goalId, rows);
        }
        
        return rows;
      }
    }
    
    // Fallback: regular batching without goal merging
    const allWhereClauses: Record<string, Set<any>> = {};
    for (const subst of substitutions) {
      const walked = await queryUtils.walkAllKeys(queryObj, subst);
      const whereCols = queryUtils.onlyGrounded(walked);
      for (const [col, value] of Object.entries(whereCols)) {
        if (!allWhereClauses[col]) allWhereClauses[col] = new Set();
        allWhereClauses[col].add(value);
      }
    }
    
    const requiredColumns = Object.keys(queryObj);
    const additionalColumns = this.options?.selectColumns || [];
    const allColumns = [...new Set([...requiredColumns, ...additionalColumns])];
    
    const query = buildSelectQuery(this.dbObj, this.table, allWhereClauses, allColumns);
    const sqlString = query.toString();
    this.dbObj.addQuery(sqlString);
    this.logger.log("DB_QUERY_BATCH", {
      table: this.table,
      sql: sqlString,
      goalId,
      substitutionCount: substitutions.length
    });
    
    const rows = await query;
    if (rows.length) {
      this.logger.log("DB_ROWS", {
        table: this.table,
        sql: sqlString,
        goalId,
        rows,
      });
    }
    
    return rows;
  }


  // Make unifyRowWithQuery public so it can be used by batch processor
  public unifyRowWithQuery(row: any, queryObj: Record<string, Term>, s: Subst): Subst | null {
    let result = s;
    for (const [column, term] of Object.entries(queryObj)) {
      const value = row[column];
      if (value === undefined) continue;
      const unified = unify(term, value, result);
      if (unified === null) {
        return null;
      }
      result = unified;
    }
    return result;
  }
}