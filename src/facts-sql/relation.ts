import { nextTick } from "process";
import { log } from "console";
import type { Term, Subst, Goal, Observable } from "../core/types.ts";
import { unify, isVar, walk } from "../core/kernel.ts";
import { Logger, getDefaultLogger } from "../shared/logger.ts";
import { queryUtils } from "../shared/utils.ts";
import { SimpleObservable } from "../core/observable.ts";
import { SQL_GROUP_ID, SQL_GROUP_PATH, SQL_INNER_GROUP_GOALS, SQL_OUTER_GROUP_GOALS } from "../core/combinators.ts"
import { RelationOptions } from "./types.ts";
import type { DBManager, GoalRecord } from "./index.ts";

/** DEBUGGING GOALS */
const DEBUG_GOALS = [1];

const ROW_CACHE = Symbol.for("sql-row-cache");

// WeakMap to link observables to their goal IDs
const observableToGoalId = new WeakMap<Observable<any>, number>();

// Global registry to track goals by group ID
const goalsByGroupId = new Map<number, Set<number>>();

// Removed global query cache - using improved grouping mechanism instead

// Adjustable batch size for IN queries
const BATCH_SIZE = 100;
// Adjustable debounce window for batching (ms)
const BATCH_DEBOUNCE_MS = 50;

// Removed global cache utilities - using improved grouping mechanism instead

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
 * Find goals that share the same top-level execution path and could benefit from caching.
 * This includes goals from different groups but same top-level execution context.
 */
function findGoalsForCaching(myGoal: GoalRecord, dbObj: DBManager, subst: Subst, logger: Logger): GoalRecord[] {
  const myPath = subst.get(SQL_GROUP_PATH) as any[] || [];
  const myTopLevelId = myPath.length > 0 ? myPath[0].id : null;
  
  logger.log("CROSS_GROUP_CACHE_CHECK", {
    myGoalId: myGoal.goalId,
    myPath,
    myTopLevelId
  });
  
  // Find all goals that share the same table and have compatible query structure
  // regardless of execution path (this enables cross-OR-branch caching)
  const allGoals = dbObj.getGoals();
  const compatibleGoals: GoalRecord[] = [];
  
  for (const otherGoal of allGoals) {
    if (otherGoal.goalId === myGoal.goalId || otherGoal.table !== myGoal.table) {
      continue;
    }
    
    // Check if this goal could benefit from our results (subset relationship)
    if (couldBenefitFromCache(myGoal, otherGoal)) {
      logger.log("CROSS_GROUP_CACHE_CHECK", {
        myGoalId: myGoal.goalId,
        otherGoalId: otherGoal.goalId,
        canBenefit: true,
        reason: "same-table-compatible-query"
      });
      compatibleGoals.push(otherGoal);
    }
  }
  
  return compatibleGoals;
}

/**
 * Check if otherGoal's query would be satisfied by myGoal's results.
 * This is true if otherGoal's constraints are a superset of myGoal's constraints.
 */
function couldBenefitFromCache(myGoal: GoalRecord, otherGoal: GoalRecord): boolean {
  // For now, implement a simple heuristic:
  // If both goals query the same table with the same columns, they can share cache
  const myColumns = Object.keys(myGoal.queryObj);
  const otherColumns = Object.keys(otherGoal.queryObj);
  
  // Check if they have the same query structure (same columns)
  if (myColumns.length !== otherColumns.length) {
    return false;
  }
  
  return myColumns.every(col => otherColumns.includes(col));
}

/**
 * Find goals compatible for merging: same table and transitively connected via shared variables.
 * Uses graph traversal to find all goals that are connected through variable sharing.
 */
function findCompatibleGoals(myGoal: GoalRecord, commonGoals: { goal: GoalRecord, matchingIds: string[] }[]): GoalRecord[] {
  // Filter to same table goals (don't filter by direct matches - we'll check transitive connections)
  const sameTableGoals = commonGoals.filter(g => g.goal.table === myGoal.table);
  
  if (sameTableGoals.length === 0) {
    return [];
  }
  
  // Build adjacency list for variable sharing graph
  const allGoals = [myGoal, ...sameTableGoals.map(g => g.goal)];
  const adjacencyList = new Map<number, Set<number>>();
  
  // Initialize adjacency list
  for (const goal of allGoals) {
    adjacencyList.set(goal.goalId, new Set());
  }
  
  // Helper to check if two goals share variables
  const haveSharedVars = (goalA: GoalRecord, goalB: GoalRecord): boolean => {
    const aVarIds = Object.values(queryUtils.onlyVars(goalA.queryObj)).map(x => x.id);
    const bVarIds = Object.values(queryUtils.onlyVars(goalB.queryObj)).map(x => x.id);
    return aVarIds.some(av => bVarIds.includes(av));
  };
  
  // Build edges between goals that share variables
  for (let i = 0; i < allGoals.length; i++) {
    for (let j = i + 1; j < allGoals.length; j++) {
      const goalA = allGoals[i];
      const goalB = allGoals[j];
      
      if (haveSharedVars(goalA, goalB)) {
        adjacencyList.get(goalA.goalId)!.add(goalB.goalId);
        adjacencyList.get(goalB.goalId)!.add(goalA.goalId);
      }
    }
  }
  
  // DFS to find all goals connected to myGoal
  const visited = new Set<number>();
  const connected = new Set<number>();
  
  const dfs = (goalId: number) => {
    if (visited.has(goalId)) return;
    visited.add(goalId);
    connected.add(goalId);
    
    const neighbors = adjacencyList.get(goalId) || new Set();
    for (const neighborId of neighbors) {
      dfs(neighborId);
    }
  };
  
  // Start DFS from myGoal
  dfs(myGoal.goalId);
  
  // Return all connected goals except myGoal itself
  return allGoals.filter(goal => 
    connected.has(goal.goalId) && 
    goal.goalId !== myGoal.goalId
  );
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
    // Get both inner and outer group goals from the substitution
    const innerGroupGoals = s.get(SQL_INNER_GROUP_GOALS) as Goal[] || [];
    const outerGroupGoals = s.get(SQL_OUTER_GROUP_GOALS) as Goal[] || [];
    
    // For query merging, use inner group goals (same logical group)
    // For caching, use outer group goals (cross-branch sharing)
    const goalsForCaching = outerGroupGoals.length > 0 ? outerGroupGoals : innerGroupGoals;
    
    if (goalsForCaching.length === 0) {
      return [];
    }
    
    // Look up goal IDs for each goal function using the WeakMap
    const otherGoalIds = goalsForCaching
      .map(goalFn => observableToGoalId.get(goalFn as unknown as Observable<any>))
      .filter(goalId => goalId !== undefined && goalId !== myGoal.goalId) as number[];
    
    // Get the goal records
    const otherGoals = otherGoalIds
      .map(goalId => this.dbObj.getGoalById(goalId))
      .filter(goal => goal !== undefined) as GoalRecord[];
    
    this.logger.log("COMPATIBLE_GOALS", {
      myGoalId: myGoal.goalId,
      innerGroupGoalsCount: innerGroupGoals.length,
      outerGroupGoalsCount: outerGroupGoals.length,
      usingOuterGroupForCaching: outerGroupGoals.length > 0,
      foundOtherGoalIds: otherGoalIds,
      compatibleGoalIds: otherGoals.map(g => g.goalId),
      allGoals: otherGoals.map(g => ({
        goalId: g.goalId,
        table: g.table 
      }))
    });
    
    // Return all goals with empty matchingIds - let findCompatibleGoals handle transitive matching
    return otherGoals.map(goal => ({
      goal,
      matchingIds: [] // Empty since we'll use transitive closure in findCompatibleGoals
    }));
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
            this.logger.log("GOAL_NEXT", {
              goalId,
              batchIndex,
              input_complete,
            });
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
                  // Preserve cache entries for other goals when processing cache hits
                  const originalCache = getOrCreateRowCache(subst);
                  const preservedCache = new Map(originalCache);
                  preservedCache.delete(goalId); // Only remove our own cache
                  unifiedSubst.set(ROW_CACHE, preservedCache);

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
    const betterFnName = `SQL_${this.table}_${goalId}`;
    mySubstHandler.displayName = betterFnName;
    // Register the observable with its goalId
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
    
    // Removed global cache check - using improved grouping mechanism instead
    
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
        this.dbObj.addQuery(`${goalId} - ${allGoalsToMerge.map(g => g.goalId)} - ${sqlString}`);
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
        
        // Removed global cache storage - using improved grouping mechanism instead
        
        // Cache results for all goals in outer group that query the same table
        for (const subst of substitutions) {
          const passed = getOrCreateRowCache(subst);
          
          // Get outer group goals for cross-branch caching
          const outerGroupGoals = subst.get(SQL_OUTER_GROUP_GOALS) as Goal[] || [];
          
          // Find all outer group goal IDs that query the same table
          const outerGroupGoalIds = outerGroupGoals
            .map(goalFn => observableToGoalId.get(goalFn as unknown as Observable<any>))
            .filter(goalId => goalId !== undefined) as number[];
          
          const sameTableGoalsInOuterGroup = outerGroupGoalIds
            .map(goalId => this.dbObj.getGoalById(goalId))
            .filter(goal => goal !== undefined && goal.table === this.table && goal.goalId !== goalId) as GoalRecord[];
          
          this.logger.log("OUTER_GROUP_CACHE_POPULATION", {
            myGoalId: goalId,
            outerGroupGoalCount: outerGroupGoals.length,
            sameTableGoalsCount: sameTableGoalsInOuterGroup.length,
            sameTableGoalIds: sameTableGoalsInOuterGroup.map(g => g.goalId)
          });
          
          // Cache for all same-table goals in outer group (cross-branch caching)
          for (const otherGoal of sameTableGoalsInOuterGroup) {
            passed.set(otherGoal.goalId, rows);
            this.logger.log("CACHED_FOR_OTHER_GOAL", {
              myGoalId: goalId,
              otherGoalId: otherGoal.goalId,
              rowCount: rows.length,
              reason: "outer-group-table-match"
            });
          }
          
          // Cache for goals in the same inner group (existing logic for query merging)
          for (const otherGoal of compatibleGoals) {
            if (otherGoal.goalId !== goalId && otherGoal.table === myGoal.table) {
              passed.set(otherGoal.goalId, rows);
              this.logger.log("CACHED_FOR_OTHER_GOAL", {
                myGoalId: goalId,
                otherGoalId: otherGoal.goalId,
                rowCount: rows.length,
                reason: "same-inner-group"
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
    this.dbObj.addQuery(`${goalId} - ${sqlString}`);
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
    
    // Removed global cache storage - using improved grouping mechanism instead
    
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