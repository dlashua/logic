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
 * Find goals compatible for merging: same table and same columns.
 */
function findCompatibleGoals(myGoal: GoalRecord, commonGoals: { goal: GoalRecord, matchingIds: string[] }[]): GoalRecord[] {
  return commonGoals
    .filter(g =>
      g.goal.table === myGoal.table &&
      Object.keys(g.goal.queryObj).every(col => col in myGoal.queryObj) &&
      Object.keys(myGoal.queryObj).every(col => col in g.goal.queryObj)
    )
    .map(g => g.goal);
}

/**
 * Collect all WHERE clause values from a set of goals for merging.
 */
async function collectAllWhereClauses(goals: GoalRecord[], s: Subst): Promise<Record<string, Set<any>>> {
  const allWhereClauses: Record<string, Set<any>> = {};
  for (const goal of goals) {
    const walked = await queryUtils.walkAllKeys(goal.queryObj, s);
    const whereCols = queryUtils.onlyGrounded(walked);
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
 */
function buildSelectQuery(dbObj: DBManager, table: string, whereClauses: Record<string, Set<any>>, selectColumns?: string[] | '*') {
  let query = dbObj.db(table);
  for (const [col, values] of Object.entries(whereClauses)) {
    if (values.size === 1) {
      query = query.where(col, Array.from(values)[0]);
    } else {
      query = query.whereIn(col, Array.from(values));
    }
  }
  if (selectColumns && selectColumns !== '*') {
    query = query.select(selectColumns);
  } else {
    query = query.select('*');
  }
  return query;
}

/**
 * Build a simple select query for a table with grounded columns only.
 */
function buildSimpleQuery(dbObj: DBManager, table: string, queryObj: Record<string, Term>, whereCols: Record<string, any>, selectColumns?: string[] | '*') {
  let query = dbObj.db(table);
  for (const [column, value] of Object.entries(whereCols)) {
    query = query.where(column, value);
  }
  if (selectColumns && selectColumns !== '*') {
    query = query.select(selectColumns);
  } else {
    query = query.select(Object.keys(queryObj));
  }
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
    // Get the group goals from the substitution
    const groupGoals = s.get(SQL_GROUP_GOALS) as Goal[] || [];
    
    if (groupGoals.length === 0) {
      this.logger.log("COMPATIBLE_GOALS", {
        myGoalId: myGoal.goalId,
        groupGoalsCount: 0,
        compatibleGoalIds: [],
        message: "No group goals found"
      });
      return [];
    }
    
    // Look up goal IDs for each goal function and get the corresponding goal records
    const otherGoals = groupGoals
      .map(goalFn => observableToGoalId.get(goalFn as unknown as Observable<any>))
      .filter(goalId => goalId !== undefined && goalId !== myGoal.goalId)
      .map(goalId => this.dbObj.getGoalById(goalId!))
      .filter(goal => goal !== undefined) as GoalRecord[];
    
    this.logger.log("COMPATIBLE_GOALS", {
      myGoalId: myGoal.goalId,
      groupGoalsCount: groupGoals.length,
      compatibleGoalIds: otherGoals.map(g => g.goalId),
      allGoals: otherGoals.map(g => ({
        goalId: g.goalId,
        table: g.table 
      }))
    });
    
    return otherGoals.map(x => this.haveAtLeastOneMatchingVar(myGoal, x)).filter(x => x !== null);
  }

  async cacheOrQuery(goalId: number, queryObj: Record<string, Term>, s: Subst): Promise<any[]> {    
    const cache = takeCache(goalId, s);
    if(cache) {
      this.logger.log("CACHE_HIT", {
        goalId,
        rowCount: cache.length,
        table: this.dbObj.getGoalById(goalId)?.table,
        rows: cache,
      });
      return cache;
    }

    this.logger.log("CACHE_MISS", {
      goalId,
      table: this.dbObj.getGoalById(goalId)?.table
    });

    const myGoal = this.dbObj.getGoalById(goalId);
    if(!myGoal) return [];

    const commonGoals = await this.processGoal(myGoal, s);

    this.logger.log("COMMON_GOALS", {
      myGoalId: myGoal.goalId,
      myGoalTable: myGoal.table,
      myGoalGroupId: myGoal.batchKey,
      allGoals: this.dbObj.getGoals().map(g => ({
        goalId: g.goalId,
        table: g.table,
        groupId: g.batchKey 
      })),
      commonGoals: commonGoals.map(g => ({
        goalId: g.goal.goalId,
        table: g.goal.table,
        groupId: g.goal.batchKey 
      })),
    });

    // If no joinable goals, check if we can merge WHERE clauses with other goals in the same batch
    const compatibleGoals = findCompatibleGoals(myGoal, commonGoals);
    if (compatibleGoals.length > 0) {
      this.logger.log("COMPATIBLE_MERGE_GOALS", {
        myGoalId: myGoal.goalId,
        compatibleGoalIds: compatibleGoals.map(g => g.goalId)
      });
      // Collect all WHERE conditions from compatible goals
      const allGoalsToMerge = [myGoal, ...compatibleGoals];
      const allWhereClauses = await collectAllWhereClauses(allGoalsToMerge, s);
      // Build merged query
      const query = buildSelectQuery(this.dbObj, this.table, allWhereClauses, this.options?.selectColumns || '*');
      const sqlString = query.toString();
      this.dbObj.addQuery(sqlString);
      this.logger.log("DB_QUERY_MERGED", {
        table: this.table,
        sql: sqlString,
        goalId,
        mergedGoalIds: allGoalsToMerge.map(g => g.goalId)
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
      // Cache results for other goals that can use this data
      const passed = getOrCreateRowCache(s);
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
      return rows;
    }

    const rows = await this.executeQuery(goalId, queryObj, s);
    
    // Cache results for other compatible goals in the same batch
    const passed = getOrCreateRowCache(s);
    for (const goalInfo of commonGoals) {
      const otherGoal = goalInfo.goal;
      if (otherGoal.goalId !== goalId && otherGoal.table === myGoal.table) {
        // Cache our results for the other goal to use
        passed.set(otherGoal.goalId, rows);
        this.logger.log("CACHED_FOR_OTHER_GOAL", {
          myGoalId: goalId,
          otherGoalId: otherGoal.goalId,
          rowCount: rows.length
        });
      }
    }
    
    return rows;
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

            // Unified approach: 
            // 1. If batch size > 1, try substitution batching first (more efficient)
            // 2. If that doesn't work or batch size = 1, try goal-level caching
            
            const goalMergeResults = new Map<number, any[]>();
            
            // Strategy 1: Check for cache hits first (always try goal-level caching)
            let cacheHits = 0;
            const cacheMisses: typeof batch = [];
            
            for (const subst of batch) {
              if (cancelled) return;
              
              const cache = takeCache(goalId, subst);
              if (cache) {
                goalMergeResults.set(subst as any, cache);
                cacheHits++;
                this.logger.log("CACHE_HIT", {
                  goalId,
                  rowCount: cache.length,
                  table: this.dbObj.getGoalById(goalId)?.table,
                  rows: cache,
                });
              } else {
                cacheMisses.push(subst);
              }
            }
            
            // Strategy 2: For cache misses, use substitution batching if multiple, else individual query
            if (cacheMisses.length > 0) {
              if (cacheMisses.length > 1) {
                this.logger.log("USING_SUBSTITUTION_BATCHING", {
                  goalId,
                  batchSize: cacheMisses.length,
                  cacheHits
                });
                
                // Collect all WHERE clause values from cache misses
                const allWhereClauses: Record<string, Set<any>> = {};
                for (const subst of cacheMisses) {
                  const walked = await queryUtils.walkAllKeys(queryObj, subst);
                  const whereCols = queryUtils.onlyGrounded(walked);
                  for (const [col, value] of Object.entries(whereCols)) {
                    if (!allWhereClauses[col]) allWhereClauses[col] = new Set();
                    allWhereClauses[col].add(value);
                  }
                }
                
                // Build and run a single merged query
                const query = buildSelectQuery(this.dbObj, this.table, allWhereClauses, this.options?.selectColumns || '*');
                const sqlString = query.toString();
                this.dbObj.addQuery(sqlString);
                this.logger.log("DB_QUERY_BATCH", {
                  table: this.table,
                  sql: sqlString,
                  goalId,
                  batchSize: cacheMisses.length
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
                
                // Store results for cache misses
                for (const subst of cacheMisses) {
                  goalMergeResults.set(subst as any, rows);
                }
              } else {
                // Single cache miss - use goal-level caching with merging
                this.logger.log("USING_GOAL_CACHING", {
                  goalId,
                  batchSize: cacheMisses.length,
                  cacheHits
                });
                
                for (const subst of cacheMisses) {
                  if (cancelled) return;
                  
                  const rows = await this.cacheOrQuery(goalId, queryObj, subst) || [];
                  goalMergeResults.set(subst as any, rows);
                }
              }
            }
            
            if (cacheHits > 0) {
              this.logger.log("CACHE_PERFORMANCE", {
                goalId,
                cacheHits,
                cacheMisses: cacheMisses.length,
                totalSubstitutions: batch.length
              });
            }
            
            // Process results for all substitutions
            for (const subst of batch) {
              if (cancelled) return;
              
              const rows = goalMergeResults.get(subst as any) || [];
              for (const row of rows) {
                if (cancelled) return;
                
                const unifiedSubst = this.unifyRowWithQuery(row, queryObj, new Map(subst));
                if (unifiedSubst && !cancelled) {
                  const updatedSubst = createUpdatedSubstWithCacheForOtherGoals(
                    this.dbObj,
                    unifiedSubst,
                    goalId,
                    row,
                    this.logger
                  );
                  const log_s = new Map(updatedSubst);
                  const log_c = log_s.get(ROW_CACHE) as Map<number, Record<string, any>>;
                  const cacheRowCounts: Record<number, number> = {};
                  if (log_c && typeof log_c.forEach === "function") {
                    log_c.forEach((rows, key) => {
                      cacheRowCounts[key] = Array.isArray(rows) ? rows.length : 0;
                    });
                  }
                  this.logger.log("UNIFY_SUCCESS", {
                    goalId,
                    queryObj,
                    row,
                    log_s,
                    cacheRowCounts
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
          next: (subst: Subst) => {
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
            batchProcessor.addItem(subst);
            this.logger.log("GOAL_NEXT", {
              goalId,
              input_complete,
              subst,
            });
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

  private async executeQuery(goalId: number, queryObj: Record<string, Term>, s: Subst): Promise<any[]> {
    const walkedQuery = await queryUtils.walkAllKeys(queryObj, s);
    const whereCols = queryUtils.onlyGrounded(walkedQuery);
    const query = buildSimpleQuery(this.dbObj, this.table, queryObj, whereCols, this.options?.selectColumns || '*');
    const sqlString = query.toString();
    this.dbObj.addQuery(sqlString);
    this.logger.log("DB_QUERY", {
      table: this.table,
      sql: sqlString,
      goalId,
    });
    const rows = await query;
    if (rows.length) {
      this.logger.log("DB_ROWS", {
        table: this.table,
        sql: sqlString,
        goalId,
        rows,
      });
    } else {
      this.logger.log("NO_DB_ROWS", {
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