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
      let query = this.dbObj.db(this.table);
      for (const [col, values] of Object.entries(allWhereClauses)) {
        if (values.size === 1) {
          query = query.where(col, Array.from(values)[0]);
        } else {
          query = query.whereIn(col, Array.from(values));
        }
      }
      if (this.options?.selectColumns) {
        query = query.select(this.options.selectColumns);
      } else {
        query = query.select('*');
      }
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
        let batch: Subst[] = [];
        let batchIndex = 0;
        let debounceTimer: NodeJS.Timeout | null = null;
        let flushingPromise: Promise<void> | null = null;
      
        this.logger.log("GOAL_STARTED", {
          goalId,
          table: this.table,
          queryObj,
        });

        const clearDebounce = () => {
          if (debounceTimer) {
            clearTimeout(debounceTimer);
            debounceTimer = null;
          }
        };

        const scheduleDebounce = () => {
          clearDebounce();
          debounceTimer = setTimeout(() => {
            flushBatch();
          }, BATCH_DEBOUNCE_MS);
        };

        const flushBatch = async () => {
          clearDebounce();
          if (flushingPromise) return flushingPromise;
          if (batch.length === 0 || cancelled) return Promise.resolve();
          flushingPromise = (async () => {
            try {
              this.logger.log("FLUSH_BATCH", {
                goalId,
                batchIndex,
                batchSize: batch.length,
              });
              // Use cacheOrQuery for each substitution to enable query merging
              for (const subst of batch) {
                if (cancelled) {
                  this.logger.log("FLUSH_BATCH_CANCELLED_DURING_SUBST", {
                    goalId,
                    batchIndex 
                  });
                  return;
                }
              
                this.logger.log("ABOUT_TO_CALL_CACHE_OR_QUERY", {
                  goalId,
                  batchIndex
                });
                const rows = await this.cacheOrQuery(goalId, queryObj, subst) || [];
              
                for (const row of rows) {
                  if (cancelled) {
                    this.logger.log("FLUSH_BATCH_CANCELLED_DURING_ROWS", {
                      goalId,
                      batchIndex 
                    });
                    return;
                  }
                
                  const unifiedSubst = this.unifyRowWithQuery(row, queryObj, new Map(subst));
                
                  if (unifiedSubst && !cancelled) {
                  // Create updated substitution with proper cache management and populate cache for other goals
                    const updatedSubst = createUpdatedSubstWithCacheForOtherGoals(
                      this.dbObj,
                      unifiedSubst,
                      goalId,
                      row,
                      this.logger
                    );
                  
                    const log_s = new Map(updatedSubst);
                    const log_c = log_s.get(ROW_CACHE) as Map<number, Record<string, any>>;
                    // log_s.delete(ROW_CACHE);
                    // count the rows in each key of log_c and provide a variable with details about the counts for each key
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
                    // unifiedSubst: updatedSubst,
                    });
                    observer.next(new Map(updatedSubst));
                    // let other things do their work
                    await new Promise(resolve => nextTick(resolve));
                  }
                }
              }
              batch = [];
              batchIndex++;
            } catch (error) {
              if (!cancelled) {
                observer.error?.(error);
              }
            } finally {
              flushingPromise = null;
              this.logger.log("FLUSH_BATCH_COMPLETE", {
                goalId,
                batchIndex,
                batchSize: batch.length,
              });
            }
          })();
          return flushingPromise;
        };

        let input_complete = false;
        let batchKeyUpdated = false;
      
        const subscription = input$.subscribe({
          next: (subst: Subst) => {
            if (cancelled) return;
          
            // Register in global registry and log group information on first substitution
            if (!batchKeyUpdated) {
              const groupId = subst.get(SQL_GROUP_ID) as number | undefined;
              batchKeyUpdated = true;
              
              // Register this goal in the global registry for its group
              if (groupId !== undefined) {
                if (!goalsByGroupId.has(groupId)) {
                  goalsByGroupId.set(groupId, new Set());
                }
                goalsByGroupId.get(groupId)!.add(goalId);
              }
            
              this.logger.log("GOAL_GROUP_INFO", {
                goalId,
                table: this.table,
                groupId,
                registeredInGroup: groupId !== undefined,
                queryObj
              });
            }
          
            batch.push(subst);
            this.logger.log("GOAL_NEXT", {
              goalId,
              input_complete,
              subst,
              batchLength: batch.length,
            });
            if (batch.length >= BATCH_SIZE) {
              flushBatch();
            } else {
              scheduleDebounce();
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
              batchSize: batch.length,
            });
            input_complete = true;
            flushBatch().then(() => {
              this.logger.log("GOAL_COMPLETE", {
                goalId,
                batchIndex,
                input_complete,
                cancelled,
                batchSize: batch.length,
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
            batchSize: batch.length,
          });
          cancelled = true;
          clearDebounce();
          subscription.unsubscribe?.();
        };
      });
      
      return resultObservable;
    };
    
    // Register this handler with its goal ID
    observableToGoalId.set(mySubstHandler as unknown as Observable<any>, goalId);
    
    return mySubstHandler;
  }

  private async executeQuery(goalId: number, queryObj: Record<string, Term>, s: Subst): Promise<any[]> {
    const walkedQuery = await queryUtils.walkAllKeys(queryObj, s);
    const whereCols = queryUtils.onlyGrounded(walkedQuery);

    const query = this.buildQuery(queryObj, whereCols);
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

  private buildQuery(queryObj: Record<string, Term>, whereCols: Record<string, any>) {
    let query = this.dbObj.db(this.table);
    
    // Add WHERE clauses for grounded values
    for (const [column, value] of Object.entries(whereCols)) {
      query = query.where(column, value);
    }

    // Select all columns if no specific columns are configured
    if (this.options?.selectColumns) {
      query = query.select(this.options.selectColumns);
    } else {
      query = query.select(Object.keys(queryObj));
    }

    return query;
  }

  private unifyRowWithQuery(row: any, queryObj: Record<string, Term>, s: Subst): Subst | null {
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