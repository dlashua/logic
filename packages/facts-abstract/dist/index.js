// src/index.ts
import { getDefaultLogger as getDefaultLogger2, or } from "logic";

// src/abstract-relation.ts
import {
  GOAL_GROUP_ALL_GOALS,
  GOAL_GROUP_CONJ_GOALS,
  getDefaultLogger,
  isVar as isVar2,
  SimpleObservable,
  walk as walk2
} from "logic";

// src/abstract-relation-helpers.ts
import { isVar, queryUtils, unify, walk } from "logic";
function couldBenefitFromCache(myGoal, otherGoal, subst) {
  if (myGoal.relationIdentifier !== otherGoal.relationIdentifier) {
    return "different_relation";
  }
  const myColumns = Object.keys(myGoal.queryObj);
  const otherColumns = Object.keys(otherGoal.queryObj);
  let matches = 0;
  for (const column of myColumns) {
    if (otherColumns.includes(column)) {
      const myValueRaw = myGoal.queryObj[column];
      const otherValueRaw = otherGoal.queryObj[column];
      const myValue = walk(myValueRaw, subst);
      const otherValue = walk(otherValueRaw, subst);
      if (!isVar(myValue)) {
        if (!isVar(otherValue)) {
          if (myValue === otherValue) {
            matches++;
          } else {
            return "value_not_match";
          }
        } else {
          return "term_to_var";
        }
      } else {
        if (isVar(otherValue)) {
          matches++;
        } else {
          return "var_to_term";
        }
      }
    }
  }
  if (matches > 0) {
    return "match";
  }
  return "no_matches";
}
function canMergeQueries(goalA, goalB) {
  const aColumns = Object.keys(goalA.queryObj);
  const bColumns = Object.keys(goalB.queryObj);
  if (aColumns.length !== bColumns.length) {
    return false;
  }
  if (!aColumns.every((col) => bColumns.includes(col))) {
    return false;
  }
  for (const column of aColumns) {
    const aValue = goalA.queryObj[column];
    const bValue = goalB.queryObj[column];
    if (isVar(aValue) && isVar(bValue)) {
      if (aValue.id !== bValue.id) {
        return false;
      }
    } else if (isVar(aValue) || isVar(bValue)) {
      return false;
    } else {
      if (aValue !== bValue) {
        return false;
      }
    }
  }
  return true;
}
async function collectAllWhereClauses(goals, s) {
  const allWhereClauses = {};
  for (const goal of goals) {
    const whereCols = queryUtils.onlyGrounded(goal.queryObj);
    for (const [col, value] of Object.entries(whereCols)) {
      if (!allWhereClauses[col]) allWhereClauses[col] = /* @__PURE__ */ new Set();
      allWhereClauses[col].add(value);
    }
  }
  return allWhereClauses;
}
async function collectWhereClausesFromSubstitutions(queryObj, substitutions) {
  const whereClauses = {};
  for (const subst of substitutions) {
    const walked = await queryUtils.walkAllKeys(queryObj, subst);
    const whereCols = queryUtils.onlyGrounded(walked);
    for (const [col, value] of Object.entries(whereCols)) {
      if (!whereClauses[col]) whereClauses[col] = /* @__PURE__ */ new Set();
      whereClauses[col].add(value);
    }
  }
  return whereClauses;
}
function collectColumnsFromGoals(myQueryObj, cacheCompatibleGoals, mergeCompatibleGoals) {
  const allGoalColumns = /* @__PURE__ */ new Set();
  Object.keys(myQueryObj).forEach((col) => allGoalColumns.add(col));
  if (mergeCompatibleGoals) {
    for (const goal of mergeCompatibleGoals) {
      Object.keys(goal.queryObj).forEach((col) => allGoalColumns.add(col));
    }
  }
  for (const cacheGoal of cacheCompatibleGoals) {
    Object.keys(cacheGoal.queryObj).forEach((col) => allGoalColumns.add(col));
  }
  const additionalColumns = [];
  const columns = [.../* @__PURE__ */ new Set([...allGoalColumns, ...additionalColumns])];
  return {
    columns,
    additionalColumns
  };
}
function buildWhereConditions(whereClauses) {
  const conditions = [];
  for (const [column, values] of Object.entries(whereClauses)) {
    if (values.size === 1) {
      conditions.push({
        column,
        operator: "eq",
        value: Array.from(values)[0]
      });
    } else if (values.size > 1) {
      conditions.push({
        column,
        operator: "in",
        value: null,
        values: Array.from(values)
      });
    }
  }
  return conditions;
}
function unifyRowWithQuery(row, queryObj, s) {
  let result = s;
  for (const [column, term] of Object.entries(queryObj)) {
    const value = row[column];
    if (value === void 0) continue;
    const unified = unify(term, value, result);
    if (unified === null) {
      return null;
    }
    result = unified;
  }
  return result;
}

// src/cache-manager.ts
var ROW_CACHE = Symbol.for("abstract-row-cache");
var DefaultCacheManager = class {
  /**
   * Get cached rows for a goal from a substitution
   */
  get(goalId, subst) {
    const cache = this.getOrCreateRowCache(subst);
    if (cache.has(goalId)) {
      const entry = cache.get(goalId);
      return entry.data;
    }
    return null;
  }
  /**
   * Set cached rows for a goal in a substitution
   */
  set(goalId, subst, rows, meta) {
    const cache = this.getOrCreateRowCache(subst);
    cache.set(goalId, {
      data: rows,
      timestamp: Date.now(),
      goalId,
      meta
    });
  }
  /**
   * Clear cache entries
   */
  clear(goalId) {
    if (goalId !== void 0) {
    }
  }
  /**
   * Check if cache entry exists
   */
  has(goalId, subst) {
    const cache = this.getOrCreateRowCache(subst);
    return cache.has(goalId);
  }
  /**
   * Remove cache entry for a specific goal from a substitution
   */
  delete(goalId, subst) {
    const cache = this.getOrCreateRowCache(subst);
    cache.delete(goalId);
  }
  /**
   * Get or create the cache map from a substitution
   */
  getOrCreateRowCache(subst) {
    if (!subst.has(ROW_CACHE)) {
      subst.set(ROW_CACHE, /* @__PURE__ */ new Map());
    }
    return subst.get(ROW_CACHE);
  }
  /**
   * Format cache for logging (matches current implementation)
   */
  formatCacheForLog(subst) {
    const result = {};
    const cache = subst.get(ROW_CACHE);
    if (!(cache instanceof Map)) return result;
    for (const [goalId, entry] of cache.entries()) {
      if (Array.isArray(entry.data)) {
        if (entry.data.length <= 5) {
          result[goalId] = entry.data;
        } else {
          result[goalId] = {
            count: entry.data.length,
            timestamp: entry.timestamp
          };
        }
      }
    }
    return result;
  }
};

// src/abstract-relation.ts
var observableToGoalId = /* @__PURE__ */ new WeakMap();
var DEFAULT_BATCH_SIZE = 100;
var DEFAULT_DEBOUNCE_MS = 50;
var AbstractRelation = class {
  constructor(dataStore, goalManager, relationIdentifier, logger, _options, config) {
    this.dataStore = dataStore;
    this.goalManager = goalManager;
    this.relationIdentifier = relationIdentifier;
    this._options = _options;
    this.logger = logger ?? getDefaultLogger();
    this.cacheManager = config?.cacheManager ?? new DefaultCacheManager();
    this.config = {
      batchSize: config?.batchSize ?? DEFAULT_BATCH_SIZE,
      debounceMs: config?.debounceMs ?? DEFAULT_DEBOUNCE_MS,
      enableCaching: config?.enableCaching ?? true,
      enableQueryMerging: config?.enableQueryMerging ?? true,
      cacheManager: this.cacheManager
    };
  }
  logger;
  cacheManager;
  config;
  /**
   * Create a goal for this relation
   */
  createGoal(queryObj) {
    const goalId = this.goalManager.getNextGoalId();
    this.logger.log("GOAL_CREATED", {
      goalId,
      relationIdentifier: this.relationIdentifier,
      queryObj,
      dataStore: this.dataStore.type
    });
    this.goalManager.addGoal(
      goalId,
      this.relationIdentifier,
      queryObj,
      void 0,
      this._options
    );
    const goalFunction = (input$) => {
      return new SimpleObservable((observer) => {
        let cancelled = false;
        let batchIndex = 0;
        let inputComplete = false;
        this.logger.log("GOAL_STARTED", {
          goalId,
          relationIdentifier: this.relationIdentifier,
          queryObj,
          dataStore: this.dataStore.type
        });
        const batchProcessor = this.createBatchProcessor({
          batchSize: this.config.batchSize,
          debounceMs: this.config.debounceMs,
          onFlush: async (batch) => {
            if (cancelled) return;
            this.logger.log("FLUSH_BATCH", {
              goalId,
              batchIndex,
              batchSize: batch.length,
              dataStore: this.dataStore.type
            });
            const rows = await this.executeQueryForSubstitutions(
              goalId,
              queryObj,
              batch
            );
            const representativeSubst = batch[0];
            const myGoal = this.goalManager.getGoalById(goalId);
            let cacheCompatibleGoals = [];
            if (myGoal && representativeSubst) {
              const relatedGoals = await this.findRelatedGoals(
                myGoal,
                representativeSubst
              );
              cacheCompatibleGoals = this.findCacheCompatibleGoals(
                myGoal,
                relatedGoals,
                representativeSubst
              );
            }
            await this.processFreshRows(
              goalId,
              queryObj,
              rows,
              batch,
              observer,
              cacheCompatibleGoals
            );
            batchIndex++;
          }
        });
        let active = 0;
        let completed = false;
        const subscription = input$.subscribe({
          next: async (subst) => {
            if (cancelled) return;
            active++;
            this.logger.log("GOAL_NEXT", {
              goalId,
              batchIndex,
              inputComplete,
              dataStore: this.dataStore.type
            });
            if (this.config.enableCaching) {
              const cachedRows = this.cacheManager.get(goalId, subst);
              if (cachedRows) {
                this.logger.log("CACHE_HIT_IMMEDIATE", {
                  goalId,
                  rowCount: cachedRows.length,
                  relationIdentifier: this.relationIdentifier,
                  dataStore: this.dataStore.type
                });
                await this.processCachedRows(
                  goalId,
                  queryObj,
                  cachedRows,
                  subst,
                  observer
                );
                active--;
                if (completed && active === 0) observer.complete?.();
                return;
              }
            }
            batchProcessor.addItem(subst);
            this.logger.log("CACHE_MISS_TO_BATCH", {
              goalId,
              inputComplete,
              dataStore: this.dataStore.type
            });
            active--;
            if (completed && active === 0) observer.complete?.();
          },
          error: (err) => {
            if (!cancelled) observer.error?.(err);
          },
          complete: () => {
            this.logger.log("UPSTREAM_GOAL_COMPLETE", {
              goalId,
              batchIndex,
              inputComplete,
              cancelled,
              dataStore: this.dataStore.type
            });
            inputComplete = true;
            batchProcessor.complete().then(() => {
              this.logger.log("GOAL_COMPLETE", {
                goalId,
                batchIndex,
                inputComplete,
                cancelled,
                dataStore: this.dataStore.type
              });
              completed = true;
              if (completed && active === 0) observer.complete?.();
            }).catch((e) => {
              console.error(e);
              completed = true;
              if (completed && active === 0) observer.complete?.();
            });
          }
        });
        return () => {
          this.logger.log("GOAL_CANCELLED", {
            goalId,
            batchIndex,
            inputComplete,
            cancelled,
            dataStore: this.dataStore.type
          });
          cancelled = true;
          batchProcessor.cancel();
          subscription.unsubscribe?.();
        };
      });
    };
    const displayName = `${this.dataStore.type.toUpperCase()}_${this.relationIdentifier}_${goalId}`;
    goalFunction.displayName = displayName;
    observableToGoalId.set(goalFunction, goalId);
    return goalFunction;
  }
  /**
   * Execute query for a set of substitutions
   */
  async executeQueryForSubstitutions(goalId, queryObj, substitutions) {
    if (substitutions.length === 0) return [];
    this.logger.log("EXECUTING_UNIFIED_QUERY", {
      goalId,
      substitutionCount: substitutions.length,
      relationIdentifier: this.relationIdentifier,
      dataStore: this.dataStore.type
    });
    const myGoal = this.goalManager.getGoalById(goalId);
    if (!myGoal) return [];
    const representativeSubst = substitutions[0];
    const relatedGoals = await this.findRelatedGoals(
      myGoal,
      representativeSubst
    );
    const mergeCompatibleGoals = this.config.enableQueryMerging ? this.findMergeCompatibleGoals(myGoal, relatedGoals) : [];
    const cacheCompatibleGoals = this.config.enableCaching ? this.findCacheCompatibleGoals(myGoal, relatedGoals, representativeSubst) : [];
    return await this.buildAndExecuteQuery(
      goalId,
      queryObj,
      substitutions,
      mergeCompatibleGoals,
      cacheCompatibleGoals
    );
  }
  /**
   * Build query parameters and execute via data store
   */
  async buildAndExecuteQuery(goalId, queryObj, substitutions, mergeCompatibleGoals, cacheCompatibleGoals) {
    const whereClauses = await this.collectWhereClausesFromSubstitutions(
      queryObj,
      substitutions
    );
    if (mergeCompatibleGoals.length > 0) {
      const myGoal = this.goalManager.getGoalById(goalId);
      if (myGoal) {
        const allGoalsToMerge = [myGoal, ...mergeCompatibleGoals];
        const goalWhereClauses = await this.collectAllWhereClauses(
          allGoalsToMerge,
          substitutions[0]
        );
        for (const [col, values] of Object.entries(goalWhereClauses)) {
          if (whereClauses[col]) {
            for (const value of values) {
              whereClauses[col].add(value);
            }
          } else {
            whereClauses[col] = new Set(values);
          }
        }
      }
    }
    const columns = this.collectColumnsFromGoals(
      queryObj,
      cacheCompatibleGoals,
      mergeCompatibleGoals
    );
    const whereConditions = this.buildWhereConditions(whereClauses);
    const mergeCompatibleGoalIds = mergeCompatibleGoals.map((x) => x.goalId).join(",");
    const cacheCompatibleGoalIds = cacheCompatibleGoals.map((x) => x.goalId).join(",");
    const iffmt = (v, fn) => v ? fn(v) : "";
    const annotatedLogQuery = (queryString) => this.goalManager.addQuery(
      `G:${goalId}${iffmt(mergeCompatibleGoalIds, (v) => ` M:${v}`)}${iffmt(cacheCompatibleGoalIds, (v) => ` C:${v}`)} - ${queryString}`
    );
    const queryParams = {
      relationIdentifier: this.relationIdentifier,
      selectColumns: columns.columns,
      whereConditions,
      relationOptions: this._options,
      goalId,
      logQuery: annotatedLogQuery
    };
    const rows = await this.dataStore.executeQuery(queryParams);
    this.logger.log("DB_QUERY_EXECUTED", {
      goalId,
      relationIdentifier: this.relationIdentifier,
      rowCount: rows.length,
      queryParams,
      dataStore: this.dataStore.type
    });
    return rows;
  }
  /**
   * Find related goals for merging and caching
   */
  async findRelatedGoals(myGoal, s) {
    const innerGroupGoals = s.get(GOAL_GROUP_CONJ_GOALS) || [];
    const outerGroupGoals = s.get(GOAL_GROUP_ALL_GOALS) || [];
    const goalsForCaching = outerGroupGoals;
    if (goalsForCaching.length === 0) {
      return [];
    }
    const otherGoalIds = goalsForCaching.map(
      (goalFn) => observableToGoalId.get(goalFn)
    ).filter(
      (goalId) => goalId !== void 0 && goalId !== myGoal.goalId
    );
    const otherGoals = otherGoalIds.map((goalId) => this.goalManager.getGoalById(goalId)).filter((goal) => goal !== void 0);
    this.logger.log("FOUND_RELATED_GOALS", {
      myGoalId: myGoal.goalId,
      myGoalQueryObj: myGoal.queryObj,
      foundOtherGoalIds: otherGoalIds,
      relatedGoals: otherGoals.map((g) => ({
        goalId: g.goalId,
        relationIdentifier: g.relationIdentifier,
        queryObj: g.queryObj
      })),
      dataStore: this.dataStore.type
    });
    return otherGoals.map((goal) => ({
      goal,
      matchingIds: []
      // Empty for now - could implement variable matching logic
    }));
  }
  /**
   * Find goals that are compatible for query merging
   */
  findMergeCompatibleGoals(myGoal, relatedGoals) {
    const compatibleGoals = [];
    for (const { goal } of relatedGoals) {
      if (goal.relationIdentifier === myGoal.relationIdentifier && this.canMergeQueries(myGoal, goal)) {
        compatibleGoals.push(goal);
      }
    }
    this.logger.log("MERGE_COMPATIBILITY_CHECK", {
      myGoalId: myGoal.goalId,
      candidateGoals: relatedGoals.map((g) => ({
        goalId: g.goal.goalId,
        queryObj: g.goal.queryObj
      })),
      mergeCompatibleGoalIds: compatibleGoals.map((g) => g.goalId),
      relationIdentifier: this.relationIdentifier,
      dataStore: this.dataStore.type
    });
    return compatibleGoals;
  }
  /**
   * Find goals that are compatible for result caching
   */
  findCacheCompatibleGoals(myGoal, relatedGoals, subst) {
    const cacheBeneficiaryGoals = [];
    const candidateGoalsWithCompatibility = [];
    for (const { goal } of relatedGoals) {
      const isCompatible = this.couldBenefitFromCache(myGoal, goal, subst);
      candidateGoalsWithCompatibility.push({
        goalId: goal.goalId,
        queryObj: goal.queryObj,
        cacheCompatible: isCompatible
      });
      if (isCompatible === "match") {
        cacheBeneficiaryGoals.push(goal);
      }
    }
    this.logger.log("CACHE_COMPATIBILITY_CHECK", {
      myGoalId: myGoal.goalId,
      myGoalQueryObj: myGoal.queryObj,
      candidateGoals: candidateGoalsWithCompatibility,
      cacheCompatibleGoalIds: cacheBeneficiaryGoals.map((g) => g.goalId),
      relationIdentifier: this.relationIdentifier,
      dataStore: this.dataStore.type
    });
    return cacheBeneficiaryGoals;
  }
  /**
   * Process cached rows
   */
  async processCachedRows(goalId, queryObj, cachedRows, subst, observer) {
    const filteredRows = cachedRows.filter((row) => {
      for (const [col, term] of Object.entries(queryObj)) {
        const grounded = walk2(term, subst);
        if (!isVar2(grounded) && row[col] !== grounded) {
          return false;
        }
      }
      return true;
    });
    this.logger.log("CACHE_ROWS_PROCESSED", {
      goalId,
      originalCount: cachedRows.length,
      filteredCount: filteredRows.length,
      relationIdentifier: this.relationIdentifier,
      dataStore: this.dataStore.type
    });
    for (const row of filteredRows) {
      const unifiedSubst = this.unifyRowWithQuery(
        row,
        queryObj,
        new Map(subst)
      );
      if (unifiedSubst) {
        observer.next(unifiedSubst);
      }
      await new Promise((resolve) => setTimeout(() => resolve(void 0), 0));
    }
  }
  /**
   * Process fresh query rows
   */
  async processFreshRows(goalId, queryObj, rows, substitutions, observer, cacheCompatibleGoals) {
    for (const subst of substitutions) {
      if (this.config.enableCaching) {
        this.cacheManager.clear(goalId);
      }
      if (rows.length === 0) {
        this.logger.log("DB_NO_ROWS", {
          goalId,
          queryObj,
          wasFromCache: false,
          relationIdentifier: this.relationIdentifier,
          dataStore: this.dataStore.type
        });
        continue;
      }
      for (const row of rows) {
        const unifiedSubst = this.unifyRowWithQuery(
          row,
          queryObj,
          new Map(subst)
        );
        if (unifiedSubst) {
          if (this.config.enableCaching) {
            for (const otherGoal of cacheCompatibleGoals) {
              if (otherGoal.goalId !== goalId) {
                this.cacheManager.set(otherGoal.goalId, unifiedSubst, rows, {
                  fromGoalId: goalId
                });
                this.logger.log("CACHED_FOR_OTHER_GOAL", {
                  myGoalId: goalId,
                  otherGoalId: otherGoal.goalId,
                  rowCount: rows.length,
                  dataStore: this.dataStore.type
                });
              }
            }
          }
          this.logger.log("UNIFY_SUCCESS", {
            goalId,
            queryObj,
            row,
            wasFromCache: false,
            relationIdentifier: this.relationIdentifier,
            dataStore: this.dataStore.type
          });
          observer.next(unifiedSubst);
          await new Promise((resolve) => setTimeout(resolve, 0));
        } else {
          this.logger.log("UNIFY_FAILURE", {
            goalId,
            queryObj,
            row,
            wasFromCache: false,
            relationIdentifier: this.relationIdentifier,
            dataStore: this.dataStore.type
          });
        }
      }
    }
  }
  // Helper method delegates
  couldBenefitFromCache = couldBenefitFromCache;
  canMergeQueries = canMergeQueries;
  collectWhereClausesFromSubstitutions = collectWhereClausesFromSubstitutions;
  collectAllWhereClauses = collectAllWhereClauses;
  collectColumnsFromGoals = collectColumnsFromGoals;
  buildWhereConditions = buildWhereConditions;
  unifyRowWithQuery = unifyRowWithQuery;
  /**
   * Create a batch processor utility
   */
  createBatchProcessor(options) {
    let batch = [];
    let debounceTimer = null;
    let flushingPromise = null;
    let cancelled = false;
    const clearDebounce = () => {
      if (debounceTimer) {
        clearTimeout(debounceTimer);
        debounceTimer = null;
      }
    };
    const flushBatch = () => {
      clearDebounce();
      if (flushingPromise) return flushingPromise;
      if (batch.length === 0 || cancelled) return Promise.resolve();
      const toFlush = batch;
      batch = [];
      flushingPromise = Promise.resolve(options.onFlush(toFlush)).finally(
        () => {
          flushingPromise = null;
        }
      );
      return flushingPromise;
    };
    const addItem = (item) => {
      batch.push(item);
      if (batch.length >= options.batchSize) {
        flushBatch();
      } else {
        clearDebounce();
        debounceTimer = setTimeout(() => flushBatch(), options.debounceMs);
      }
    };
    const complete = async () => {
      await flushBatch();
    };
    const cancel = () => {
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
};

// src/goal-manager.ts
var DefaultGoalManager = class {
  goals = [];
  queries = [];
  nextGoalId = 0;
  getNextGoalId() {
    return ++this.nextGoalId;
  }
  addGoal(goalId, relationIdentifier, queryObj, batchKey, relationOptions) {
    this.goals.push({
      goalId,
      relationIdentifier,
      queryObj,
      batchKey,
      relationOptions
    });
  }
  getGoalById(id) {
    return this.goals.find((goal) => goal.goalId === id);
  }
  getGoalsByBatchKey(batchKey) {
    return this.goals.filter((goal) => goal.batchKey === batchKey);
  }
  getGoals() {
    return [...this.goals];
  }
  clearGoals() {
    this.goals.length = 0;
  }
  addQuery(query) {
    this.queries.push(query);
  }
  getQueries() {
    return [...this.queries];
  }
  clearQueries() {
    this.queries.length = 0;
  }
  getQueryCount() {
    return this.queries.length;
  }
};

// src/index.ts
var AbstractRelationFactory = class {
  constructor(dataStore, logger, config) {
    this.dataStore = dataStore;
    this.logger = logger ?? getDefaultLogger2();
    this.goalManager = new DefaultGoalManager();
    this.config = config ?? {};
  }
  goalManager;
  logger;
  config;
  /**
   * Get the appropriate relation identifier based on datastore type and options
   */
  getRelationIdentifier(table, options) {
    if (this.dataStore.type === "rest") {
      const restOptions = options;
      if (restOptions?.pathTemplate) {
        return restOptions.pathTemplate;
      }
    }
    return table;
  }
  /**
   * Create a regular relation for a table
   */
  createRelation(table, options) {
    const relationIdentifier = this.getRelationIdentifier(table, options);
    const relation = new AbstractRelation(
      this.dataStore,
      this.goalManager,
      relationIdentifier,
      this.logger,
      options,
      this.config
    );
    return (queryObj) => {
      return relation.createGoal(queryObj);
    };
  }
  /**
   * Create a symmetric relation for bidirectional queries
   */
  createSymmetricRelation(table, keys, options) {
    const relationIdentifier = this.getRelationIdentifier(
      table,
      options
    );
    const relation = new AbstractRelation(
      this.dataStore,
      this.goalManager,
      relationIdentifier,
      this.logger,
      options,
      this.config
    );
    return (queryObj) => {
      const queryObjSwapped = {
        [keys[0]]: queryObj[keys[1]],
        [keys[1]]: queryObj[keys[0]]
      };
      return or(
        relation.createGoal(queryObj),
        relation.createGoal(queryObjSwapped)
      );
    };
  }
  /**
   * Get debugging information
   */
  getQueries() {
    return this.goalManager.getQueries();
  }
  clearQueries() {
    this.goalManager.clearQueries();
  }
  getQueryCount() {
    return this.goalManager.getQueryCount();
  }
  /**
   * Access the underlying data store
   */
  getDataStore() {
    return this.dataStore;
  }
  /**
   * Close the data store connection
   */
  async close() {
    if (this.dataStore.close) {
      await this.dataStore.close();
    }
  }
};
function createAbstractRelationSystem(dataStore, logger, config) {
  const factory = new AbstractRelationFactory(
    dataStore,
    logger,
    config
  );
  return {
    rel: factory.createRelation.bind(factory),
    relSym: factory.createSymmetricRelation?.bind(factory),
    getQueries: factory.getQueries.bind(factory),
    clearQueries: factory.clearQueries.bind(factory),
    getQueryCount: factory.getQueryCount.bind(factory),
    getDataStore: factory.getDataStore.bind(factory),
    close: factory.close.bind(factory)
  };
}
export {
  AbstractRelation,
  AbstractRelationFactory,
  DefaultCacheManager,
  DefaultGoalManager,
  createAbstractRelationSystem
};
//# sourceMappingURL=index.js.map