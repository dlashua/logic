// src/facts-memory.ts
import { getDefaultLogger } from "logic";

// src/memory-obj-relation.ts
import {
  indexUtils,
  isVar,
  queryUtils,
  SimpleObservable,
  unificationUtils
} from "logic";
var MemoryObjRelation = class {
  constructor(keys, logger, config) {
    this.keys = keys;
    this.logger = logger;
    this.config = config;
  }
  /**
   * Update facts matching a where-clause with new values.
   * @param where - fields and values to match
   * @param newValues - fields and values to update
   */
  updateFacts(where, newValues, upsert = false) {
    let updated = false;
    for (let i = 0; i < this.facts.length; i++) {
      const fact = this.facts[i];
      let match = true;
      for (const key of Object.keys(where)) {
        if (fact[key] !== where[key]) {
          match = false;
          break;
        }
      }
      if (match) {
        updated = true;
        if (this.config.enableIndexing !== false) {
          for (const key of this.keys) {
            if (key in newValues && key in fact && indexUtils.isIndexable(fact[key])) {
              const index = this.indexes.get(key);
              if (index) {
                this.removeFromIndex(index, fact[key], i);
              }
            }
          }
        }
        Object.assign(fact, newValues);
        if (this.config.enableIndexing !== false) {
          for (const key of this.keys) {
            if (key in newValues && indexUtils.isIndexable(fact[key])) {
              let index = this.indexes.get(key);
              if (!index) {
                index = indexUtils.createIndex();
                this.indexes.set(key, index);
              }
              indexUtils.addToIndex(index, fact[key], i);
            }
          }
        }
        this.logger.log("FACT_UPDATED", {
          message: `Updated object fact at index ${i}`,
          fact
        });
      }
    }
    if (!updated && upsert) {
      const newFact = { ...where, ...newValues };
      this.addFact(newFact);
      this.logger.log("FACT_INSERTED", {
        message: `Inserted new object fact (upsert)`,
        fact: newFact
      });
    }
  }
  /**
   * Remove a fact index from a Map<value, Set<index>>
   */
  removeFromIndex(index, value, factIndex) {
    const set = index.get(value);
    if (set) {
      set.delete(factIndex);
      if (set.size === 0) {
        index.delete(value);
      }
    }
  }
  facts = [];
  indexes = /* @__PURE__ */ new Map();
  goalIdCounter = 0;
  createRelation() {
    const goalFn = (queryObj) => {
      const goalId = this.generateGoalId();
      return this.createGoal(queryObj, goalId);
    };
    goalFn.set = (factObj) => {
      this.addFact(factObj);
    };
    goalFn.update = (where, newValues) => {
      this.updateFacts(where, newValues, false);
    };
    goalFn.upsert = (where, newValues) => {
      this.updateFacts(where, newValues, true);
    };
    goalFn.raw = this.facts;
    goalFn.indexes = this.indexes;
    goalFn.keys = this.keys;
    return goalFn;
  }
  generateGoalId() {
    return ++this.goalIdCounter;
  }
  createGoal(queryObj, goalId) {
    return (input$) => new SimpleObservable((observer) => {
      let cancelled = false;
      this.logger.log("RUN_START", {
        message: `Starting memory object relation goal ${goalId}`,
        queryObj
      });
      this.logger.log("STARTING PROCESS ALL", {
        goalId
      });
      const subscription = input$.subscribe({
        next: async (s) => {
          try {
            const queryKeys = Object.keys(queryObj);
            const walkedQuery = await queryUtils.walkAllKeys(queryObj, s);
            const indexedKeys = [];
            for (const key of queryKeys) {
              if (!isVar(walkedQuery[key]) && this.indexes.has(key)) {
                indexedKeys.push(key);
              }
            }
            let candidateIndexes = null;
            if (indexedKeys.length > 0) {
              this.logger.log("INDEX_LOOKUP", {
                message: `Using indexes for keys: ${indexedKeys.join(", ")}`
              });
              for (const key of indexedKeys) {
                const value = walkedQuery[key];
                const index = this.indexes.get(key);
                if (!index) continue;
                const factNums = index.get(value);
                if (!factNums || factNums.size === 0) {
                  candidateIndexes = /* @__PURE__ */ new Set();
                  break;
                }
                if (candidateIndexes === null) {
                  candidateIndexes = new Set(factNums);
                } else {
                  candidateIndexes = indexUtils.intersect(
                    candidateIndexes,
                    factNums
                  );
                  if (candidateIndexes.size === 0) break;
                }
              }
            }
            if (candidateIndexes === null) {
              this.logger.log("MEMORY_SCAN", {
                message: `Full scan of ${this.facts.length} object facts`
              });
              await this.processFacts(
                this.facts,
                queryObj,
                walkedQuery,
                queryKeys,
                s,
                observer,
                () => cancelled
              );
            } else {
              this.logger.log("INDEX_LOOKUP", {
                message: `Checking ${candidateIndexes.size} indexed object facts`
              });
              const indexedFacts = Array.from(candidateIndexes).map(
                (i) => this.facts[i]
              );
              await this.processFacts(
                indexedFacts,
                queryObj,
                walkedQuery,
                queryKeys,
                s,
                observer,
                () => cancelled
              );
            }
          } catch (err) {
            if (!cancelled) observer.error?.(err);
          }
        },
        complete: () => {
          if (!cancelled) {
            this.logger.log("RUN_END", {
              message: `Completed memory object relation goal ${goalId}`
            });
            observer.complete?.();
          }
        },
        error: (err) => {
          if (!cancelled) observer.error?.(err);
        }
      });
      return () => {
        cancelled = true;
        subscription.unsubscribe?.();
      };
    });
  }
  async processFacts(facts, queryObj, walkedQuery, queryKeys, s, observer, isCancelled) {
    for (let i = 0; i < facts.length; i++) {
      if (isCancelled()) break;
      const fact = facts[i];
      const s1 = await unificationUtils.unifyRowWithWalkedQ(
        queryKeys,
        walkedQuery,
        fact,
        s
      );
      if (s1 && !isCancelled()) {
        this.logger.log("FACT_MATCH", {
          message: "Object fact matched",
          fact,
          queryObj
        });
        observer.next(s1);
      }
      if (i % 10 === 0) {
        await new Promise((resolve) => queueMicrotask(() => resolve(null)));
      }
    }
  }
  addFact(factObj) {
    const factIndex = this.facts.length;
    this.facts.push(factObj);
    if (this.config.enableIndexing !== false) {
      for (const key of this.keys) {
        if (key in factObj) {
          const term = factObj[key];
          if (indexUtils.isIndexable(term)) {
            let index = this.indexes.get(key);
            if (!index) {
              index = indexUtils.createIndex();
              this.indexes.set(key, index);
            }
            indexUtils.addToIndex(index, term, factIndex);
          }
        }
      }
    }
    this.logger.log("FACT_ADDED", {
      message: `Added object fact at index ${factIndex}`,
      fact: factObj
    });
  }
};

// src/memory-relation.ts
import {
  indexUtils as indexUtils2,
  isVar as isVar2,
  queryUtils as queryUtils2,
  SimpleObservable as SimpleObservable2,
  unificationUtils as unificationUtils2
} from "logic";
var MemoryRelation = class {
  constructor(logger, config) {
    this.logger = logger;
    this.config = config;
  }
  facts = [];
  indexes = /* @__PURE__ */ new Map();
  goalIdCounter = 0;
  createRelation() {
    const goalFn = (...query) => {
      const goalId = this.generateGoalId();
      return this.createGoal(query, goalId);
    };
    goalFn.set = (...fact) => {
      this.addFact(fact);
    };
    goalFn.raw = this.facts;
    goalFn.indexes = this.indexes;
    return goalFn;
  }
  generateGoalId() {
    return ++this.goalIdCounter;
  }
  createGoal(query, goalId) {
    return (input$) => new SimpleObservable2((observer) => {
      let cancelled = false;
      this.logger.log("RUN_START", {
        message: `Starting memory relation goal ${goalId}`,
        query
      });
      const subscription = input$.subscribe({
        next: async (s) => {
          try {
            if (cancelled) return;
            const walkedQuery = queryUtils2.walkAllArray(query, s);
            const indexedPositions = [];
            walkedQuery.forEach((term, i) => {
              if (!isVar2(term) && this.indexes.has(i)) {
                indexedPositions.push(i);
              }
            });
            let candidateIndexes = null;
            if (indexedPositions.length > 0) {
              this.logger.log("INDEX_LOOKUP", {
                message: `Using indexes for positions: ${indexedPositions.join(", ")}`
              });
              for (const pos of indexedPositions) {
                const term = walkedQuery[pos];
                const index = this.indexes.get(pos);
                if (!index) continue;
                const factNums = index.get(term);
                if (!factNums || factNums.size === 0) {
                  candidateIndexes = /* @__PURE__ */ new Set();
                  break;
                }
                if (candidateIndexes === null) {
                  candidateIndexes = new Set(factNums);
                } else {
                  candidateIndexes = indexUtils2.intersect(
                    candidateIndexes,
                    factNums
                  );
                  if (candidateIndexes.size === 0) break;
                }
              }
            }
            if (candidateIndexes === null) {
              this.logger.log("MEMORY_SCAN", {
                message: `Full scan of ${this.facts.length} facts`
              });
              await this.processFacts(
                this.facts,
                query,
                s,
                observer,
                () => cancelled
              );
            } else {
              this.logger.log("INDEX_LOOKUP", {
                message: `Checking ${candidateIndexes.size} indexed facts`
              });
              const indexedFacts = Array.from(candidateIndexes).map(
                (i) => this.facts[i]
              );
              await this.processFacts(
                indexedFacts,
                query,
                s,
                observer,
                () => cancelled
              );
            }
          } catch (err) {
            if (!cancelled) observer.error?.(err);
          }
        },
        complete: () => {
          if (!cancelled) {
            this.logger.log("RUN_END", {
              message: `Completed memory relation goal ${goalId}`
            });
            observer.complete?.();
          }
        },
        error: (err) => {
          if (!cancelled) observer.error?.(err);
        }
      });
      return () => {
        cancelled = true;
        subscription.unsubscribe?.();
      };
    });
  }
  async processFacts(facts, query, s, observer, isCancelled) {
    for (let i = 0; i < facts.length; i++) {
      if (isCancelled()) break;
      const fact = facts[i];
      const s1 = await unificationUtils2.unifyArrays(query, fact, s);
      if (s1 && !isCancelled()) {
        this.logger.log("FACT_MATCH", {
          message: "Fact matched",
          fact,
          query
        });
        observer.next(s1);
      }
      if (i % 10 === 0) {
        await new Promise((resolve) => queueMicrotask(() => resolve(null)));
      }
    }
  }
  addFact(fact) {
    const factIndex = this.facts.length;
    this.facts.push(fact);
    if (this.config.enableIndexing !== false) {
      fact.forEach((term, position) => {
        if (indexUtils2.isIndexable(term)) {
          let index = this.indexes.get(position);
          if (!index) {
            index = indexUtils2.createIndex();
            this.indexes.set(position, index);
          }
          indexUtils2.addToIndex(index, term, factIndex);
        }
      });
    }
    this.logger.log("FACT_ADDED", {
      message: `Added fact at index ${factIndex}`,
      fact
    });
  }
};

// src/symmetric-relation.ts
import { eq } from "logic";
var SymmetricMemoryRelation = class {
  memoryRelation;
  constructor(logger, config) {
    this.memoryRelation = new MemoryRelation(logger, config);
  }
  createRelation() {
    const baseRelation = this.memoryRelation.createRelation();
    const origSet = baseRelation.set;
    const symGoal = (...query) => {
      if (query.length !== 2) {
        return eq(1, 0);
      }
      return baseRelation(...query);
    };
    symGoal.set = (...fact) => {
      if (fact.length === 2) {
        origSet(fact[0], fact[1]);
        origSet(fact[1], fact[0]);
        return;
      }
      throw Error("Symmetric Facts are Binary");
    };
    symGoal.raw = baseRelation.raw;
    symGoal.indexes = baseRelation.indexes;
    return symGoal;
  }
};
var SymmetricMemoryObjRelation = class {
  constructor(keys, logger, config) {
    this.keys = keys;
    if (keys.length !== 2) {
      throw new Error("Symmetric object relations must have exactly 2 keys");
    }
    this.memoryObjRelation = new MemoryObjRelation(keys, logger, config);
  }
  memoryObjRelation;
  createRelation() {
    const baseRelation = this.memoryObjRelation.createRelation();
    const origSet = baseRelation.set;
    const symGoal = (queryObj) => {
      return baseRelation(queryObj);
    };
    symGoal.set = (factObj) => {
      const [key1, key2] = this.keys;
      if (!(key1 in factObj) || !(key2 in factObj)) {
        throw new Error(
          `Symmetric object fact must have both keys: ${key1}, ${key2}`
        );
      }
      origSet(factObj);
      const reversedFact = {
        [key1]: factObj[key2],
        [key2]: factObj[key1],
        ...Object.fromEntries(
          Object.entries(factObj).filter(([k]) => k !== key1 && k !== key2)
        )
      };
      origSet(reversedFact);
    };
    symGoal.raw = baseRelation.raw;
    symGoal.indexes = baseRelation.indexes;
    symGoal.keys = baseRelation.keys;
    return symGoal;
  }
};

// src/relation-factory.ts
var FactRelationFactory = class {
  constructor(deps) {
    this.deps = deps;
  }
  createArrayRelation() {
    const relation = new MemoryRelation(this.deps.logger, this.deps.config);
    return relation.createRelation();
  }
  createObjectRelation(keys) {
    const relation = new MemoryObjRelation(
      keys,
      this.deps.logger,
      this.deps.config
    );
    return relation.createRelation();
  }
  createSymmetricArrayRelation() {
    const relation = new SymmetricMemoryRelation(
      this.deps.logger,
      this.deps.config
    );
    return relation.createRelation();
  }
  createSymmetricObjectRelation(keys) {
    const relation = new SymmetricMemoryObjRelation(
      keys,
      this.deps.logger,
      this.deps.config
    );
    return relation.createRelation();
  }
};

// src/facts-memory.ts
var makeFacts = (config, factConfig) => {
  const logger = getDefaultLogger();
  const factory = new FactRelationFactory({
    logger,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true
    }
  });
  return factory.createArrayRelation();
};
var makeFactsObj = (keys, config, factConfig) => {
  const logger = getDefaultLogger();
  const factory = new FactRelationFactory({
    logger,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true
    }
  });
  return factory.createObjectRelation(keys);
};
var makeFactsSym = (config, factConfig) => {
  const logger = getDefaultLogger();
  const factory = new FactRelationFactory({
    logger,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true
    }
  });
  return factory.createSymmetricArrayRelation();
};
var makeFactsObjSym = (keys, config, factConfig) => {
  const logger = getDefaultLogger();
  const factory = new FactRelationFactory({
    logger,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true
    }
  });
  return factory.createSymmetricObjectRelation(keys);
};
export {
  FactRelationFactory,
  MemoryObjRelation,
  MemoryRelation,
  SymmetricMemoryObjRelation,
  SymmetricMemoryRelation,
  makeFacts,
  makeFactsObj,
  makeFactsObjSym,
  makeFactsSym
};
//# sourceMappingURL=index.js.map