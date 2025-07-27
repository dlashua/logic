import {
  type Goal,
  indexUtils,
  isVar,
  type Logger,
  queryUtils,
  SimpleObservable,
  type Subst,
  type Term,
  unificationUtils,
} from "logic";
import type { FactObjRelation, FactRelationConfig } from "./types.js";

export class MemoryObjRelation {
  /**
   * Update facts matching a where-clause with new values.
   * @param where - fields and values to match
   * @param newValues - fields and values to update
   */
  private updateFacts(
    where: Partial<Record<string, Term>>,
    newValues: Record<string, Term>,
    upsert: boolean = false,
  ): void {
    let updated = false;
    for (let i = 0; i < this.facts.length; i++) {
      const fact = this.facts[i];
      let match = true;
      for (const key of Object.keys(where)) {
        // Use strict equality for now; can swap for deep equality or unification if needed
        if (fact[key] !== where[key]) {
          match = false;
          break;
        }
      }
      if (match) {
        updated = true;
        // Remove old index entries for changed keys
        if (this.config.enableIndexing !== false) {
          for (const key of this.keys) {
            if (
              key in newValues &&
              key in fact &&
              indexUtils.isIndexable(fact[key])
            ) {
              const index = this.indexes.get(key);
              if (index) {
                this.removeFromIndex(index, fact[key], i);
              }
            }
          }
        }
        // Update fact fields
        Object.assign(fact, newValues);
        // Add new index entries for changed keys
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
          fact,
        });
      }
    }
    // If no record was updated and upsert is true, insert a new one
    if (!updated && upsert) {
      const newFact: Record<string, Term> = { ...where, ...newValues };
      this.addFact(newFact);
      this.logger.log("FACT_INSERTED", {
        message: `Inserted new object fact (upsert)`,
        fact: newFact,
      });
    }
  }

  /**
   * Remove a fact index from a Map<value, Set<index>>
   */
  private removeFromIndex(
    index: Map<unknown, Set<number>>,
    value: unknown,
    factIndex: number,
  ): void {
    const set = index.get(value);
    if (set) {
      set.delete(factIndex);
      if (set.size === 0) {
        index.delete(value);
      }
    }
  }
  private facts: Record<string, Term>[] = [];
  private indexes: Map<string, Map<unknown, Set<number>>> = new Map();
  private goalIdCounter = 0;

  constructor(
    private keys: string[],
    private logger: Logger,
    private config: FactRelationConfig,
  ) {}

  createRelation(): FactObjRelation {
    const goalFn = (queryObj: Record<string, Term>): Goal => {
      const goalId = this.generateGoalId();
      return this.createGoal(queryObj, goalId);
    };

    goalFn.set = (factObj: Record<string, Term>) => {
      this.addFact(factObj);
    };

    goalFn.update = (
      where: Partial<Record<string, Term>>,
      newValues: Record<string, Term>,
    ) => {
      this.updateFacts(where, newValues, false);
    };

    goalFn.upsert = (
      where: Partial<Record<string, Term>>,
      newValues: Record<string, Term>,
    ) => {
      this.updateFacts(where, newValues, true);
    };

    goalFn.raw = this.facts;
    goalFn.indexes = this.indexes;
    goalFn.keys = this.keys;

    return goalFn;
  }

  private generateGoalId(): number {
    return ++this.goalIdCounter;
  }

  private createGoal(queryObj: Record<string, Term>, goalId: number): Goal {
    return (input$) =>
      new SimpleObservable<Subst>((observer) => {
        let cancelled = false;
        this.logger.log("RUN_START", {
          message: `Starting memory object relation goal ${goalId}`,
          queryObj,
        });
        this.logger.log("STARTING PROCESS ALL", {
          goalId,
        });
        const subscription = input$.subscribe({
          next: async (s: Subst) => {
            try {
              const queryKeys = Object.keys(queryObj);
              const walkedQuery = await queryUtils.walkAllKeys(queryObj, s);
              // Find indexable, grounded keys
              const indexedKeys: string[] = [];
              for (const key of queryKeys) {
                if (!isVar(walkedQuery[key]) && this.indexes.has(key)) {
                  indexedKeys.push(key);
                }
              }
              let candidateIndexes: Set<number> | null = null;
              if (indexedKeys.length > 0) {
                this.logger.log("INDEX_LOOKUP", {
                  message: `Using indexes for keys: ${indexedKeys.join(", ")}`,
                });
                for (const key of indexedKeys) {
                  const value = walkedQuery[key];
                  const index = this.indexes.get(key);
                  if (!index) continue;
                  const factNums = index.get(value);
                  if (!factNums || factNums.size === 0) {
                    candidateIndexes = new Set();
                    break;
                  }
                  if (candidateIndexes === null) {
                    candidateIndexes = new Set(factNums);
                  } else {
                    candidateIndexes = indexUtils.intersect(
                      candidateIndexes,
                      factNums,
                    );
                    if (candidateIndexes.size === 0) break;
                  }
                }
              }
              if (candidateIndexes === null) {
                this.logger.log("MEMORY_SCAN", {
                  message: `Full scan of ${this.facts.length} object facts`,
                });
                await this.processFacts(
                  this.facts,
                  queryObj,
                  walkedQuery,
                  queryKeys,
                  s,
                  observer,
                  () => cancelled,
                );
              } else {
                this.logger.log("INDEX_LOOKUP", {
                  message: `Checking ${candidateIndexes.size} indexed object facts`,
                });
                const indexedFacts = Array.from(candidateIndexes).map(
                  (i) => this.facts[i],
                );
                await this.processFacts(
                  indexedFacts,
                  queryObj,
                  walkedQuery,
                  queryKeys,
                  s,
                  observer,
                  () => cancelled,
                );
              }
            } catch (err) {
              if (!cancelled) observer.error?.(err);
            }
          },
          complete: () => {
            if (!cancelled) {
              this.logger.log("RUN_END", {
                message: `Completed memory object relation goal ${goalId}`,
              });
              observer.complete?.();
            }
          },
          error: (err) => {
            if (!cancelled) observer.error?.(err);
          },
        });
        return () => {
          cancelled = true;
          subscription.unsubscribe?.();
        };
      });
  }

  private async processFacts(
    facts: Record<string, Term>[],
    queryObj: Record<string, Term>,
    walkedQuery: Record<string, Term>,
    queryKeys: string[],
    s: Subst,
    observer: {
      next: (s: Subst) => void;
      complete?: () => void;
      error?: (err: unknown) => void;
    },
    isCancelled: () => boolean,
  ): Promise<void> {
    for (let i = 0; i < facts.length; i++) {
      if (isCancelled()) break;

      const fact = facts[i];
      const s1 = await unificationUtils.unifyRowWithWalkedQ(
        queryKeys,
        walkedQuery,
        fact,
        s,
      );
      if (s1 && !isCancelled()) {
        this.logger.log("FACT_MATCH", {
          message: "Object fact matched",
          fact,
          queryObj,
        });
        observer.next(s1);
      }

      // Yield control periodically to allow cancellation
      if (i % 10 === 0) {
        await new Promise((resolve) => queueMicrotask(() => resolve(null)));
      }
    }
  }

  private addFact(factObj: Record<string, Term>): void {
    const factIndex = this.facts.length;
    this.facts.push(factObj);

    if (this.config.enableIndexing !== false) {
      // Index by keys
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
      fact: factObj,
    });
  }
}
