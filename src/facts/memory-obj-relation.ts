import { Term, Subst, Goal } from "../core/types.ts";
import { isVar } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";
import { queryUtils, unificationUtils, indexUtils } from "../shared/utils.ts";
import { SimpleObservable } from "../core/observable.ts";
import { FactObjRelation, FactRelationConfig } from "./types.ts";

export class MemoryObjRelation {
  private facts: Record<string, Term>[] = [];
  private indexes = new Map<string, Map<any, Set<number>>>();
  private goalIdCounter = 0;

  constructor(
    private keys: string[],
    private logger: Logger,
    private config: FactRelationConfig
  ) {}

  createRelation(): FactObjRelation {
    const goalFn = (queryObj: Record<string, Term>): Goal => {
      const goalId = this.generateGoalId();
      return this.createGoal(queryObj, goalId);
    };

    goalFn.set = (factObj: Record<string, Term>) => {
      this.addFact(factObj);
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
    return (s: Subst) => new SimpleObservable<Subst>((observer) => {
      let cancelled = false;
      
      this.logger.log(
        "RUN_START",
        {
          message: `Starting memory object relation goal ${goalId}`,
          queryObj,
        }
      );

      const processQuery = async () => {
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
            this.logger.log(
              "INDEX_LOOKUP",
              {
                message: `Using indexes for keys: ${indexedKeys.join(', ')}`,
              }
            );
            
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
                candidateIndexes = indexUtils.intersect(candidateIndexes, factNums);
                if (candidateIndexes.size === 0) break;
              }
            }
          }

          if (candidateIndexes === null) {
            this.logger.log(
              "MEMORY_SCAN",
              {
                message: `Full scan of ${this.facts.length} object facts`,
              }
            );
            
            // Process all facts with cancellation support
            await this.processFacts(this.facts, queryObj, walkedQuery, queryKeys, s, observer, () => cancelled);
          } else {
            this.logger.log(
              "INDEX_LOOKUP",
              {
                message: `Checking ${candidateIndexes.size} indexed object facts`,
              }
            );
            
            // Process indexed facts
            const indexedFacts = Array.from(candidateIndexes).map(i => this.facts[i]);
            await this.processFacts(indexedFacts, queryObj, walkedQuery, queryKeys, s, observer, () => cancelled);
          }

          if (!cancelled) {
            this.logger.log(
              "RUN_END",
              {
                message: `Completed memory object relation goal ${goalId}`,
              }
            );
            observer.complete?.();
          }
        } catch (error) {
          if (!cancelled) {
            observer.error?.(error);
          }
        }
      };

      processQuery();

      return () => {
        cancelled = true;
      };
    });
  }

  private async processFacts(
    facts: Record<string, Term>[],
    queryObj: Record<string, Term>,
    walkedQuery: Record<string, Term>,
    queryKeys: string[],
    s: Subst,
    observer: any,
    isCancelled: () => boolean
  ): Promise<void> {
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
        this.logger.log(
          "FACT_MATCH",
          {
            message: "Object fact matched",
            fact,
            queryObj,
          }
        );
        observer.next(s1);
      }
      
      // Yield control periodically to allow cancellation
      if (i % 10 === 0) {
        await new Promise(resolve => queueMicrotask(() => resolve(null)));
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

    this.logger.log(
      "FACT_ADDED",
      {
        message: `Added object fact at index ${factIndex}`,
        fact: factObj,
      }
    );
  }
}