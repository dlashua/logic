import { Term, Subst, Goal } from "../core/types.ts";
import { isVar, walk, unify } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";
import { queryUtils, unificationUtils, indexUtils } from "../shared/utils.ts";
import { SimpleObservable } from "../core/observable.ts";
import { FactRelation, FactRelationConfig } from "./types.ts";

export class MemoryRelation {
  private facts: Term[][] = [];
  private indexes = new Map<number, Map<any, Set<number>>>();
  private goalIdCounter = 0;

  constructor(
    private logger: Logger,
    private config: FactRelationConfig
  ) {}

  createRelation(): FactRelation {
    const goalFn = (...query: Term[]): Goal => {
      const goalId = this.generateGoalId();
      return this.createGoal(query, goalId);
    };

    goalFn.set = (...fact: Term[]) => {
      this.addFact(fact);
    };

    goalFn.raw = this.facts;
    goalFn.indexes = this.indexes;

    return goalFn;
  }

  private generateGoalId(): number {
    return ++this.goalIdCounter;
  }

  private createGoal(query: Term[], goalId: number): Goal {
    return (input$) => new SimpleObservable<Subst>((observer) => {
      let cancelled = false;
      
      this.logger.log(
        "RUN_START",
        {
          message: `Starting memory relation goal ${goalId}`,
          query,
        }
      );

      const processAll = () => {
        try {
          // For each incoming substitution, process as before
          const subs: Subst[] = [];
          input$.subscribe({
            next: (s: Subst) => subs.push(s),
            complete: async () => {
              for (const s of subs) {
                if (cancelled) break;
                
                const walkedQuery = queryUtils.walkAllArray(query, s);
                
                // Try to use indexes for optimization
                const indexedPositions: number[] = [];
                walkedQuery.forEach((term, i) => {
                  if (!isVar(term) && this.indexes.has(i)) {
                    indexedPositions.push(i);
                  }
                });

                let candidateIndexes: Set<number> | null = null;
                
                if (indexedPositions.length > 0) {
                  this.logger.log(
                    "INDEX_LOOKUP",
                    {
                      message: `Using indexes for positions: ${indexedPositions.join(', ')}`,
                    }
                  );
                  
                  for (const pos of indexedPositions) {
                    const term = walkedQuery[pos];
                    const index = this.indexes.get(pos);
                    if (!index) continue;
                    
                    const factNums = index.get(term);
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
                      message: `Full scan of ${this.facts.length} facts`,
                    }
                  );
                  
                  // Process facts one by one with cancellation support
                  await this.processFacts(this.facts, query, s, observer, () => cancelled);
                } else {
                  this.logger.log(
                    "INDEX_LOOKUP",
                    {
                      message: `Checking ${candidateIndexes.size} indexed facts`,
                    }
                  );
                  
                  // Process indexed facts
                  const indexedFacts = Array.from(candidateIndexes).map(i => this.facts[i]);
                  await this.processFacts(indexedFacts, query, s, observer, () => cancelled);
                }
              }
              
              if (!cancelled) {
                this.logger.log(
                  "RUN_END",
                  {
                    message: `Completed memory relation goal ${goalId}`,
                  }
                );
                observer.complete?.();
              }
            },
            error: (err) => {
              if (!cancelled) observer.error?.(err);
            }
          });
        } catch (error) {
          if (!cancelled) {
            observer.error?.(error);
          }
        }
      };

      processAll();

      return () => {
        cancelled = true;
      };
    });
  }

  private async processFacts(
    facts: Term[][],
    query: Term[],
    s: Subst,
    observer: any,
    isCancelled: () => boolean
  ): Promise<void> {
    for (let i = 0; i < facts.length; i++) {
      if (isCancelled()) break;
      
      const fact = facts[i];
      const s1 = await unificationUtils.unifyArrays(query, fact, s);
      if (s1 && !isCancelled()) {
        this.logger.log(
          "FACT_MATCH",
          {
            message: "Fact matched",
            fact,
            query,
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

  private addFact(fact: Term[]): void {
    const factIndex = this.facts.length;
    this.facts.push(fact);

    if (this.config.enableIndexing !== false) {
      fact.forEach((term, position) => {
        if (indexUtils.isIndexable(term)) {
          let index = this.indexes.get(position);
          if (!index) {
            index = indexUtils.createIndex();
            this.indexes.set(position, index);
          }
          indexUtils.addToIndex(index, term, factIndex);
        }
      });
    }

    this.logger.log(
      "FACT_ADDED",
      {
        message: `Added fact at index ${factIndex}`,
        fact,
      }
    );
  }
}