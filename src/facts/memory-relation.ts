import { Term, Subst } from "../core/types.ts";
import { isVar , unify } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";
import { BaseCache } from "../shared/cache.ts";
import { queryUtils, unificationUtils, indexUtils } from "../shared/utils.ts";
import { GoalFunction } from "../shared/types.ts";
import { FactRelation, FactRelationConfig } from "./types.ts";

export class MemoryRelation {
  private facts: Term[][] = [];
  private indexes = new Map<number, Map<any, Set<number>>>();
  private goalIdCounter = 0;

  constructor(
    private logger: Logger,
    private cache: BaseCache,
    private config: FactRelationConfig
  ) {}

  createRelation(): FactRelation {
    const goalFn = (...query: Term[]): GoalFunction => {
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

  private createGoal(query: Term[], goalId: number): GoalFunction {
    return async function* (this: MemoryRelation, s: Subst) {
      this.logger.log("RUN_START", `Starting memory relation goal ${goalId}`, {
        query 
      });

      const walkedQuery = await queryUtils.walkAllArray(query, s);
      
      // Try to use indexes for optimization
      const indexedPositions: number[] = [];
      walkedQuery.forEach((term, i) => {
        if (!isVar(term) && this.indexes.has(i)) {
          indexedPositions.push(i);
        }
      });

      let candidateIndexes: Set<number> | null = null;
      
      if (indexedPositions.length > 0) {
        this.logger.log("INDEX_LOOKUP", `Using indexes for positions: ${indexedPositions.join(', ')}`);
        
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
        this.logger.log("MEMORY_SCAN", `Full scan of ${this.facts.length} facts`);
        
        for (const fact of this.facts) {
          const s1 = await unificationUtils.unifyArrays(query, fact, s);
          if (s1) {
            this.logger.log("FACT_MATCH", "Fact matched", {
              fact,
              query 
            });
            yield s1;
          }
        }
      } else {
        this.logger.log("INDEX_LOOKUP", `Checking ${candidateIndexes.size} indexed facts`);
        
        for (const factIndex of candidateIndexes) {
          const fact = this.facts[factIndex];
          const s1 = await unificationUtils.unifyArrays(query, fact, s);
          if (s1) {
            this.logger.log("FACT_MATCH", "Indexed fact matched", {
              fact,
              query 
            });
            yield s1;
          }
        }
      }

      this.logger.log("RUN_END", `Completed memory relation goal ${goalId}`);
    }.bind(this);
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

    this.logger.log("FACT_ADDED", `Added fact at index ${factIndex}`, {
      fact 
    });
  }
}