import { Term, Subst, isVar } from "../core.ts";
import { Logger } from "../shared/logger.ts";
import { BaseCache } from "../shared/cache.ts";
import { queryUtils, unificationUtils, indexUtils } from "../shared/utils.ts";
import { GoalFunction } from "../shared/types.ts";
import { FactObjRelation, FactRelationConfig } from "./types.ts";

export class MemoryObjRelation {
  private facts: Record<string, Term>[] = [];
  private indexes = new Map<string, Map<any, Set<number>>>();
  private goalIdCounter = 0;

  constructor(
    private keys: string[],
    private logger: Logger,
    private cache: BaseCache,
    private config: FactRelationConfig
  ) {}

  createRelation(): FactObjRelation {
    const goalFn = (queryObj: Record<string, Term>): GoalFunction => {
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

  private createGoal(queryObj: Record<string, Term>, goalId: number): GoalFunction {
    return async function* (this: MemoryObjRelation, s: Subst) {
      this.logger.log("RUN_START", `Starting memory object relation goal ${goalId}`, { queryObj });

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
        this.logger.log("INDEX_LOOKUP", `Using indexes for keys: ${indexedKeys.join(', ')}`);
        
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
        this.logger.log("MEMORY_SCAN", `Full scan of ${this.facts.length} object facts`);
        
        for (const fact of this.facts) {
          const s1 = await unificationUtils.unifyRowWithWalkedQ(
            queryKeys,
            walkedQuery,
            fact,
            s
          );
          if (s1) {
            this.logger.log("FACT_MATCH", "Object fact matched", { fact, queryObj });
            yield s1;
          }
        }
      } else {
        this.logger.log("INDEX_LOOKUP", `Checking ${candidateIndexes.size} indexed object facts`);
        
        for (const factIndex of candidateIndexes) {
          const fact = this.facts[factIndex];
          const s1 = await unificationUtils.unifyRowWithWalkedQ(
            queryKeys,
            walkedQuery,
            fact,
            s
          );
          if (s1) {
            this.logger.log("FACT_MATCH", "Indexed object fact matched", { fact, queryObj });
            yield s1;
          }
        }
      }

      this.logger.log("RUN_END", `Completed memory object relation goal ${goalId}`);
    }.bind(this);
  }

  private addFact(factObj: Record<string, Term>): void {
    const factIndex = this.facts.length;
    const fact: Record<string, Term> = { ...factObj };
    this.facts.push(fact);

    if (this.config.enableIndexing !== false) {
      for (const [key, term] of Object.entries(fact)) {
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

    this.logger.log("FACT_ADDED", `Added object fact at index ${factIndex}`, { fact });
  }
}