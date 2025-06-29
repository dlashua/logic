import { Term, or } from "../core.ts";
import { Logger } from "../shared/logger.ts";
import { BaseCache } from "../shared/cache.ts";
import { MemoryRelation } from "./memory-relation.ts";
import { MemoryObjRelation } from "./memory-obj-relation.ts";
import { FactRelation, FactObjRelation, FactRelationConfig } from "./types.ts";

export class SymmetricMemoryRelation {
  private memoryRelation: MemoryRelation;

  constructor(
    private logger: Logger,
    private cache: BaseCache,
    private config: FactRelationConfig
  ) {
    this.memoryRelation = new MemoryRelation(logger, cache, config);
  }

  createRelation(): FactRelation {
    const baseRelation = this.memoryRelation.createRelation();
    const origSet = baseRelation.set;

    const symGoal = (...query: Term[]) => {
      if (query.length === 2) {
        return or(baseRelation(query[0], query[1]), baseRelation(query[1], query[0]));
      }
      return baseRelation(...query);
    };

    symGoal.set = (...fact: Term[]) => {
      if (fact.length === 2) {
        origSet(fact[0], fact[1]);
        origSet(fact[1], fact[0]);
      } else {
        origSet(...fact);
      }
    };

    symGoal.raw = baseRelation.raw;
    symGoal.indexes = baseRelation.indexes;

    return symGoal as FactRelation;
  }
}

export class SymmetricMemoryObjRelation {
  private memoryObjRelation: MemoryObjRelation;

  constructor(
    private keys: string[],
    private logger: Logger,
    private cache: BaseCache,
    private config: FactRelationConfig
  ) {
    this.memoryObjRelation = new MemoryObjRelation(keys, logger, cache, config);
  }

  createRelation(): FactObjRelation {
    const baseRelation = this.memoryObjRelation.createRelation();

    const symGoal = (queryObj: Record<string, Term>) => {
      if (this.keys.length === 2) {
        const [k1, k2] = this.keys;
        const swapped: Record<string, Term> = {};
        swapped[k1] = queryObj[k2];
        swapped[k2] = queryObj[k1];
        return or(baseRelation(queryObj), baseRelation(swapped));
      }
      return baseRelation(queryObj);
    };

    Object.assign(symGoal, baseRelation);
    return symGoal as FactObjRelation;
  }
}