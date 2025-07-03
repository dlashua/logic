import { Term } from "../core/types.ts";
import { eq, or } from "../core/combinators.ts";
import { Logger } from "../shared/logger.ts";
import { MemoryRelation } from "./memory-relation.ts";
import { MemoryObjRelation } from "./memory-obj-relation.ts";
import { FactRelation, FactObjRelation, FactRelationConfig } from "./types.ts";

export class SymmetricMemoryRelation {
  private memoryRelation: MemoryRelation;

  constructor(
    logger: Logger,
    config: FactRelationConfig
  ) {
    this.memoryRelation = new MemoryRelation(logger, config);
  }

  createRelation(): FactRelation {
    const baseRelation = this.memoryRelation.createRelation();
    const origSet = baseRelation.set;

    const symGoal = (...query: Term[]) => {
      if (query.length !== 2) {
        return eq(1,0); //fail
      }
      return baseRelation(...query);
    };

    symGoal.set = (...fact: Term[]) => {
      if (fact.length === 2) {
        origSet(fact[0], fact[1]);
        origSet(fact[1], fact[0]);
        return;
      }
      throw Error("Symmetric Facts are Binary");
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
    logger: Logger,
    config: FactRelationConfig
  ) {
    this.memoryObjRelation = new MemoryObjRelation(keys, logger, config);
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
      return eq(1,0); // fail
    };

    Object.assign(symGoal, baseRelation);
    return symGoal as FactObjRelation;
  }
}