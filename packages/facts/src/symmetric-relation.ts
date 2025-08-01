import { eq, type Logger, type Term } from "@swiftfall/logic";
import { MemoryObjRelation } from "./memory-obj-relation.js";
import { MemoryRelation } from "./memory-relation.js";
import type {
  FactObjRelation,
  FactRelation,
  FactRelationConfig,
} from "./types.js";

export class SymmetricMemoryRelation {
  private memoryRelation: MemoryRelation;

  constructor(logger: Logger, config: FactRelationConfig) {
    this.memoryRelation = new MemoryRelation(logger, config);
  }

  createRelation(): FactRelation {
    const baseRelation = this.memoryRelation.createRelation();
    const origSet = baseRelation.set;

    const symGoal = (...query: Term[]) => {
      if (query.length !== 2) {
        return eq(1, 0); // fail
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
    config: FactRelationConfig,
  ) {
    if (keys.length !== 2) {
      throw new Error("Symmetric object relations must have exactly 2 keys");
    }
    this.memoryObjRelation = new MemoryObjRelation(keys, logger, config);
  }

  createRelation(): FactObjRelation {
    const baseRelation = this.memoryObjRelation.createRelation();
    const origSet = baseRelation.set;

    const symGoal = (queryObj: Record<string, Term>) => {
      return baseRelation(queryObj);
    };

    symGoal.set = (factObj: Record<string, Term>) => {
      const [key1, key2] = this.keys;

      if (!(key1 in factObj) || !(key2 in factObj)) {
        throw new Error(
          `Symmetric object fact must have both keys: ${key1}, ${key2}`,
        );
      }

      // Add both directions
      origSet(factObj);

      // Add reversed fact
      const reversedFact = {
        [key1]: factObj[key2],
        [key2]: factObj[key1],
        ...Object.fromEntries(
          Object.entries(factObj).filter(([k]) => k !== key1 && k !== key2),
        ),
      };
      origSet(reversedFact);
    };

    symGoal.raw = baseRelation.raw;
    symGoal.indexes = baseRelation.indexes;
    symGoal.keys = baseRelation.keys;

    return symGoal as FactObjRelation;
  }
}
