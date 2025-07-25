import { Logger } from "logic";
import { MemoryRelation } from "./memory-relation.js";
import { MemoryObjRelation } from "./memory-obj-relation.js";
import { SymmetricMemoryRelation, SymmetricMemoryObjRelation } from "./symmetric-relation.js";
import { FactRelation, FactObjRelation, FactRelationConfig } from "./types.js";

export interface FactRelationFactoryDependencies {
  logger: Logger;
  config: FactRelationConfig;
}

export class FactRelationFactory {
  constructor(private deps: FactRelationFactoryDependencies) {}

  createArrayRelation(): FactRelation {
    const relation = new MemoryRelation(
      this.deps.logger,
      this.deps.config
    );
    return relation.createRelation();
  }

  createObjectRelation(keys: string[]): FactObjRelation {
    const relation = new MemoryObjRelation(
      keys,
      this.deps.logger,
      this.deps.config
    );
    return relation.createRelation();
  }

  createSymmetricArrayRelation(): FactRelation {
    const relation = new SymmetricMemoryRelation(
      this.deps.logger,
      this.deps.config
    );
    return relation.createRelation();
  }

  createSymmetricObjectRelation(keys: string[]): FactObjRelation {
    const relation = new SymmetricMemoryObjRelation(
      keys,
      this.deps.logger,
      this.deps.config
    );
    return relation.createRelation();
  }
}