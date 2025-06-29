import { Logger } from "../shared/logger.ts";
import { BaseCache } from "../shared/cache.ts";
import { MemoryRelation } from "./memory-relation.ts";
import { MemoryObjRelation } from "./memory-obj-relation.ts";
import { SymmetricMemoryRelation, SymmetricMemoryObjRelation } from "./symmetric-relation.ts";
import { FactRelation, FactObjRelation, FactRelationConfig } from "./types.ts";

export interface FactRelationFactoryDependencies {
  logger: Logger;
  cache: BaseCache;
  config: FactRelationConfig;
}

export class FactRelationFactory {
  constructor(private deps: FactRelationFactoryDependencies) {}

  createArrayRelation(): FactRelation {
    const relation = new MemoryRelation(
      this.deps.logger,
      this.deps.cache,
      this.deps.config
    );
    return relation.createRelation();
  }

  createObjectRelation(keys: string[]): FactObjRelation {
    const relation = new MemoryObjRelation(
      keys,
      this.deps.logger,
      this.deps.cache,
      this.deps.config
    );
    return relation.createRelation();
  }

  createSymmetricArrayRelation(): FactRelation {
    const relation = new SymmetricMemoryRelation(
      this.deps.logger,
      this.deps.cache,
      this.deps.config
    );
    return relation.createRelation();
  }

  createSymmetricObjectRelation(keys: string[]): FactObjRelation {
    const relation = new SymmetricMemoryObjRelation(
      keys,
      this.deps.logger,
      this.deps.cache,
      this.deps.config
    );
    return relation.createRelation();
  }
}