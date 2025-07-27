// Observable-based Facts System Export
// -----------------------------------------------------------------------------

export {
  makeFacts,
  makeFactsObj,
  makeFactsObjSym,
  makeFactsSym,
} from "./facts-memory.js";
export { MemoryObjRelation } from "./memory-obj-relation.js";
export { MemoryRelation } from "./memory-relation.js";
export { FactRelationFactory } from "./relation-factory.js";
export {
  SymmetricMemoryObjRelation,
  SymmetricMemoryRelation,
} from "./symmetric-relation.js";
export type {
  FactManagerDependencies,
  FactObjRelation,
  FactPattern,
  FactRelation,
  FactRelationConfig,
} from "./types.ts";
