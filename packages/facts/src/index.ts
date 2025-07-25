// Observable-based Facts System Export
// -----------------------------------------------------------------------------

export {
  makeFacts,
  makeFactsObj,
  makeFactsSym,
  makeFactsObjSym
} from './facts-memory.ts';

export type {
  FactRelation,
  FactObjRelation,
  FactRelationConfig,
  FactPattern,
  FactManagerDependencies
} from './types.ts';

export { FactRelationFactory } from './relation-factory.ts';
export { MemoryRelation } from './memory-relation.ts';
export { MemoryObjRelation } from './memory-obj-relation.ts';
export { SymmetricMemoryRelation, SymmetricMemoryObjRelation } from './symmetric-relation.ts';