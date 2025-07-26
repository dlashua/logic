// Observable-based Facts System Export
// -----------------------------------------------------------------------------

export {
	makeFacts,
	makeFactsObj,
	makeFactsObjSym,
	makeFactsSym,
} from "./facts-memory.ts";
export { MemoryObjRelation } from "./memory-obj-relation.ts";
export { MemoryRelation } from "./memory-relation.ts";
export { FactRelationFactory } from "./relation-factory.ts";
export {
	SymmetricMemoryObjRelation,
	SymmetricMemoryRelation,
} from "./symmetric-relation.ts";
export type {
	FactManagerDependencies,
	FactObjRelation,
	FactPattern,
	FactRelation,
	FactRelationConfig,
} from "./types.ts";
