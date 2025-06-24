// logic_lib.ts has been refactored into multiple files: core.ts, relation.ts, run.ts, facts.ts
// This file now re-exports all public APIs for backward compatibility.

export * from "./core.ts";
export * from "./facts.ts";
export * from "./facts-sql.ts";
export * from "./relations.ts";
export * from "./relations-agg.ts";
export * from "./relations-ex.ts";
export * from "./relations-list.ts";
export * from "./run.ts";
// export { and_noopt } from "./relations.ts";
