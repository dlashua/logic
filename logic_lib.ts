// logic_lib.ts has been refactored into multiple files: core.ts, relation.ts, run.ts, facts.ts
// This file now re-exports all public APIs for backward compatibility.

export * from './core.ts';
export * from './relation.ts';
export * from './run.ts';
export * from './facts.ts';