import { and, or, eq } from "../core/combinators.ts";
import { query } from "../query.ts";
import { cspSolver, allDifferentConstraint } from "../core/csp-solver.ts";

console.log("=== Simple CSP Integration Test ===");

const results = await query()
  .where($ => [
    eq($.x, "solution_x"),
    eq($.y, "solution_y"),
  ]).toArray();

console.log("Basic query results:", results.length);
console.dir(results[0], { depth: 2 });
