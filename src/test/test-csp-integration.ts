import { and, eq, or } from "../core/combinators.ts";
import { allDifferentConstraint, cspSolver } from "../core/csp-solver.ts";
import { query } from "../query.ts";

console.log("=== Simple CSP Integration Test ===");

const results = await query()
	.where(($) => [eq($.x, "solution_x"), eq($.y, "solution_y")])
	.toArray();

console.log("Basic query results:", results.length);
console.dir(results[0], { depth: 2 });
