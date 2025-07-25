import { query } from "../query.ts";
import { eq } from "../core/combinators.ts";
import { cspSolver, allDifferentConstraint, equalConstraint, notEqualConstraint } from "../core/csp-solver.ts";

console.log("=== Testing CSP Solver ===");

// Simple test: 3 variables with domains [1,2,3], all must be different
const result1 = await query()
  .where($ => [
    cspSolver(
      [
        { id: "x", domain: [1, 2, 3]},
        { id: "y", domain: [1, 2, 3]},
        { id: "z", domain: [1, 2, 3]}
      ],
      [
        allDifferentConstraint(["x", "y", "z"])
      ]
    )
  ]).toArray();

console.log("All different constraint results:", result1.length);

// Test with additional constraints
const result2 = await query()
  .where($ => [
    cspSolver(
      [
        { id: "a", domain: [1, 2]},
        { id: "b", domain: [1, 2]},
      ],
      [
        notEqualConstraint("a", "b")
      ]
    )
  ]).toArray();

console.log("Not equal constraint results:", result2.length);

// Test impossible constraint
const result3 = await query()
  .where($ => [
    cspSolver(
      [
        { id: "p", domain: [1]},
        { id: "q", domain: [1]},
      ],
      [
        notEqualConstraint("p", "q")
      ]
    )
  ]).toArray();

console.log("Impossible constraint results:", result3.length);
