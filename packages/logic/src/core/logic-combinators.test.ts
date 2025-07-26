import { beforeEach, describe, expect, it } from "vitest";
import { and, eq, or, run } from "./combinators.js";
import { lvar, resetVarCounter } from "./kernel.js";
import { SimpleObservable } from "./observable.js";
import { query } from "./query.js";

describe("Logic Combinators", () => {
	beforeEach(() => {
		resetVarCounter();
	});

	describe("eq (equality)", () => {
		it("should unify identical values", async () => {
			const goal = eq(42, 42);
			const result = await run(goal);

			expect(result.results).toHaveLength(1);
			expect(result.completed).toBe(true);
		});

		it("should unify variable with value", async () => {
			const results = await query()
				.select(($) => ({
					x: $.x,
				}))
				.where(($) => eq($.x, 42))
				.toArray();

			expect(results).toHaveLength(1);
			expect(results[0].x).toBe(42);
		});

		it("should unify variable with value (direct substitution test)", async () => {
			const x = lvar("x");
			const goal = eq(x, 42);
			const result = await run(goal);

			expect(result.results).toHaveLength(1);
			expect(result.results[0].get(x.id)).toBe(42);
		});

		it("should fail to unify different values", async () => {
			const goal = eq(42, 43);
			const result = await run(goal);

			expect(result.results).toHaveLength(0);
			expect(result.completed).toBe(true);
		});

		it("should unify two variables", async () => {
			const results = await query()
				.select(($) => ({
					x: $.x,
					y: $.y,
				}))
				.where(($) => eq($.x, $.y))
				.toArray();

			expect(results).toHaveLength(1);
			// Both variables should be bound to the same value (one to the other)
			expect(results[0].x).toBe(results[0].y);
		});

		it("should unify two variables (direct substitution test)", async () => {
			const x = lvar("x");
			const y = lvar("y");
			const goal = eq(x, y);
			const result = await run(goal);

			expect(result.results).toHaveLength(1);
			// One variable should be bound to the other
			const subst = result.results[0];
			expect(subst.has(x.id) || subst.has(y.id)).toBe(true);
		});
	});

	describe("and (conjunction)", () => {
		it("should succeed when all goals succeed", async () => {
			const results = await query()
				.select(($) => ({
					x: $.x,
					y: $.y,
				}))
				.where(($) => and(eq($.x, 42), eq($.y, "hello")))
				.toArray();

			expect(results).toHaveLength(1);
			expect(results[0].x).toBe(42);
			expect(results[0].y).toBe("hello");
		});

		it("should fail when any goal fails", async () => {
			const results = await query()
				.select(($) => ({
					x: $.x,
				}))
				.where(($) =>
					and(
						eq($.x, 42),
						eq($.x, 43), // This should fail
					),
				)
				.toArray();

			expect(results).toHaveLength(0);
		});

		it("should handle variable propagation", async () => {
			const results = await query()
				.select(($) => ({
					x: $.x,
					y: $.y,
					z: $.z,
				}))
				.where(($) => and(eq($.x, $.y), eq($.y, $.z), eq($.z, 42)))
				.toArray();

			expect(results).toHaveLength(1);
			// All variables should eventually resolve to 42
			expect(results[0].x).toBe(42);
			expect(results[0].y).toBe(42);
			expect(results[0].z).toBe(42);
		});

		it("should handle variable propagation (direct substitution test)", async () => {
			const x = lvar("x");
			const y = lvar("y");
			const z = lvar("z");
			const goal = and(eq(x, y), eq(y, z), eq(z, 42));
			const result = await run(goal);

			expect(result.results).toHaveLength(1);
			// All variables should eventually resolve to 42
			const subst = result.results[0];
			expect(subst.get(z.id)).toBe(42);
		});
	});

	describe("or (disjunction)", () => {
		it("should succeed with multiple solutions", async () => {
			const results = await query()
				.select(($) => ({
					x: $.x,
				}))
				.where(($) => or(eq($.x, 42), eq($.x, 43)))
				.toArray();

			expect(results).toHaveLength(2);
			const values = results.map((r) => r.x).sort();
			expect(values).toEqual([42, 43]);
		});

		it("should succeed with at least one solution", async () => {
			const results = await query()
				.select(($) => ({
					x: $.x,
				}))
				.where(($) =>
					or(
						eq($.x, 42),
						eq(42, 43), // This fails but first succeeds
					),
				)
				.toArray();

			expect(results).toHaveLength(1);
			expect(results[0].x).toBe(42);
		});

		it("should fail when all goals fail", async () => {
			const goal = or(eq(42, 43), eq("hello", "world"));
			const result = await run(goal);

			expect(result.results).toHaveLength(0);
			expect(result.completed).toBe(true);
		});
	});

	describe("complex combinations", () => {
		it("should handle nested and/or correctly", async () => {
			const results = await query()
				.select(($) => ({
					x: $.x,
					y: $.y,
				}))
				.where(($) =>
					and(or(eq($.x, 1), eq($.x, 2)), or(eq($.y, "a"), eq($.y, "b"))),
				)
				.toArray();

			expect(results).toHaveLength(4); // Cartesian product: 2 * 2
			const pairs = results.map((r) => [r.x, r.y]).sort();
			expect(pairs).toEqual([
				[1, "a"],
				[1, "b"],
				[2, "a"],
				[2, "b"],
			]);
		});

		it("should handle nested and/or correctly (direct substitution test)", async () => {
			const x = lvar("x");
			const y = lvar("y");
			const goal = and(or(eq(x, 1), eq(x, 2)), or(eq(y, "a"), eq(y, "b")));
			const result = await run(goal);

			expect(result.results).toHaveLength(4); // Cartesian product: 2 * 2
			const pairs = result.results
				.map((subst) => [subst.get(x.id), subst.get(y.id)])
				.sort();
			expect(pairs).toEqual([
				[1, "a"],
				[1, "b"],
				[2, "a"],
				[2, "b"],
			]);
		});
	});
});
