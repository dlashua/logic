import { beforeEach, describe, expect, it } from "vitest";
import { and, eq, or, run } from "../core/combinators.js";
import { lvar, resetVarCounter, walk } from "../core/kernel.js";
import { aggregateVar, aggregateVarMulti } from "./procedural-helpers.js";

describe("Procedural Helpers", () => {
	beforeEach(() => {
		resetVarCounter();
	});

	describe("aggregateVar", () => {
		it("should aggregate variable values into array", async () => {
			const x = lvar("x");
			const goal = aggregateVar(x, or(eq(x, 1), eq(x, 2), eq(x, 3)));

			const results = (await run(goal)).results.map((x) => x.get("x_0"));

			expect(results).toEqual([[1, 2, 3]]);
		});

		it("should work with empty results", async () => {
			const x = lvar("x");
			const goal = aggregateVar(
				x,
				and(eq(x, 1), eq(x, 2)), // impossible
			);

			const results = (await run(goal)).results.map((x) => x.get("x_0"));

			expect(results).toEqual([[]]);
		});
	});

	describe("aggregateVarMulti", () => {
		it("should aggregate multiple variables by groups", async () => {
			const groupVar = lvar("group");
			const valueVar = lvar("value");
			const goal = aggregateVarMulti(
				[groupVar],
				[valueVar],
				or(
					and(eq(groupVar, "A"), eq(valueVar, 1)),
					and(eq(groupVar, "A"), eq(valueVar, 2)),
					and(eq(groupVar, "B"), eq(valueVar, 3)),
				),
			);

			const results = (await run(goal)).results.map((x) => ({
				group: x.get("group_0"),
				values: x.get("value_1"),
			}));

			// Sort for consistent comparison
			results.sort((a, b) =>
				(a.group as string).localeCompare(b.group as string),
			);

			expect(results).toEqual([
				{
					group: "A",
					values: [1, 2],
				},
				{
					group: "B",
					values: [3],
				},
			]);
		});

		it("should handle empty results", async () => {
			const groupVar = lvar("group");
			const valueVar = lvar("value");
			const goal = aggregateVarMulti(
				[groupVar],
				[valueVar],
				and(eq(groupVar, "A"), eq(groupVar, "B")), // impossible
			);

			const results = (await run(goal)).results.map((x) => x.get("value_1"));

			expect(results).toEqual([[]]);
		});
	});
});
