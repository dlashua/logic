import { beforeEach, describe, expect, it } from "vitest";
import { eq } from "../core/combinators.js";
import {
	arrayToLogicList,
	resetVarCounter,
	unify,
	walk,
} from "../core/kernel.js";
import { query } from "../core/query.js";
import type { Subst, Term } from "../core/types.js";
import {
	collect_and_process_base,
	group_by_streamo_base,
} from "./aggregates-base.js";
import { membero } from "./lists.js";

describe("Aggregates Base Functions", () => {
	beforeEach(() => {
		resetVarCounter();
	});

	describe("collect_and_process_base", () => {
		it("should collect all substitutions before processing", async () => {
			// Create a goal that uses collect_and_process_base to count substitutions
			const countGoal = (countTerm: Term<number>) =>
				collect_and_process_base((buffer, observer) => {
					const s = new Map() as Subst;
					const count = buffer.length;
					const newSubst = unify(countTerm, count, s);
					if (newSubst) observer.next(newSubst);
				});
			const results = await query()
				.select(($) => ({
					result: $.result,
				}))
				.where(($) => [membero($.x, [1, 2, 3]), countGoal($.result)])
				.toArray();

			expect(results).toEqual([
				{
					result: 3,
				}, // Should process all 3 substitutions at once
			]);
		});

		it("should handle empty streams", async () => {
			// Create a goal that uses collect_and_process_base to count substitutions
			const countGoal = (countTerm: Term<number>) =>
				collect_and_process_base((buffer, observer) => {
					const s = new Map() as Subst;
					const count = buffer.length;
					const newSubst = unify(countTerm, count, s);
					if (newSubst) observer.next(newSubst);
				});
			const results = await query()
				.select(($) => ({
					result: $.result,
				}))
				.where(($) => [
					eq($.x, "nonexistent"),
					eq($.x, "different"), // Impossible constraint
					countGoal($.result),
				])
				.toArray();

			expect(results).toEqual([
				{
					result: 0,
				},
			]);
		});

		it("should pass all substitutions with their variables intact", async () => {
			// Processor that collects all x values from the buffer and preserves context
			const collectXGoal = (xVar: Term, collectedTerm: Term) =>
				collect_and_process_base((buffer, observer) => {
					if (buffer.length > 0) {
						// Start with the first substitution to preserve existing variable bindings
						const baseSubst = new Map(buffer[0]) as Subst;
						const xValues = buffer
							.map((subst) => walk(xVar, subst))
							.filter((v) => v !== undefined);
						const logicList = arrayToLogicList(xValues);
						const newSubst = unify(collectedTerm, logicList, baseSubst);
						if (newSubst) observer.next(newSubst);
					}
				});
			const results = await query()
				.select(($) => ({
					collected: $.collected,
					y: $.y,
				}))
				.where(($) => [
					membero($.x, [1, 2, 3]),
					eq($.y, "constant"),
					collectXGoal($.x, $.collected),
				])
				.toArray();

			expect(results).toEqual([
				{
					collected: [1, 2, 3],
					y: "constant",
				}, // Preserves y variable from original stream
			]);
		});

		it("should handle processor that emits multiple results", async () => {
			const multiEmitGoal = (resultTerm: Term<number>) =>
				collect_and_process_base((buffer, observer) => {
					// Emit each substitution count
					for (let i = 0; i < buffer.length; i++) {
						const s = new Map() as Subst;
						const newSubst = unify(resultTerm, i + 1, s);
						if (newSubst) observer.next(newSubst);
					}
				});
			const results = await query()
				.select(($) => ({
					result: $.result,
				}))
				.where(($) => [membero($.x, [1, 2]), multiEmitGoal($.result)])
				.toArray();

			expect(results).toEqual([
				{
					result: 1,
				},
				{
					result: 2,
				},
			]);
		});

		it("should omit specific values from stream", async () => {
			// Goal that filters out a specific value from a variable in the stream
			const filterGoal = (xVar: Term, filterValue: any, outputTerm: Term) =>
				collect_and_process_base((buffer, observer) => {
					const s = new Map() as Subst;
					const filteredValues = buffer
						.map((subst) => walk(xVar, subst))
						.filter((v) => v !== undefined && v !== filterValue);
					const logicList = arrayToLogicList(filteredValues);
					const newSubst = unify(outputTerm, logicList, s);
					if (newSubst) observer.next(newSubst);
				});
			const results = await query()
				.select(($) => ({
					filtered: $.filtered,
				}))
				.where(($) => [
					membero($.x, [1, 2, 3, 2, 4]),
					filterGoal($.x, 2, $.filtered), // Filter out value 2
				])
				.toArray();

			expect(results).toEqual([
				{
					filtered: [1, 3, 4], // Should exclude both 2s
				},
			]);
		});

		it("should repeat each substitution with counter", async () => {
			// Goal that repeats each substitution N times and adds a counter
			const repeatGoal = (_timesTerm: Term<number>, counterTerm: Term<number>) =>
				collect_and_process_base((buffer, observer) => {
					const timesToRepeat = 3; // Repeat each 3 times
					for (const subst of buffer) {
						for (let i = 0; i < timesToRepeat; i++) {
							const newSubst = new Map(subst) as Subst;
							const withCounter = unify(counterTerm, i + 1, newSubst);
							if (withCounter) observer.next(withCounter);
						}
					}
				});
			const results = await query()
				.select(($) => ({
					x: $.x,
					counter: $.counter,
				}))
				.where(($) => [membero($.x, [1, 2]), repeatGoal($.times, $.counter)])
				.toArray();

			expect(results).toEqual([
				{
					x: 1,
					counter: 1,
				},
				{
					x: 1,
					counter: 2,
				},
				{
					x: 1,
					counter: 3,
				},
				{
					x: 2,
					counter: 1,
				},
				{
					x: 2,
					counter: 2,
				},
				{
					x: 2,
					counter: 3,
				},
			]);
		});
	});

	describe("group_by_streamo_base", () => {
		it("should group by key and aggregate values (preserve mode)", async () => {
			// Create test data: different categories with values
			const testData = [
				["fruit", "apple"],
				["fruit", "banana"],
				["veggie", "carrot"],
				["fruit", "orange"],
			];

			const groupCountGoal = (
				categoryTerm: Term,
				valueTerm: Term,
				countTerm: Term,
			) =>
				group_by_streamo_base(
					categoryTerm, // keyVar
					valueTerm, // valueVar
					countTerm, // outVar
					false, // drop=false (preserve mode)
					(values, _substitutions) => values.length, // aggregator: count values per group
				);
			const results = await query()
				.select(($) => ({
					category: $.category,
					count: $.count,
				}))
				.where(($) => [
					membero([$.category, $.value], testData),
					groupCountGoal($.category, $.value, $.count),
				])
				.toArray();

			// Should preserve all original substitutions with count added
			expect(results).toEqual([
				{
					category: "fruit",
					count: 3,
				}, // apple with fruit count
				{
					category: "fruit",
					count: 3,
				}, // banana with fruit count
				{
					category: "fruit",
					count: 3,
				}, // orange with fruit count
				{
					category: "veggie",
					count: 1,
				}, // carrot with veggie count
			]);
		});

		it("should group by key and aggregate values (drop mode)", async () => {
			const testData = [
				["fruit", "apple"],
				["fruit", "banana"],
				["veggie", "carrot"],
			];

			const groupCountGoal = (
				categoryTerm: Term,
				valueTerm: Term,
				countTerm: Term,
			) =>
				group_by_streamo_base(
					categoryTerm, // keyVar
					valueTerm, // valueVar
					countTerm, // outVar
					true, // drop=true (drop mode)
					(values, _substitutions) => values.length, // aggregator: count values per group
				);
			const results = await query()
				.select(($) => ({
					category: $.category,
					count: $.count,
				}))
				.where(($) => [
					membero([$.category, $.value], testData),
					groupCountGoal($.category, $.value, $.count),
				])
				.toArray();

			// Should emit one fresh substitution per group (no value var)
			expect(results).toEqual([
				{
					category: "fruit",
					count: 2,
				},
				{
					category: "veggie",
					count: 1,
				},
			]);
		});

		it("should handle null valueVar (count-only operations)", async () => {
			const testData = ["a", "b", "a", "c", "a"];

			const groupCountGoal = (keyTerm: Term, countTerm: Term) =>
				group_by_streamo_base(
					keyTerm, // keyVar
					null, // valueVar (null for count-only)
					countTerm, // outVar
					true, // drop mode for cleaner output
					(_values, substitutions) => substitutions.length, // aggregator: count substitutions
				);
			const results = await query()
				.select(($) => ({
					key: $.key,
					count: $.count,
				}))
				.where(($) => [
					membero($.key, testData),
					groupCountGoal($.key, $.count),
				])
				.toArray();

			expect(results).toEqual([
				{
					key: "a",
					count: 3,
				},
				{
					key: "b",
					count: 1,
				},
				{
					key: "c",
					count: 1,
				},
			]);
		});

		it("should collect values into lists per group", async () => {
			const testData = [
				["fruit", "apple"],
				["fruit", "banana"],
				["veggie", "carrot"],
				["fruit", "orange"],
			];

			const groupCollectGoal = (
				categoryTerm: Term,
				itemTerm: Term,
				itemsTerm: Term,
			) =>
				group_by_streamo_base(
					categoryTerm, // keyVar
					itemTerm, // valueVar
					itemsTerm, // outVar
					true, // drop mode
					(values, _substitutions) => arrayToLogicList(values), // aggregator: collect into logic list
				);
			const results = await query()
				.select(($) => ({
					category: $.category,
					items: $.items,
				}))
				.where(($) => [
					membero([$.category, $.item], testData),
					groupCollectGoal($.category, $.item, $.items),
				])
				.toArray();

			expect(results).toEqual([
				{
					category: "fruit",
					items: ["apple", "banana", "orange"],
				},
				{
					category: "veggie",
					items: ["carrot"],
				},
			]);
		});

		it("should handle custom aggregation functions", async () => {
			const testData = [
				["a", 10],
				["b", 5],
				["a", 20],
				["b", 15],
			];

			const groupSumGoal = (keyTerm: Term, valueTerm: Term, sumTerm: Term) =>
				group_by_streamo_base(
					keyTerm, // keyVar
					valueTerm, // valueVar
					sumTerm, // outVar
					true, // drop mode
					(values, _substitutions) => values.reduce((acc, val) => acc + val, 0), // aggregator: sum
				);
			const results = await query()
				.select(($) => ({
					key: $.key,
					sum: $.sum,
				}))
				.where(($) => [
					membero([$.key, $.value], testData),
					groupSumGoal($.key, $.value, $.sum),
				])
				.toArray();

			expect(results).toEqual([
				{
					key: "a",
					sum: 30,
				}, // 10 + 20
				{
					key: "b",
					sum: 20,
				}, // 5 + 15
			]);
		});

		it("should handle empty groups gracefully", async () => {
			// Goal that produces no results
			const groupGoal = (keyTerm: Term, countTerm: Term) =>
				group_by_streamo_base(
					keyTerm,
					null,
					countTerm,
					true,
					(_values, substitutions) => substitutions.length,
				);
			const results = await query()
				.select(($) => ({
					key: $.key,
					count: $.count,
				}))
				.where(($) => [
					eq($.key, "nonexistent"),
					eq($.key, "different"), // Impossible constraint
					groupGoal($.key, $.count),
				])
				.toArray();

			expect(results).toEqual([]); // No groups to process
		});

		it("should preserve variable context in preserve mode", async () => {
			const testData = [
				["fruit", "apple", "red"],
				["fruit", "banana", "yellow"],
			];

			const groupGoal = (categoryTerm: Term, itemTerm: Term, countTerm: Term) =>
				group_by_streamo_base(
					categoryTerm,
					itemTerm,
					countTerm,
					false, // preserve mode - should keep extra variable
					(values, _substitutions) => values.length,
				);
			const results = await query()
				.select(($) => ({
					category: $.category,
					item: $.item,
					extra: $.extra,
					count: $.count,
				}))
				.where(($) => [
					membero([$.category, $.item, $.extra], testData),
					groupGoal($.category, $.item, $.count),
				])
				.toArray();

			// Should preserve all original variables
			expect(results).toEqual([
				{
					category: "fruit",
					item: "apple",
					extra: "red",
					count: 2,
				},
				{
					category: "fruit",
					item: "banana",
					extra: "yellow",
					count: 2,
				},
			]);
		});
	});
});
