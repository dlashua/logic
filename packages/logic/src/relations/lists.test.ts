import { SimpleObservable } from "@codespiral/observable";
import { beforeEach, describe, expect, it } from "vitest";
import { and, eq, run } from "../core/combinators.js";
import {
  logicList,
  logicListToArray,
  lvar,
  resetVarCounter,
  walk,
} from "../core/kernel.js";
import { query } from "../core/query.js";
import { alldistincto, lengtho, mapo, removeFirsto } from "./lists.js";

describe("List Relations", () => {
  beforeEach(() => {
    resetVarCounter();
  });

  describe("lengtho", () => {
    it("should unify array length with number", async () => {
      const results = await query()
        .select(($) => ({
          len: $.len,
        }))
        .where(($) => lengtho([1, 2, 3], $.len))
        .toArray();

      expect(results).toEqual([
        {
          len: 3,
        },
      ]);
    });

    it("should work with logic lists", async () => {
      const results = await query()
        .select(($) => ({
          len: $.len,
        }))
        .where(($) => lengtho(logicList(1, 2, 3, 4), $.len))
        .toArray();

      expect(results).toEqual([
        {
          len: 4,
        },
      ]);
    });

    it("should work with logic lists (direct substitution test)", async () => {
      const len = lvar("len");
      const list = logicList(1, 2, 3, 4);
      const goal = lengtho(list, len);
      const result = await run(goal);

      expect(result.results).toHaveLength(1);
      expect(await walk(len, result.results[0])).toBe(4);
    });

    it("should work with empty arrays", async () => {
      const results = await query()
        .select(($) => ({
          len: $.len,
        }))
        .where(($) => lengtho([], $.len))
        .toArray();

      expect(results).toEqual([
        {
          len: 0,
        },
      ]);
    });

    it("should unify known length with array", async () => {
      const results = await query()
        .select(($) => ({
          arr: $.arr,
        }))
        .where(($) => and(eq($.arr, [1, 2, 3]), lengtho($.arr, 3)))
        .toArray();

      expect(results).toEqual([
        {
          arr: [1, 2, 3],
        },
      ]);
    });

    it("should fail for wrong length", async () => {
      const goal = lengtho([1, 2, 3], 5);
      const result = await run(goal);

      expect(result.results).toEqual([]);
      expect(result.completed).toBe(true);
    });
  });

  describe("removeFirsto", () => {
    it("should remove first occurrence of element", async () => {
      const result = lvar("result");
      const goal = removeFirsto(logicList(1, 2, 3, 2), 2, result);
      const runResult = await run(goal);

      expect(runResult.results).toHaveLength(1);
      const res = await walk(result, runResult.results[0]);
      expect(logicListToArray(res)).toEqual([1, 3, 2]);
    });

    it("should remove first occurrence of element (direct substitution test)", async () => {
      const result = lvar("result");
      const list = logicList(1, 2, 3, 2);
      const goal = removeFirsto(list, 2, result);
      const runResult = await run(goal);

      expect(runResult.results).toHaveLength(1);
      const res = await walk(result, runResult.results[0]);
      expect(logicListToArray(res)).toEqual([1, 3, 2]);
    });

    it("should handle element not in list", async () => {
      const goal = removeFirsto(logicList(1, 2, 3), 4, lvar("result"));
      const result = await run(goal);

      expect(result.results).toEqual([]);
      expect(result.completed).toBe(true);
    });
  });

  describe("mapo", () => {
    it("should map a relation over lists", async () => {
      // Define a simple relation that adds 1
      const addOne = (x: any, y: any) => (input$: any) =>
        input$.flatMap((s: any) => {
          const xVal = walk(x, s);
          if (typeof xVal === "number") {
            return eq(y, xVal + 1)(SimpleObservable.of(s));
          }
          return SimpleObservable.empty();
        });

      const result = lvar("result");
      const goal = mapo(addOne, logicList(1, 2, 3), result);
      const runResult = await run(goal);

      // expect(runResult.results).toHaveLength(1);
      const res = await walk(result, runResult.results[0]);
      expect(logicListToArray(res)).toEqual([2, 3, 4]);
    });

    it("should work with empty lists", async () => {
      // Define a simple relation that adds 1
      const addOne = (x: any, y: any) => (input$: any) =>
        input$.flatMap((s: any) => {
          const xVal = walk(x, s);
          if (typeof xVal === "number") {
            return eq(y, xVal + 1)(SimpleObservable.of(s));
          }
          return SimpleObservable.empty();
        });

      const result = lvar("result");
      const goal = mapo(addOne, logicList(), result);
      const runResult = await run(goal);

      expect(runResult.results).toHaveLength(1);
      const res = await walk(result, runResult.results[0]);
      expect(logicListToArray(res)).toEqual([]);
    });
  });

  describe("alldistincto", () => {
    it("should succeed for arrays with distinct elements", async () => {
      const goal = alldistincto([1, 2, 3, 4]);
      const result = await run(goal);

      expect(result.results.length).toBe(1);
      expect(result.completed).toBe(true);
    });

    it("should fail for arrays with duplicate elements", async () => {
      const goal = alldistincto([1, 2, 2, 4]);
      const result = await run(goal);

      expect(result.results.length).toBe(0);
      expect(result.completed).toBe(true);
    });

    it("should work with logic lists", async () => {
      const list = logicList(1, 2, 3);
      const goal = alldistincto(list);
      const result = await run(goal);

      expect(result.results.length).toBe(1);
      expect(result.completed).toBe(true);
    });

    it("should fail for logic lists with duplicates", async () => {
      const list = logicList(1, 2, 2);
      const goal = alldistincto(list);
      const result = await run(goal);

      expect(result.results.length).toBe(0);
      expect(result.completed).toBe(true);
    });

    it("should succeed for empty arrays", async () => {
      const goal = alldistincto([]);
      const result = await run(goal);

      expect(result.results.length).toBe(1);
      expect(result.completed).toBe(true);
    });
  });
});
