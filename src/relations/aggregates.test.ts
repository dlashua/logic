import { describe, it, expect, beforeEach } from 'vitest';
import { lvar, resetVarCounter, walk, logicListToArray } from '../core/kernel.ts';
import { eq, or, and } from '../core/combinators.ts';
import { } from '../relations/lists.ts';
import {
  collecto,
  collect_distincto,
  counto,
  group_by_collecto,
  group_by_counto
} from './aggregates.ts';

describe('Aggregation Relations', () => {
  beforeEach(() => {
    resetVarCounter();
  });

  describe('collecto', () => {
    it('should collect all values into a logic list', async () => {
      const x = lvar('x');
      const result = lvar('result');
      const goal = collecto(
        x,
        or(eq(x, 1), eq(x, 2), eq(x, 3)),
        result
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        const res = await walk(result, subst);
        results.push(logicListToArray(res));
      }
      
      expect(results).toEqual([[1, 2, 3]]);
    });

    it('should work with empty results', async () => {
      const x = lvar('x');
      const result = lvar('result');
      const goal = collecto(
        x,
        and(eq(x, 1), eq(x, 2)), // impossible constraint
        result
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        const res = await walk(result, subst);
        results.push(logicListToArray(res));
      }
      
      expect(results).toEqual([[]]);
    });
  });

  describe('collect_distincto', () => {
    it('should collect distinct values only', async () => {
      const x = lvar('x');
      const result = lvar('result');
      const goal = collect_distincto(
        x,
        or(eq(x, 1), eq(x, 2), eq(x, 1), eq(x, 3), eq(x, 2)),
        result
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        const res = await walk(result, subst);
        const collected = logicListToArray(res);
        // Sort for consistent comparison since collect_distincto doesn't guarantee order
        collected.sort();
        results.push(collected);
      }
      
      expect(results).toEqual([[1, 2, 3]]);
    });
  });

  describe('counto', () => {
    it('should count the number of solutions', async () => {
      const x = lvar('x');
      const count = lvar('count');
      const goal = counto(
        x,
        or(eq(x, 1), eq(x, 2), eq(x, 3)),
        count
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(await walk(count, subst));
      }
      
      expect(results).toEqual([3]);
    });

    it('should count zero for no solutions', async () => {
      const x = lvar('x');
      const count = lvar('count');
      const goal = counto(
        x,
        and(eq(x, 1), eq(x, 2)), // impossible
        count
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(await walk(count, subst));
      }
      
      expect(results).toEqual([0]);
    });
  });

  describe('group_by_collecto', () => {
    it('should group by key and collect values', async () => {
      const key = lvar('key');
      const value = lvar('value');
      const outKey = lvar('outKey');
      const outValues = lvar('outValues');
      
      const goal = group_by_collecto(
        key,
        value,
        or(
          and(eq(key, 'a'), eq(value, 1)),
          and(eq(key, 'a'), eq(value, 2)),
          and(eq(key, 'b'), eq(value, 3)),
          and(eq(key, 'b'), eq(value, 4))
        ),
        outKey,
        outValues
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        const k = await walk(outKey, subst);
        const v = await walk(outValues, subst);
        results.push({
          key: k,
          values: logicListToArray(v)
        });
      }
      
      // Sort results for consistent comparison
      results.sort((a, b) => (a.key as string).localeCompare(b.key as string));
      
      expect(results).toEqual([
        {
          key: 'a',
          values: [1, 2]
        },
        {
          key: 'b',
          values: [3, 4]
        }
      ]);
    });
  });

  describe('group_by_counto', () => {
    it('should group by key and count values', async () => {
      const key = lvar('key');
      const value = lvar('value');
      const outKey = lvar('outKey');
      const outCount = lvar('outCount');
      
      const goal = group_by_counto(
        key,
        value,
        or(
          and(eq(key, 'a'), eq(value, 1)),
          and(eq(key, 'a'), eq(value, 2)),
          and(eq(key, 'b'), eq(value, 3))
        ),
        outKey,
        outCount
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        const k = await walk(outKey, subst);
        const c = await walk(outCount, subst);
        results.push({
          key: k,
          count: c
        });
      }
      
      // Sort results for consistent comparison
      results.sort((a, b) => (a.key as string).localeCompare(b.key as string));
      
      expect(results).toEqual([
        {
          key: 'a',
          count: 2 
        },
        {
          key: 'b',
          count: 1 
        }
      ]);
    });
  });
});
