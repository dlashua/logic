import { describe, it, expect, beforeEach } from 'vitest';
import { lvar, resetVarCounter, logicList, logicListToArray } from '../core/kernel.ts';
import { eq, and, or } from '../core/combinators.ts';
import { query } from '../query.ts';
import { membero } from './lists.ts';
import { collecto } from './aggregates.ts';
import { not } from './control.ts';

describe('Core Relations', () => {
  beforeEach(() => {
    resetVarCounter();
  });

  describe('membero', () => {
    it('should find element in array', async () => {
      const x = lvar('x');
      const goal = membero(x, [1, 2, 3]);
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => goal)
        .toArray();
      expect(results.map(r => r.x).sort()).toEqual([1, 2, 3]);
    });

    it('should succeed when element is in array', async () => {
      const goal = membero(2, [1, 2, 3]);
      const results = await query()
        .select($ => ({}))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(1);
    });

    it('should fail when element is not in array', async () => {
      const goal = membero(4, [1, 2, 3]);
      const results = await query()
        .select($ => ({}))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(0);
    });

    it('should work with logic lists', async () => {
      const x = lvar('x');
      const list = logicList(1, 2, 3);
      const goal = membero(x, list);
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => goal)
        .toArray();
      expect(results.map(r => r.x).sort()).toEqual([1, 2, 3]);
    });

    it('should generate array elements when array is variable', async () => {
      const list = lvar('list');
      const goal = and(
        eq(list, [1, 2, 3]),
        membero(2, list)
      );
      const results = await query()
        .select($ => ({
          list 
        }))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(1);
      expect(results[0].list).toEqual([1, 2, 3]);
    });
  });

  describe('collecto', () => {
    it('should collect all solutions', async () => {
      const x = lvar('x');
      const result = lvar('result');
      const goal = collecto(
        x,
        membero(x, [1, 2, 3]),
        result
      );
      const results = await query()
        .select($ => ({
          result 
        }))
        .where($ => goal)
        .toArray();
      const arr = results[0].result;
      const collected = Array.isArray(arr) ? arr.sort() : logicListToArray(arr).sort();
      expect(results).toHaveLength(1);
      expect(collected).toEqual([1, 2, 3]);
    });

    it('should collect filtered solutions', async () => {
      const x = lvar('x');
      const result = lvar('result');
      const goal = collecto(
        x,
        and(
          membero(x, [1, 2, 3, 4, 5]),
          or(eq(x, 2), eq(x, 4)) // Only collect even numbers 2 and 4
        ),
        result
      );
      const results = await query()
        .select($ => ({
          result 
        }))
        .where($ => goal)
        .toArray();
      const arr = results[0].result;
      const collected = Array.isArray(arr) ? arr.sort() : logicListToArray(arr).sort();
      expect(results).toHaveLength(1);
      expect(collected).toEqual([2, 4]);
    });

    it('should handle empty collections', async () => {
      const x = lvar('x');
      const result = lvar('result');
      const goal = collecto(
        x,
        and(
          membero(x, [1, 2, 3]),
          eq(x, 99) // This will never succeed
        ),
        result
      );
      const results = await query()
        .select($ => ({
          result 
        }))
        .where($ => goal)
        .toArray();
      const arr = results[0].result;
      const collected = Array.isArray(arr) ? arr : logicListToArray(arr);
      expect(results).toHaveLength(1);
      expect(collected).toEqual([]);
    });
  });

  describe('not', () => {
    it('should succeed when goal fails', async () => {
      const goal = not(eq(1, 2));
      const results = await query()
        .select($ => ({}))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({});
    });

    it('should fail when goal succeeds', async () => {
      const goal = not(eq(1, 1));
      const results = await query()
        .select($ => ({}))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(0);
    });

    it('should work with variable bindings', async () => {
      const x = lvar('x');
      const goal = and(
        eq(x, 42),
        not(eq(x, 43))
      );
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(1);
      expect(results[0].x).toBe(42);
    });

    it('should not bind variables in negated goals', async () => {
      const x = lvar('x');
      const y = lvar('y');
      const goal = and(
        eq(x, 42),
        not(eq(y, 'hello')) // y should not be bound by this
      );
      const results = await query()
        .select($ => ({
          x,
          y 
        }))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(1);
      expect(results[0].x).toBe(42);
      expect(results[0].y).toStrictEqual({
        tag: "var",
        id: "y_1" 
      });
    });
  });

  describe('complex combinations', () => {
    it('should handle membero with collecto and not', async () => {
      const x = lvar('x');
      const evens = lvar('evens');
      const goal = collecto(
        x,
        and(
          membero(x, [1, 2, 3, 4, 5, 6]),
          not(and(
            membero(x, [1, 3, 5]) // Not odd numbers
          ))
        ),
        evens
      );
      const results = await query()
        .select($ => ({
          evens 
        }))
        .where($ => goal)
        .toArray();
      const arr = results[0].evens;
      const collected = Array.isArray(arr) ? arr.sort() : logicListToArray(arr).sort();
      expect(results).toHaveLength(1);
      expect(collected).toEqual([2, 4, 6]);
    });
  });
});