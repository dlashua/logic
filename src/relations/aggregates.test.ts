import { describe, it, expect, beforeEach } from 'vitest';
import { lvar, resetVarCounter, walk, logicListToArray } from '../core/kernel.ts';
import { eq, or, and } from '../core/combinators.ts';
import { query } from '../query.ts';
import { } from '../relations/lists.ts';
import {
  group_by_collecto,
  group_by_counto,
  collect_distincto,
  counto,
  collecto
} from "./aggregates-subqueries.ts"

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
      const results = await query()
        .select($ => ({
          result 
        }))
        .where($ => goal)
        .toArray();
      expect(results.map(r => r.result)).toEqual([[1, 2, 3]]);
    });

    it('should work with empty results', async () => {
      const x = lvar('x');
      const result = lvar('result');
      const goal = collecto(
        x,
        and(eq(x, 1), eq(x, 2)), // impossible constraint
        result
      );
      const results = await query()
        .select($ => ({
          result 
        }))
        .where($ => goal)
        .toArray();
      expect(results.map(r => logicListToArray(r.result))).toEqual([[]]);
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
      const results = await query()
        .select($ => ({
          result 
        }))
        .where($ => goal)
        .toArray();
      const collected = results[0].result;
      // @ts-expect-error
      collected.sort();
      expect([collected]).toEqual([[1, 2, 3]]);
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
      const results = await query()
        .select($ => ({
          count 
        }))
        .where($ => goal)
        .toArray();
      expect(results.map(r => r.count)).toEqual([3]);
    });

    it('should count zero for no solutions', async () => {
      const x = lvar('x');
      const count = lvar('count');
      const goal = counto(
        x,
        and(eq(x, 1), eq(x, 2)), // impossible
        count
      );
      const results = await query()
        .select($ => ({
          count 
        }))
        .where($ => goal)
        .toArray();
      expect(results.map(r => r.count)).toEqual([0]);
    });
  });

  describe('group_by_collecto', () => {
    it('should group by key and collect values', async () => {
      const key = lvar('key');
      const value = lvar('value');
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
        outValues
      );
      const results = await query()
        .select($ => ({
          outKey: key,
          outValues 
        }))
        .where($ => goal)
        .toArray();
      const mapped = results.map(r => ({
        key: r.outKey,
        values: r.outValues,
      }));
      mapped.sort((a, b) => (a.key as string).localeCompare(b.key as string));
      expect(mapped).toEqual([
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
      const results = await query()
        .select($ => ({
          outKey: $.key,
          outCount: $.outCount,
        }))
        .where($ => [
          group_by_counto(
            $.key,
            $.value,
            or(
              and(eq($.key, 'a'), eq($.value, 1)),
              and(eq($.key, 'a'), eq($.value, 2)),
              and(eq($.key, 'b'), eq($.value, 3))
            ),
            $.outCount
          )
        ])
        .toArray();
      console.log(results);
      // results.sort((a, b) => (a.outKey as string).localeCompare(b.outKey as string));
      expect(results).toEqual([
        {
          outCount: 2, 
          outKey: 'a',
        },
        {
          outCount: 1, 
          outKey: 'b',
        }
      ]);
    });
  });
});
