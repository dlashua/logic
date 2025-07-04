import { describe, it, expect, beforeEach } from 'vitest';
import type { Term, Subst } from './core/types.ts';
import {
  lvar,
  resetVarCounter,
  walk,
  unify,
  isCons,
  isNil,
  isLogicList,
  isVar,
  arrayToLogicList,
  logicList,
  nil,
  cons,
  logicListToArray
} from './core/kernel.ts'
import { eq, fresh, and, or } from './core/combinators.ts';
import { query } from './query.ts';
import { membero, firsto, resto, appendo } from './relations/lists.ts';
import { gto } from './relations/numeric.ts';
import { not, neqo } from './relations/control.ts';

describe('Core Logic Engine', () => {
  beforeEach(() => {
    resetVarCounter();
  });

  describe('Variables and Types', () => {
    it('should create unique logic variables', () => {
      const x = lvar('x');
      const y = lvar('y');
      
      expect(isVar(x)).toBe(true);
      expect(isVar(y)).toBe(true);
      expect(x.id).not.toBe(y.id);
      expect(x.id).toBe('x_0');
      expect(y.id).toBe('y_1');
    });

    it('should reset variable counter', () => {
      lvar('a');
      lvar('b');
      resetVarCounter();
      const x = lvar('x');
      expect(x.id).toBe('x_0');
    });

    it('should create cons cells and nil', () => {
      const cell = cons(1, nil);
      expect(isCons(cell)).toBe(true);
      expect(isNil(nil)).toBe(true);
      expect(cell.head).toBe(1);
      expect(cell.tail).toBe(nil);
    });

    it('should convert arrays to logic lists', () => {
      const arr = [1, 2, 3];
      const list = arrayToLogicList(arr);
      expect(isCons(list)).toBe(true);
      if (isCons(list)) {
        expect(list.head).toBe(1);
        expect(isCons(list.tail)).toBe(true);
      }
    });

    it('should create logic lists from arguments', () => {
      const list = logicList(1, 2, 3);
      const converted = logicListToArray(list);
      expect(converted).toEqual([1, 2, 3]);
    });

    it('should identify logic list types correctly', () => {
      const list = logicList(1, 2, 3);
      const array = [1, 2, 3];
      const variable = lvar('x');
      
      expect(isLogicList(list)).toBe(true);
      expect(isLogicList(array)).toBe(false);
      expect(isLogicList(variable)).toBe(false);
      expect(isLogicList(nil)).toBe(true);
    });
  });

  describe('Walking and Unification', () => {
    it('should walk variables in substitutions', async () => {
      const x = lvar('x');
      const y = lvar('y');
      const s = new Map();
      s.set(x.id, 42);
      s.set(y.id, x);
      
      const result = await walk(y, s);
      expect(result).toBe(42);
    });

    it('should walk cons cells', async () => {
      const x = lvar('x');
      const list = cons(x, nil);
      const s = new Map();
      s.set(x.id, 42);
      
      const result = await walk(list, s);
      expect(isCons(result)).toBe(true);
      expect((result as any).head).toBe(42);
    });

    it('should unify identical terms', async () => {
      const s = new Map();
      const result = await unify(42, 42, s);
      expect(result).toBe(s);
    });

    it('should unify variable with value', async () => {
      const x = lvar('x');
      const s = new Map();
      const result = await unify(x, 42, s);
      
      expect(result).not.toBeNull();
      expect(result!.get(x.id)).toBe(42);
    });

    it('should fail to unify different values', async () => {
      const s = new Map();
      const result = await unify(42, 43, s);
      expect(result).toBeNull();
    });

    it('should unify arrays of same length', async () => {
      const x = lvar('x');
      const s = new Map();
      const result = await unify([x, 2], [1, 2], s);
      
      expect(result).not.toBeNull();
      expect(result!.get(x.id)).toBe(1);
    });

    it('should fail to unify arrays of different lengths', async () => {
      const s = new Map();
      const result = await unify([1, 2], [1, 2, 3], s);
      expect(result).toBeNull();
    });
  });

  describe('Goals and Relations', () => {
    it('should succeed with eq goal when terms unify', async () => {
      const x = lvar('x');
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => eq(x, 42))
        .toArray();
      expect(results.length).toBe(1);
      expect(results[0].x).toBe(42);
    });

    it('should fail with eq goal when terms dont unify', async () => {
      const results = await query()
        .select($ => ({
          x: lvar('x') 
        }))
        .where($ => eq(42, 43))
        .toArray();
      expect(results.length).toBe(0);
    });

    it('should work with fresh variables', async () => {
      const results = await query()
        .select($ => ({
          x: $.x,
          y: $.y 
        }))
        .where($ => fresh((x, y) => and(eq(x, 1), eq(y, 2))))
        .toArray();
      expect(results.length).toBe(1);
    });

    it('should work with and (conjunction)', async () => {
      const x = lvar('x');
      const y = lvar('y');
      const results = await query()
        .select($ => ({
          x,
          y 
        }))
        .where($ => and(eq(x, 1), eq(y, 2)))
        .toArray();
      expect(results.length).toBe(1);
      expect(results[0].x).toBe(1);
      expect(results[0].y).toBe(2);
    });

    it('should work with or (disjunction)', async () => {
      const x = lvar('x');
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => or(eq(x, 1), eq(x, 2)))
        .toArray();
      expect(results.length).toBe(2);
      expect(results[0].x).toBe(1);
      expect(results[1].x).toBe(2);
    });

    it('should work with gto (greater than constraint)', async () => {
      const results = await query()
        .select($ => ({}))
        .where($ => gto(5, 3))
        .toArray();
      expect(results.length).toBe(1);
    });

    it('should fail with gto when constraint not met', async () => {
      const results = await query()
        .select($ => ({}))
        .where($ => gto(3, 5))
        .toArray();
      expect(results.length).toBe(0);
    });

    it('should work with not goal', async () => {
      const x = lvar('x');
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => and(eq(x, 42), not(eq(x, 43))))
        .toArray();
      expect(results.length).toBe(1);
      expect(results[0].x).toBe(42);
    });

    it('should work with neqo (not equal constraint)', async () => {
      const x = lvar('x');
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => and(eq(x, 42), neqo(x, 43)))
        .toArray();
      expect(results.length).toBe(1);
      expect(results[0].x).toBe(42);
    });
  });

  describe('List Relations', () => {
    it('should work with membero on logic lists', async () => {
      const x = lvar('x');
      const list = logicList(1, 2, 3);
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => membero(x, list))
        .toArray();
      expect(results.map(r => r.x)).toEqual([1, 2, 3]);
    });

    it('should work with membero on arrays', async () => {
      const x = lvar('x');
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => membero(x, [1, 2, 3]))
        .toArray();
      expect(results.map(r => r.x)).toEqual([1, 2, 3]);
    });

    it('should work with firsto', async () => {
      const x = lvar('x');
      const list = logicList(1, 2, 3);
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => firsto(x, list))
        .toArray();
      expect(results.map(r => r.x)).toEqual([1]);
    });

    it('should work with resto', async () => {
      const x = lvar('x');
      const list = logicList(1, 2, 3);
      const results = await query()
        .select($ => ({
          x 
        }))
        .where($ => resto(list, x))
        .toArray();
      expect(results.map(r => r.x)).toEqual([[2, 3]]);
    });

    it('should work with appendo', async () => {
      const z = lvar('z');
      const list1 = logicList(1, 2);
      const list2 = logicList(3, 4);
      const results = await query()
        .select($ => ({
          z 
        }))
        .where($ => appendo(list1, list2, z))
        .toArray();
      expect(results.map(r => r.z)).toEqual([[1, 2, 3, 4]]);
    });
  });
});
