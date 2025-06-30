import { describe, it, expect, beforeEach } from 'vitest';
import {
  lvar,
  resetVarCounter,
  logicList,
  logicListToArray,
  walk,
  eq,
  and
} from './core.ts';
import {
  arrayLength,
  permuteo,
  mapo,
  removeFirsto,
  alldistincto
} from './relations-list.ts';

describe('List Relations', () => {
  beforeEach(() => {
    resetVarCounter();
  });

  describe('arrayLength', () => {
    it('should unify array length with number', async () => {
      const len = lvar('len');
      const goal = arrayLength([1, 2, 3], len);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(await walk(len, subst));
      }
      
      expect(results).toEqual([3]);
    });

    it('should work with logic lists', async () => {
      const len = lvar('len');
      const list = logicList(1, 2, 3, 4);
      const goal = arrayLength(list, len);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(await walk(len, subst));
      }
      
      expect(results).toEqual([4]);
    });

    it('should work with empty arrays', async () => {
      const len = lvar('len');
      const goal = arrayLength([], len);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(await walk(len, subst));
      }
      
      expect(results).toEqual([0]);
    });

    it('should unify known length with array', async () => {
      const arr = lvar('arr');
      const goal = and(
        eq(arr, [1, 2, 3]),
        arrayLength(arr, 3)
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(await walk(arr, subst));
      }
      
      expect(results).toEqual([[1, 2, 3]]);
    });

    it('should fail for wrong length', async () => {
      const goal = arrayLength([1, 2, 3], 5);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toEqual([]);
    });
  });

  describe('removeFirsto', () => {
    it('should remove first occurrence of element', async () => {
      const result = lvar('result');
      const list = logicList(1, 2, 3, 2);
      const goal = removeFirsto(list, 2, result);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        const res = await walk(result, subst);
        results.push(logicListToArray(res));
      }
      
      expect(results).toEqual([[1, 3, 2]]);
    });

    it('should handle element not in list', async () => {
      const result = lvar('result');
      const list = logicList(1, 2, 3);
      const goal = removeFirsto(list, 4, result);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toEqual([]);
    });
  });

  describe('mapo', () => {
    it('should map a relation over lists', async () => {
      const result = lvar('result');
      const list1 = logicList(1, 2, 3);
      
      // Define a simple relation that adds 1
      const addOne = (x: any, y: any) => {
        return async function* (s: any) {
          const xVal = await walk(x, s);
          if (typeof xVal === 'number') {
            yield* eq(y, xVal + 1)(s);
          }
        };
      };
      
      const goal = mapo(addOne, list1, result);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        const res = await walk(result, subst);
        results.push(logicListToArray(res));
      }
      
      expect(results).toEqual([[2, 3, 4]]);
    });

    it('should work with empty lists', async () => {
      const result = lvar('result');
      const emptyList = logicList();
      
      const addOne = (x: any, y: any) => {
        return async function* (s: any) {
          const xVal = await walk(x, s);
          if (typeof xVal === 'number') {
            yield* eq(y, xVal + 1)(s);
          }
        };
      };
      
      const goal = mapo(addOne, emptyList, result);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        const res = await walk(result, subst);
        results.push(logicListToArray(res));
      }
      
      expect(results).toEqual([[]]);
    });
  });

  describe('alldistincto', () => {
    it('should succeed for arrays with distinct elements', async () => {
      const goal = alldistincto([1, 2, 3, 4]);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results.length).toBe(1);
    });

    it('should fail for arrays with duplicate elements', async () => {
      const goal = alldistincto([1, 2, 2, 4]);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results.length).toBe(0);
    });

    it('should work with logic lists', async () => {
      const list = logicList(1, 2, 3);
      const goal = alldistincto(list);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results.length).toBe(1);
    });

    it('should fail for logic lists with duplicates', async () => {
      const list = logicList(1, 2, 2);
      const goal = alldistincto(list);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results.length).toBe(0);
    });

    it('should succeed for empty arrays', async () => {
      const goal = alldistincto([]);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results.length).toBe(1);
    });
  });
});