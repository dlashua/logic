import { describe, it, expect, beforeEach } from 'vitest';
import { lvar, resetVarCounter } from './kernel.ts';
import { eq, and, or } from './combinators.ts';

describe('Logic Combinators', () => {
  beforeEach(() => {
    resetVarCounter();
  });

  describe('eq (equality)', () => {
    it('should unify identical values', async () => {
      const goal = eq(42, 42);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(1);
      expect(results[0]).toEqual(new Map());
    });

    it('should unify variable with value', async () => {
      const x = lvar('x');
      const goal = eq(x, 42);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(1);
      expect(results[0].get(x.id)).toBe(42);
    });

    it('should fail to unify different values', async () => {
      const goal = eq(42, 43);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(0);
    });

    it('should unify two variables', async () => {
      const x = lvar('x');
      const y = lvar('y');
      const goal = eq(x, y);
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(1);
      // One variable should be bound to the other
      const subst = results[0];
      expect(subst.size).toBe(1);
      expect(subst.has(x.id) || subst.has(y.id)).toBe(true);
    });
  });

  describe('and (conjunction)', () => {
    it('should succeed when all goals succeed', async () => {
      const x = lvar('x');
      const y = lvar('y');
      const goal = and(
        eq(x, 42),
        eq(y, 'hello')
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(1);
      expect(results[0].get(x.id)).toBe(42);
      expect(results[0].get(y.id)).toBe('hello');
    });

    it('should fail when any goal fails', async () => {
      const x = lvar('x');
      const goal = and(
        eq(x, 42),
        eq(x, 43) // This should fail
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(0);
    });

    it('should handle variable propagation', async () => {
      const x = lvar('x');
      const y = lvar('y');
      const z = lvar('z');
      const goal = and(
        eq(x, y),
        eq(y, z),
        eq(z, 42)
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(1);
      // All variables should eventually resolve to 42
      const subst = results[0];
      expect(subst.get(z.id)).toBe(42);
    });
  });

  describe('or (disjunction)', () => {
    it('should succeed with multiple solutions', async () => {
      const x = lvar('x');
      const goal = or(
        eq(x, 42),
        eq(x, 43)
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(2);
      const values = results.map(subst => subst.get(x.id)).sort();
      expect(values).toEqual([42, 43]);
    });

    it('should succeed with at least one solution', async () => {
      const x = lvar('x');
      const goal = or(
        eq(x, 42),
        eq(42, 43) // This fails but first succeeds
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(1);
      expect(results[0].get(x.id)).toBe(42);
    });

    it('should fail when all goals fail', async () => {
      const goal = or(
        eq(42, 43),
        eq('hello', 'world')
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(0);
    });
  });

  describe('complex combinations', () => {
    it('should handle nested and/or correctly', async () => {
      const x = lvar('x');
      const y = lvar('y');
      const goal = and(
        or(
          eq(x, 1),
          eq(x, 2)
        ),
        or(
          eq(y, 'a'),
          eq(y, 'b')
        )
      );
      const s = new Map();
      
      const results = [];
      for await (const subst of goal(s)) {
        results.push(subst);
      }
      
      expect(results).toHaveLength(4); // Cartesian product: 2 * 2
      const pairs = results.map(subst => [subst.get(x.id), subst.get(y.id)]).sort();
      expect(pairs).toEqual([
        [1, 'a'], [1, 'b'], [2, 'a'], [2, 'b']
      ]);
    });
  });
});