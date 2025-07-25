import { describe, it, beforeEach } from 'vitest';
import {
  lvar,
  resetVarCounter,
  walk,
  unify,
  logicList,
  arrayToLogicList
} from './core/kernel.js'
import { eq, and, or } from './core/combinators.js'
import { query } from './core/query.js';
import { membero } from './relations/lists.js';
import { benchmark, formatBenchmarkResult, compareBenchmarks } from './benchmark.js';

describe('Performance Tests', () => {
  beforeEach(() => {
    resetVarCounter();
  });

  it('should benchmark basic operations', async () => {
    console.log('\n=== Core Performance Benchmarks ===');

    // Test variable creation
    const varResult = await benchmark('Variable creation', () => {
      return lvar('test');
    }, 10000);
    console.log(formatBenchmarkResult(varResult));

    // Test simple unification
    const unifyResult = await benchmark('Simple unification', async () => {
      const x = lvar('x');
      const s = new Map();
      return await unify(x, 42, s);
    }, 5000);
    console.log('\n' + formatBenchmarkResult(unifyResult));

    // Test walking
    const walkResult = await benchmark('Walking variables', async () => {
      const x = lvar('x');
      const y = lvar('y');
      const s = new Map();
      s.set(x.id, y);
      s.set(y.id, 42);
      return await walk(x, s);
    }, 5000);
    console.log('\n' + formatBenchmarkResult(walkResult));

    // Test deep substitution chains
    const deepWalkResult = await benchmark('Deep substitution walking', async () => {
      const vars = Array.from({
        length: 10 
      }, (_, i) => lvar(`x${i}`));
      const s = new Map();
      
      // Create a chain: x0 -> x1 -> x2 -> ... -> x9 -> 42
      for (let i = 0; i < vars.length - 1; i++) {
        s.set(vars[i].id, vars[i + 1]);
      }
      s.set(vars[vars.length - 1].id, 42);
      
      return await walk(vars[0], s);
    }, 1000);
    console.log('\n' + formatBenchmarkResult(deepWalkResult));

    // Test goal execution
    const goalResult = await benchmark('Goal execution (eq)', async () => {
      const x = lvar('x');
      return await query()
        .select($ => ({
          x 
        }))
        .where($ => eq(x, 42))
        .toArray();
    }, 2000);
    console.log('\n' + formatBenchmarkResult(goalResult));

    // Test complex goal (conjunction)
    const complexGoalResult = await benchmark('Complex goal (and)', async () => {
      const x = lvar('x');
      const y = lvar('y');
      const z = lvar('z');
      return await query()
        .select($ => ({
          x,
          y,
          z 
        }))
        .where($ => and(
          eq(x, 1),
          eq(y, 2),
          eq(z, 3)
        ))
        .toArray();
    }, 1000);
    console.log('\n' + formatBenchmarkResult(complexGoalResult));

    // Test list operations
    const listResult = await benchmark('Logic list operations', async () => {
      const x = lvar('x');
      const list = logicList(1, 2, 3, 4, 5);
      return await query()
        .select($ => ({
          x 
        }))
        .where($ => membero(x, list))
        .toArray();
    }, 1000);
    console.log('\n' + formatBenchmarkResult(listResult));

    // Test query builder
    const queryResult = await benchmark('Query builder', async () => {
      return await query()
        .select($ => ({
          x: $.x,
          y: $.y 
        }))
        .where($ => [
          or(eq($.x, 1), eq($.x, 2), eq($.x, 3)),
          eq($.y, 42)
        ])
        .toArray();
    }, 500);
    console.log('\n' + formatBenchmarkResult(queryResult));

  }, 60000); // 60 second timeout for benchmarks
});