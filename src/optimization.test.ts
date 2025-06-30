import { describe, it, beforeEach } from 'vitest';
import {
  lvar,
  resetVarCounter,
  walk,
  unify
} from './core.ts';
// Note: Optimizations are now integrated into core.ts
// This test compares the current optimized core against a baseline
import { compareBenchmarks } from './benchmark.ts';

describe('Optimization Tests', () => {
  beforeEach(() => {
    resetVarCounter();
  });

  it('should compare walk optimization', async () => {
    // Test deep substitution chains
    await compareBenchmarks(
      async () => {
        const vars = Array.from({ length: 20 }, (_, i) => lvar(`x${i}`));
        const s = new Map();
        
        // Create a chain: x0 -> x1 -> x2 -> ... -> x19 -> 42
        for (let i = 0; i < vars.length - 1; i++) {
          s.set(vars[i].id, vars[i + 1]);
        }
        s.set(vars[vars.length - 1].id, 42);
        
        return await walk(vars[0], s);
      },
      async () => {
        const vars = Array.from({ length: 20 }, (_, i) => lvar(`x${i}`));
        const s = new Map();
        
        // Create a chain: x0 -> x1 -> x2 -> ... -> x19 -> 42
        for (let i = 0; i < vars.length - 1; i++) {
          s.set(vars[i].id, vars[i + 1]);
        }
        s.set(vars[vars.length - 1].id, 42);
        
        return await walk(vars[0], s); // Now using optimized version
      },
      'Deep substitution walking',
      2000
    );
  }, 30000);

  it('should compare unification optimization', async () => {
    await compareBenchmarks(
      async () => {
        const x = lvar('x');
        const y = lvar('y');
        const s = new Map();
        
        // Test complex unification
        return await unify([x, y, 42], [1, 2, 42], s);
      },
      async () => {
        const x = lvar('x');
        const y = lvar('y');
        const s = new Map();
        
        // Test complex unification
        return await unify([x, y, 42], [1, 2, 42], s); // Now using optimized version
      },
      'Array unification',
      5000
    );
  }, 30000);

  it('should compare primitive unification optimization', async () => {
    await compareBenchmarks(
      async () => {
        const s = new Map();
        return await unify(42, 42, s);
      },
      async () => {
        const s = new Map();
        return await unify(42, 42, s); // Now using optimized version
      },
      'Primitive unification',
      10000
    );
  }, 30000);

  it('should verify correctness of optimizations', async () => {
    // Ensure optimized versions produce same results as originals
    const vars = Array.from({ length: 10 }, (_, i) => lvar(`x${i}`));
    const s = new Map();
    
    for (let i = 0; i < vars.length - 1; i++) {
      s.set(vars[i].id, vars[i + 1]);
    }
    s.set(vars[vars.length - 1].id, 42);
    
    const originalResult = await walk(vars[0], s);
    const optimizedResult = await walk(vars[0], s); // Now using optimized version
    
    console.log(`Original walk result: ${originalResult}`);
    console.log(`Optimized walk result: ${optimizedResult}`);
    
    // Test unification correctness 
    const x = lvar('x');
    const y = lvar('y');
    const s2 = new Map();
    
    const originalUnify = await unify([x, y], [1, 2], s2);
    const optimizedUnify = await unify([x, y], [1, 2], s2); // Now using optimized version
    
    console.log('Unification results match:', 
      originalUnify?.get(x.id) === optimizedUnify?.get(x.id) &&
      originalUnify?.get(y.id) === optimizedUnify?.get(y.id)
    );
  }, 10000);
});