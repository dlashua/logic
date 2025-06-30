/**
 * Benchmarking utilities for measuring performance improvements
 */

export interface BenchmarkResult {
  name: string;
  iterations: number;
  totalTime: number;
  avgTime: number;
  minTime: number;
  maxTime: number;
  opsPerSec: number;
}

export async function benchmark(
  name: string,
  fn: () => Promise<any> | any,
  iterations = 1000
): Promise<BenchmarkResult> {
  const times: number[] = [];
  
  // Warm up
  for (let i = 0; i < Math.min(10, iterations); i++) {
    await fn();
  }
  
  // Run benchmark
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    await fn();
    const end = performance.now();
    times.push(end - start);
  }
  
  const totalTime = times.reduce((sum, time) => sum + time, 0);
  const avgTime = totalTime / iterations;
  const minTime = Math.min(...times);
  const maxTime = Math.max(...times);
  const opsPerSec = 1000 / avgTime;
  
  return {
    name,
    iterations,
    totalTime,
    avgTime,
    minTime,
    maxTime,
    opsPerSec
  };
}

export function formatBenchmarkResult(result: BenchmarkResult): string {
  return [
    `Benchmark: ${result.name}`,
    `  Iterations: ${result.iterations}`,
    `  Total time: ${result.totalTime.toFixed(2)}ms`,
    `  Average: ${result.avgTime.toFixed(4)}ms`,
    `  Min: ${result.minTime.toFixed(4)}ms`,
    `  Max: ${result.maxTime.toFixed(4)}ms`,
    `  Ops/sec: ${result.opsPerSec.toFixed(0)}`
  ].join('\n');
}

export async function compareBenchmarks(
  baseline: () => Promise<any> | any,
  optimized: () => Promise<any> | any,
  name: string,
  iterations = 1000
): Promise<void> {
  console.log(`\n=== Comparing: ${name} ===`);
  
  const baselineResult = await benchmark(`${name} (baseline)`, baseline, iterations);
  const optimizedResult = await benchmark(`${name} (optimized)`, optimized, iterations);
  
  console.log(formatBenchmarkResult(baselineResult));
  console.log();
  console.log(formatBenchmarkResult(optimizedResult));
  
  const improvement = ((baselineResult.avgTime - optimizedResult.avgTime) / baselineResult.avgTime) * 100;
  const speedup = baselineResult.avgTime / optimizedResult.avgTime;
  
  console.log();
  if (improvement > 0) {
    console.log(`🚀 Improvement: ${improvement.toFixed(1)}% faster (${speedup.toFixed(2)}x speedup)`);
  } else {
    console.log(`📉 Regression: ${Math.abs(improvement).toFixed(1)}% slower (${(1/speedup).toFixed(2)}x slowdown)`);
  }
}