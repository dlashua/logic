import { Goal, Term, Subst } from "../core/types.ts";
import { isVar, walk, isCons, isNil, isLogicList } from "../core/kernel.ts";

type GoalFunction = (...args: any[]) => Goal;

// The cacheKeyFn now takes the original arguments AND the current substitution
type CacheKeyFunction<T extends GoalFunction> = (this: ThisParameterType<T>, args: Parameters<T>, s: Subst) => Promise<string>;

export function memoize<T extends GoalFunction>(
  fn: T,
  options?: { cacheKeyFn?: CacheKeyFunction<T>; ttl?: number; cleanupInterval?: number }
): T {
  const cache = new Map<string, { value: Subst[]; expiry: number }>(); // Stores resolved arrays of substitutions with expiry
  const effectiveCacheKeyFn = options?.cacheKeyFn || defaultCacheKeyFn;
  const ttl = options?.ttl ?? 5000; // Default to 5000ms (5 seconds)
  const cleanupInterval = options?.cleanupInterval ?? 60000; // Default to 60000ms (1 minute)

  const cleanup = () => {
    const now = Date.now();
    for (const [key, entry] of cache.entries()) {
      if (entry.expiry <= now) {
        cache.delete(key);
      }
    }
  };

  // Start periodic cleanup
  const intervalId = setInterval(cleanup, cleanupInterval);

  const memoizedFn = ((...args: Parameters<T>): Goal => {
    return async function* (s: Subst): Goal {
      // Generate the key using the original arguments and the current substitution
      const key = await effectiveCacheKeyFn.call(this, args, s); // Pass 'this' context

      if (cache.has(key)) {
        const cachedEntry = cache.get(key)!;
        if (cachedEntry.expiry > Date.now()) {
          // Cache hit and not expired
          for (const subst of cachedEntry.value) {
            yield subst;
          }
          return;
        } else {
          // Cache expired, remove it
          cache.delete(key);
        }
      }

      const results: Subst[] = [];
      for await (const subst of fn.apply(this, args)(s)) {
        results.push(subst);
        yield subst;
      }
      // Store new results with expiry
      cache.set(key, { value: results, expiry: Date.now() + ttl });
    };
  }) as T;

  // Add a way to clear the cache if needed
  (memoizedFn as any).clearCache = () => {
    cache.clear();
  };

  // Add a way to manually trigger cleanup
  (memoizedFn as any).clearExpired = cleanup;

  // Optionally, add a way to stop the periodic cleanup if the memoized function is no longer used
  // This would require a more complex lifecycle management for the memoized function.
  // (memoizedFn as any).stopCleanup = () => {
  //   clearInterval(intervalId);
  // };

  return memoizedFn;
}

// Default cache key function: now takes original arguments and Subst as arguments
async function defaultCacheKeyFn(this: any, args: any[], s: Subst): Promise<string> {
  if (args.length === 0) {
    return "no_args";
  }

  const keyParts: string[] = [];
  for (const arg of args) {
    if (typeof arg === 'string' || typeof arg === 'number' || typeof arg === 'boolean') {
      keyParts.push(String(arg));
    } else if (arg && typeof arg === 'object') {
      if (isVar(arg)) {
        // Walk the variable with the current substitution to get its concrete value
        const walkedValue = await walk(arg as Term, s);
        keyParts.push(`WALKED_VAR:${JSON.stringify(walkedValue)}`);
      } else if (isCons(arg) || isNil(arg) || isLogicList(arg)) {
        // For logic lists, stringify their structure
        keyParts.push(`LOGIC_LIST:${JSON.stringify(arg)}`);
      } else if (arg instanceof Map) {
        // For Maps (like Subst), convert to a sorted array of key-value pairs
        keyParts.push(`MAP:${JSON.stringify(Array.from(arg.entries()).sort())}`);
      } else {
        // For other objects, try to stringify them
        keyParts.push(`OBJ:${JSON.stringify(arg)}`);
      }
    } else {
      // For null, undefined, or other unhandled types
      keyParts.push(String(arg));
    }
  }
  return keyParts.join("_");
}