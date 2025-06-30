import { Goal, Term, Subst } from "../core/types.ts";
import {
  isVar,
  walk,
  isCons,
  isNil,
  isLogicList
} from "../core/kernel.ts"

type GoalFunction = (...args: any[]) => Goal;

// The cacheKeyFn now takes the original arguments AND the current substitution
type CacheKeyFunction = (args: Parameters<GoalFunction>, s: Subst) => Promise<string>;

export function memoize<T extends GoalFunction>(
  fn: T,
  cacheKeyFn: CacheKeyFunction = defaultCacheKeyFn // Use the new signature
): T {
  const cache = new Map<string, Subst[]>(); // Stores resolved arrays of substitutions

  const memoizedFn = ((...args: Parameters<T>): Goal => {
    return async function* (s: Subst) {
      // Generate the key using the original arguments and the current substitution
      const key = await cacheKeyFn(args, s); // Await the async cacheKeyFn

      if (cache.has(key)) {
        const cachedSubsts = cache.get(key)!;
        for (const subst of cachedSubsts) {
          yield subst;
        }
        return;
      }

      const results: Subst[] = [];
      for await (const subst of fn(...args)(s)) {
        results.push(subst);
        yield subst;
      }
      cache.set(key, results);
    };
  }) as T;

  // Add a way to clear the cache if needed
  (memoizedFn as any).clearCache = () => {
    cache.clear();
  };

  return memoizedFn;
}

// Default cache key function: now takes original arguments and Subst as arguments
async function defaultCacheKeyFn(args: any[], s: Subst): Promise<string> {
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