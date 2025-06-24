// Extended (non-primitive) logic relations for MiniKanren-style logic programming
// These are not part of the minimal core, but are useful for practical logic programming.

import type { Subst, Term } from "./core.ts";
import { isCons, walk } from "./core.ts";
import type { Goal } from "./relations.ts";

/**
 * alldistincto(xs): true if all elements of xs are distinct.
 */
export function alldistincto(xs: Term): Goal {
  return async function* (s: Subst) {
    const arr = await walk(xs, s);
    let jsArr: any[] = [];
    if (arr && typeof arr === "object" && "tag" in arr) {
      // Convert logic list to JS array
      let cur: Term = arr;
      while (isCons(cur)) {
        jsArr.push(cur.head);
        cur = cur.tail;
      }
    } else if (Array.isArray(arr)) {
      jsArr = arr;
    }
    const seen = new Set();
    let allDistinct = true;
    for (const v of jsArr) {
      const key = JSON.stringify(v);
      if (seen.has(key)) {
        allDistinct = false;
        break;
      }
      seen.add(key);
    }
    if (allDistinct) yield s;
  };
}
