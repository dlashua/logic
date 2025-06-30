import {
  cons,
  isCons,
  isNil,
  isLogicList,
  lvar,
  nil,
  walk,
  and,
  eq,
  Goal,
  logicListToArray,
  unify,
} from "./core.ts"
import type { Term, Subst } from "./core.ts";

// Re-export the core list functions for backward compatibility
export { membero, firsto, resto, appendo } from "./core.ts";

/**
 * A goal that unifies the length of an array or logic list with a numeric value.
 * @param arrayOrList The array or logic list to measure
 * @param length The length to unify with
 */
export function arrayLength(arrayOrList: Term, length: Term): Goal {
  return async function* arrayLengthGoal(s: Subst) {
    const walkedArray = await walk(arrayOrList, s);
    const walkedLength = await walk(length, s);
    
    let actualLength: number;
    
    // Handle logic lists
    if (isLogicList(walkedArray)) {
      actualLength = logicListToArray(walkedArray).length;
    }
    // Handle regular arrays
    else if (Array.isArray(walkedArray)) {
      actualLength = walkedArray.length;
    }
    // If neither array nor logic list, fail
    else {
      return;
    }
    
    // Unify the actual length with the length term
    const unified = await unify(actualLength, walkedLength, s);
    if (unified !== null) {
      yield unified;
    }
  };
}

export function permuteo(xs: Term, ys: Term): Goal {
  return async function* (s) {
    const xsVal = walk(xs, s);
    if (isNil(xsVal)) {
      yield* eq(ys, nil)(s);
      return;
    }
    if (isCons(xsVal)) {
      const arr = logicListToArray(xsVal);
      for (const head of arr) {
        const rest = lvar();
        for await (const s1 of and(
          removeFirsto(xsVal, head, rest),
          permuteo(rest, lvar()),
          eq(ys, cons(head, lvar())),
        )(s)) {
          const ysVal2 = walk(ys, s1);
          if (isCons(ysVal2)) {
            for await (const s2 of eq(ysVal2.tail, walk(lvar(), s1))(s1)) {
              yield s2;
            }
          }
        }
      }
    }
  };
}

export function mapo(
  rel: (x: Term, y: Term) => Goal,
  xs: Term,
  ys: Term,
): Goal {
  return async function* (s) {
    const xsVal = await walk(xs, s);
    if (isNil(xsVal)) {
      yield* eq(ys, nil)(s);
      return;
    }
    if (isCons(xsVal)) {
      const xHead = xsVal.head;
      const xTail = xsVal.tail;
      const yHead = lvar();
      const yTail = lvar();
      for await (const s1 of and(
        eq(ys, cons(yHead, yTail)),
        rel(xHead, yHead),
        mapo(rel, xTail, yTail),
      )(s)) {
        yield s1;
      }
    }
  };
}

export function removeFirsto(xs: Term, x: Term, ys: Term): Goal {
  return async function* (s) {
    const xsVal = await walk(xs, s);
    if (isNil(xsVal)) {
      // If we reach nil without finding the element, fail
      return;
    }
    if (isCons(xsVal)) {
      const walkedX = await walk(x, s);
      const walkedHead = await walk(xsVal.head, s);
      
      if (JSON.stringify(walkedHead) === JSON.stringify(walkedX)) {
        // Found the element, remove it
        yield* eq(ys, xsVal.tail)(s);
      } else {
        // Element not at head, try to remove from tail
        const rest = lvar();
        for await (const s1 of and(
          eq(ys, cons(xsVal.head, rest)),
          removeFirsto(xsVal.tail, x, rest),
        )(s)) {
          yield s1;
        }
      }
    }
  };
}

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
