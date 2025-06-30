import { Goal, LogicList, Subst, Term } from "../core/types.ts"
import {
  walk,
  unify,
  lvar,
  nil,
  cons,
  isCons,
  isLogicList,
  isNil,
  logicListToArray
} from "../core/kernel.ts"
import { and, eq } from "../core/combinators.ts";

/**
 * A goal that succeeds if `x` is a member of the logic `list`.
 * Optimized for both arrays and logic lists.
 */
export function membero(x: Term, list: Term): Goal {
  return async function* (s) {
    const l = await walk(list, s);
    
    // Fast path for arrays
    if (Array.isArray(l)) {
      for (const item of l) {
        const s2 = await unify(x, item, s);
        if (s2) yield s2;
      }
      return;
    }
    
    // Logic list traversal with iterative approach when possible
    if (l && typeof l === "object" && "tag" in l) {
      if ((l as any).tag === "cons") {
        const s1 = await unify(x, (l as any).head, s);
        if (s1) yield s1;
        // Recursive call for tail
        for await (const s2 of membero(x, (l as any).tail)(s)) yield s2;
      }
    }
  };
}

/**
 * A goal that succeeds if `h` is the head of the logic list `l`.
 */
export function firsto(x: Term, xs: Term): Goal {
  return async function* (s) {
    const l = await walk(xs, s);
    if (isCons(l)) {
      const consNode = l as { tag: "cons"; head: Term; tail: Term };
      const s1 = await unify(x, consNode.head, s);
      if (s1) yield s1;
    }
  };
}

/**
 * A goal that succeeds if `t` is the tail of the logic list `l`.
 */
export function resto(xs: Term, tail: Term): Goal {
  return async function* (s) {
    const l = await walk(xs, s);
    if (isCons(l)) {
      const consNode = l as { tag: "cons"; head: Term; tail: Term };
      const s1 = await unify(tail, consNode.tail, s);
      if (s1) yield s1;
    }
  };
}

/**
 * A goal that succeeds if logic list `zs` is the result of appending
 * logic list `ys` to `xs`.
 */
export function appendo(xs: Term, ys: Term, zs: Term): Goal {
  return async function* (s) {
    const xsVal = await walk(xs, s);
    if (isCons(xsVal)) {
      const consNode = xsVal as { tag: "cons"; head: Term; tail: Term };
      const head = consNode.head;
      const tail = consNode.tail;
      const rest = lvar();
      const s1 = await unify(
        zs,
        {
          tag: "cons",
          head,
          tail: rest,
        },
        s,
      );
      if (s1) {
        for await (const s2 of appendo(tail, ys, rest)(s1)) yield s2;
      }
    } else if (isNil(xsVal)) {
      const s1 = await unify(ys, zs, s);
      if (s1) yield s1;
    }
  };
}

/**
 * A goal that unifies the length of an array or logic list with a numeric value.
 * @param arrayOrList The array or logic list to measure
 * @param length The length to unify with
 */
export function lengtho(arrayOrList: Term, length: Term): Goal {
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
      const arr = logicListToArray(xsVal as LogicList);
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


