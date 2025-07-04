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
import { SimpleObservable } from "../core/observable.ts";

/**
 * A goal that succeeds if `x` is a member of the logic `list`.
 * Optimized for both arrays and logic lists.
 */
export function membero(x: Term, list: Term): Goal {
  return (s) => new SimpleObservable<Subst>((observer) => {
    let cancelled = false;
    const l = walk(list, s);
    
    // Fast path for arrays
    if (Array.isArray(l)) {
      // Process items synchronously, but check for cancellation
      // Use microtask scheduling to allow cancellation between items
      let currentIndex = 0;
      
      const processNext = () => {
        while (currentIndex < l.length && !cancelled) {
          const item = l[currentIndex++];
          const s2 = unify(x, item, s);
          if (s2) observer.next(s2);
          
          // Allow cancellation between items by yielding control
          if (currentIndex < l.length) {
            queueMicrotask(processNext);
            return;
          }
        }
        
        if (!cancelled) observer.complete?.();
      };
      
      queueMicrotask(processNext);
      
      return () => {
        cancelled = true;
      };
    }
    
    // Logic list traversal with iterative approach when possible
    if (l && typeof l === "object" && "tag" in l) {
      if ((l as any).tag === "cons") {
        const s1 = unify(x, (l as any).head, s);
        if (s1) observer.next(s1);
        
        // Recursive call for tail
        const tailSubscription = membero(x, (l as any).tail)(s).subscribe({
          next: observer.next,
          error: observer.error,
          complete: observer.complete
        });
        return () => {
          cancelled = true;
          tailSubscription.unsubscribe();
        };
      }
    }
    
    observer.complete?.();

    return () => {
      cancelled = true;
    }
  });
}

/**
 * A goal that succeeds if `h` is the head of the logic list `l`.
 */
export function firsto(x: Term, xs: Term): Goal {
  return (s) => new SimpleObservable<Subst>((observer) => {
    const l = walk(xs, s);
    if (isCons(l)) {
      const consNode = l as { tag: "cons"; head: Term; tail: Term };
      const s1 = unify(x, consNode.head, s);
      if (s1) observer.next(s1);
    }
    observer.complete?.();
  });
}

/**
 * A goal that succeeds if `t` is the tail of the logic list `l`.
 */
export function resto(xs: Term, tail: Term): Goal {
  return (s) => new SimpleObservable<Subst>((observer) => {
    const l = walk(xs, s);
    if (isCons(l)) {
      const consNode = l as { tag: "cons"; head: Term; tail: Term };
      const s1 = unify(tail, consNode.tail, s);
      if (s1) observer.next(s1);
    }
    observer.complete?.();
  });
}

/**
 * A goal that succeeds if logic list `zs` is the result of appending
 * logic list `ys` to `xs`.
 */
export function appendo(xs: Term, ys: Term, zs: Term): Goal {
  return (s) => new SimpleObservable<Subst>((observer) => {
    const xsVal = walk(xs, s);
    
    if (isCons(xsVal)) {
      const consNode = xsVal as { tag: "cons"; head: Term; tail: Term };
      const head = consNode.head;
      const tail = consNode.tail;
      const rest = lvar();
      const s1 = unify(
        zs,
        {
          tag: "cons",
          head,
          tail: rest,
        },
        s,
      );
      if (s1) {
        appendo(tail, ys, rest)(s1).subscribe({
          next: observer.next,
          error: observer.error,
          complete: observer.complete
        });
        return;
      }
    } else if (isNil(xsVal)) {
      const s1 = unify(ys, zs, s);
      if (s1) observer.next(s1);
    }
    
    observer.complete?.();
  });
}

/**
 * A goal that unifies the length of an array or logic list with a numeric value.
 * @param arrayOrList The array or logic list to measure
 * @param length The length to unify with
 */
export function lengtho(arrayOrList: Term, length: Term): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
    const walkedArray = walk(arrayOrList, s);
    const walkedLength = walk(length, s);
    
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
      observer.complete?.();
      return;
    }
    
    // Unify the actual length with the length term
    const unified = unify(actualLength, walkedLength, s);
    if (unified !== null) {
      observer.next(unified);
    }
    
    observer.complete?.();
  });
}

export function permuteo(xs: Term, ys: Term): Goal {
  return (s) => new SimpleObservable<Subst>((observer) => {
    const xsVal = walk(xs, s);
    if (isNil(xsVal)) {
      eq(ys, nil)(s).subscribe({
        next: observer.next,
        error: observer.error,
        complete: observer.complete
      });
      return;
    }
    if (isCons(xsVal)) {
      const arr = logicListToArray(xsVal as LogicList);
      let completedCount = 0;
      
      for (const head of arr) {
        const rest = lvar();
        and(
          removeFirsto(xsVal, head, rest),
          permuteo(rest, lvar()),
          eq(ys, cons(head, lvar())),
        )(s).subscribe({
          next: (s1) => {
            const ysVal2 = walk(ys, s1);
            if (isCons(ysVal2)) {
              eq(ysVal2.tail, walk(lvar(), s1))(s1).subscribe({
                next: observer.next,
                error: observer.error
              });
            }
          },
          error: observer.error,
          complete: () => {
            completedCount++;
            if (completedCount === arr.length) {
              observer.complete?.();
            }
          }
        });
      }
      
      if (arr.length === 0) {
        observer.complete?.();
      }
    } else {
      observer.complete?.();
    }
  });
}

export function mapo(
  rel: (x: Term, y: Term) => Goal,
  xs: Term,
  ys: Term,
): Goal {
  return (s) => new SimpleObservable<Subst>((observer) => {
    const xsVal = walk(xs, s);
    if (isNil(xsVal)) {
      eq(ys, nil)(s).subscribe({
        next: observer.next,
        error: observer.error,
        complete: observer.complete
      });
      return;
    }
    if (isCons(xsVal)) {
      const xHead = xsVal.head;
      const xTail = xsVal.tail;
      const yHead = lvar();
      const yTail = lvar();
      and(
        eq(ys, cons(yHead, yTail)),
        rel(xHead, yHead),
        mapo(rel, xTail, yTail),
      )(s).subscribe({
        next: observer.next,
        error: observer.error,
        complete: observer.complete
      });
    } else {
      observer.complete?.();
    }
  });
}

export function removeFirsto(xs: Term, x: Term, ys: Term): Goal {
  return (s) => new SimpleObservable<Subst>((observer) => {
    const xsVal = walk(xs, s);
    if (isNil(xsVal)) {
      // If we reach nil without finding the element, fail
      observer.complete?.();
      return;
    }
    if (isCons(xsVal)) {
      const walkedX = walk(x, s);
      const walkedHead = walk(xsVal.head, s);
      
      if (JSON.stringify(walkedHead) === JSON.stringify(walkedX)) {
        // Found the element, remove it
        eq(ys, xsVal.tail)(s).subscribe({
          next: observer.next,
          error: observer.error,
          complete: observer.complete
        });
      } else {
        // Element not at head, try to remove from tail
        const rest = lvar();
        and(
          eq(ys, cons(xsVal.head, rest)),
          removeFirsto(xsVal.tail, x, rest),
        )(s).subscribe({
          next: observer.next,
          error: observer.error,
          complete: observer.complete
        });
      }
    } else {
      observer.complete?.();
    }
  });
}

/**
 * alldistincto(xs): true if all elements of xs are distinct.
 */
export function alldistincto(xs: Term): Goal {
  return (s: Subst) => new SimpleObservable<Subst>((observer) => {
    const arr = walk(xs, s);
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
    if (allDistinct) observer.next(s);
    observer.complete?.();
  });
}