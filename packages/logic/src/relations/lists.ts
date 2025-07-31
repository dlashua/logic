import { and, eq } from "../core/combinators.js";
import {
  cons,
  enrichGroupInput,
  isCons,
  isLogicList,
  isNil,
  logicListToArray,
  lvar,
  nil,
  unify,
  walk,
} from "../core/kernel.js";
import { SimpleObservable } from "observable";
import type { Goal, LogicList, Subst, Term } from "../core/types.js";

export function membero(x: Term, list: Term): Goal {
  return enrichGroupInput(
    "membero",
    [],
    [],
    (input$) =>
      new SimpleObservable<Subst>((observer) => {
        const subscriptions: any[] = [];
        let cancelled = false;
        let active = 0;
        let inputComplete = false;

        const checkComplete = () => {
          if (inputComplete && active === 0 && !cancelled) {
            observer.complete?.();
          }
        };

        const inputSub = input$.subscribe({
          next: (s) => {
            if (cancelled) return;

            const l = walk(list, s);
            // Fast path for arrays
            if (Array.isArray(l)) {
              for (let i = 0; i < l.length; i++) {
                if (cancelled) break;
                const item = l[i];
                const s2 = unify(x, item, s);
                if (s2) observer.next(s2);
              }
            } else if (
              l &&
              typeof l === "object" &&
              "tag" in l &&
              (l as any).tag === "cons"
            ) {
              if (cancelled) return;

              const s1 = unify(x, (l as any).head, s);
              if (s1) observer.next(s1);

              active++;
              // Recursive call for tail
              const sub = membero(
                x,
                (l as any).tail,
              )(SimpleObservable.of(s)).subscribe({
                next: (result) => {
                  if (!cancelled) observer.next(result);
                },
                error: (err) => {
                  if (!cancelled) observer.error?.(err);
                },
                complete: () => {
                  active--;
                  checkComplete();
                },
              });
              subscriptions.push(sub);
            }
            // If neither array nor cons, do nothing (no result)
          },
          error: (err) => {
            if (!cancelled) observer.error?.(err);
          },
          complete: () => {
            inputComplete = true;
            checkComplete();
          },
        });

        subscriptions.push(inputSub);

        return () => {
          cancelled = true;
          subscriptions.forEach((sub) => {
            try {
              sub?.unsubscribe?.();
            } catch (e) {
              // Ignore cleanup errors
            }
          });
          subscriptions.length = 0;
        };
      }),
  );
}

/**
 * A goal that succeeds if `h` is the head of the logic list `l`.
 */
export function firsto(x: Term, xs: Term): Goal {
  return (input$) =>
    new SimpleObservable<Subst>((observer) => {
      input$.subscribe({
        next: (s) => {
          const l = walk(xs, s);
          if (isCons(l)) {
            const consNode = l as { tag: "cons"; head: Term; tail: Term };
            const s1 = unify(x, consNode.head, s);
            if (s1) observer.next(s1);
          }
          observer.complete?.();
        },
        error: observer.error,
        complete: observer.complete,
      });
    });
}

/**
 * A goal that succeeds if `t` is the tail of the logic list `l`.
 */
export function resto(xs: Term, tail: Term): Goal {
  return (input$) =>
    new SimpleObservable<Subst>((observer) => {
      input$.subscribe({
        next: (s) => {
          const l = walk(xs, s);
          if (isCons(l)) {
            const consNode = l as { tag: "cons"; head: Term; tail: Term };
            const s1 = unify(tail, consNode.tail, s);
            if (s1) observer.next(s1);
          }
          observer.complete?.();
        },
        error: observer.error,
        complete: observer.complete,
      });
    });
}

/**
 * A goal that succeeds if logic list `zs` is the result of appending
 * logic list `ys` to `xs`.
 */
export function appendo(xs: Term, ys: Term, zs: Term): Goal {
  return (input$) =>
    new SimpleObservable<Subst>((observer) => {
      input$.subscribe({
        next: (s) => {
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
              appendo(
                tail,
                ys,
                rest,
              )(SimpleObservable.of(s1)).subscribe({
                next: observer.next,
                error: observer.error,
                complete: observer.complete,
              });
              return;
            }
          } else if (isNil(xsVal)) {
            const s1 = unify(ys, zs, s);
            if (s1) observer.next(s1);
          }
          observer.complete?.();
        },
        error: observer.error,
        complete: observer.complete,
      });
    });
}

/**
 * A goal that unifies the length of an array or logic list with a numeric value.
 * @param arrayOrList The array or logic list to measure
 * @param length The length to unify with
 */
export function lengtho(arrayOrList: Term, length: Term): Goal {
  return (input$) =>
    new SimpleObservable<Subst>((observer) => {
      input$.subscribe({
        next: (s) => {
          const walkedArray = walk(arrayOrList, s);
          const walkedLength = walk(length, s);
          let actualLength: number;
          if (isLogicList(walkedArray)) {
            actualLength = logicListToArray(walkedArray).length;
          } else if (Array.isArray(walkedArray)) {
            actualLength = walkedArray.length;
          } else {
            // observer.complete?.();
            return;
          }
          const unified = unify(actualLength, walkedLength, s);
          if (unified !== null) {
            observer.next(unified);
          }
          // observer.complete?.();
        },
        error: observer.error,
        complete: observer.complete,
      });
    });
}

export function permuteo(xs: Term, ys: Term): Goal {
  return (input$) =>
    new SimpleObservable<Subst>((observer) => {
      input$.subscribe({
        next: (s) => {
          const xsVal = walk(xs, s);
          if (isNil(xsVal)) {
            eq(
              ys,
              nil,
            )(SimpleObservable.of(s)).subscribe({
              next: observer.next,
              error: observer.error,
              complete: observer.complete,
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
              )(SimpleObservable.of(s)).subscribe({
                next: (s1) => {
                  const ysVal2 = walk(ys, s1);
                  if (isCons(ysVal2)) {
                    eq(
                      ysVal2.tail,
                      walk(lvar(), s1),
                    )(SimpleObservable.of(s1)).subscribe({
                      next: observer.next,
                      error: observer.error,
                    });
                  }
                },
                error: observer.error,
                complete: () => {
                  completedCount++;
                  if (completedCount === arr.length) {
                    observer.complete?.();
                  }
                },
              });
            }
            if (arr.length === 0) {
              observer.complete?.();
            }
          } else {
            observer.complete?.();
          }
        },
        error: observer.error,
        complete: observer.complete,
      });
    });
}

export function mapo(
  rel: (x: Term, y: Term) => Goal,
  xs: Term,
  ys: Term,
): Goal {
  return (input$) =>
    new SimpleObservable<Subst>((observer) => {
      let active = 0;
      let completed = false;
      const subscription = input$.subscribe({
        next: (s) => {
          active++;
          const xsVal = walk(xs, s);
          if (isNil(xsVal)) {
            eq(
              ys,
              nil,
            )(SimpleObservable.of(s)).subscribe({
              next: observer.next,
              error: observer.error,
              complete: () => {
                active--;
                if (completed && active === 0) observer.complete?.();
              },
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
            )(SimpleObservable.of(s)).subscribe({
              next: observer.next,
              error: observer.error,
              complete: () => {
                active--;
                if (completed && active === 0) observer.complete?.();
              },
            });
          } else {
            active--;
            if (completed && active === 0) observer.complete?.();
          }
        },
        error: observer.error,
        complete: () => {
          completed = true;
          if (active === 0) observer.complete?.();
        },
      });
      return () => subscription.unsubscribe?.();
    });
}

export function removeFirsto(xs: Term, x: Term, ys: Term): Goal {
  return (input$) =>
    new SimpleObservable<Subst>((observer) => {
      let active = 0;
      let completed = false;
      const subscription = input$.subscribe({
        next: (s) => {
          active++;
          const xsVal = walk(xs, s);
          if (isNil(xsVal)) {
            active--;
            if (completed && active === 0) observer.complete?.();
            return;
          }
          if (isCons(xsVal)) {
            const walkedX = walk(x, s);
            const walkedHead = walk(xsVal.head, s);
            if (JSON.stringify(walkedHead) === JSON.stringify(walkedX)) {
              eq(
                ys,
                xsVal.tail,
              )(SimpleObservable.of(s)).subscribe({
                next: observer.next,
                error: observer.error,
                complete: () => {
                  active--;
                  if (completed && active === 0) observer.complete?.();
                },
              });
            } else {
              const rest = lvar();
              and(
                eq(ys, cons(xsVal.head, rest)),
                removeFirsto(xsVal.tail, x, rest),
              )(SimpleObservable.of(s)).subscribe({
                next: observer.next,
                error: observer.error,
                complete: () => {
                  active--;
                  if (completed && active === 0) observer.complete?.();
                },
              });
            }
          } else {
            active--;
            if (completed && active === 0) observer.complete?.();
          }
        },
        error: observer.error,
        complete: () => {
          completed = true;
          if (active === 0) observer.complete?.();
        },
      });
      return () => subscription.unsubscribe?.();
    });
}

/**
 * alldistincto(xs): true if all elements of xs are distinct.
 */
export function alldistincto(xs: Term): Goal {
  return (input$) =>
    new SimpleObservable<Subst>((observer) => {
      input$.subscribe({
        next: (s) => {
          const arr = walk(xs, s);
          let jsArr: any[] = [];
          if (arr && typeof arr === "object" && "tag" in arr) {
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
        },
        error: observer.error,
        complete: observer.complete,
      });
    });
}
