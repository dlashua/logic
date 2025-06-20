// Extended (non-primitive) logic relations for MiniKanren-style logic programming
// These are not part of the minimal core, but are useful for practical logic programming.

import { Term, Subst, Var, walk, lvar, isCons, isNil, nil, cons, LogicList } from './core.ts';
import { Goal, eq, and } from './relation.ts';

/**
 * Wrap a rule so it always deduplicates by the first argument (logic var or ground term).
 */
export function RelUnique(
    rule: (x: Term, ...rest: any[]) => Goal
): (x: Term, ...rest: any[]) => Goal {
    return (x: Term, ...rest: any[]) =>
        uniqueGoalBy(x, rule(x, ...rest));
}

/**
 * Wrap a Goal to deduplicate solutions by a key function or a logic variable/ground term.
 */
export function uniqueGoalBy(key: ((subst: Subst) => string) | Term, goal: Goal): Goal {
    let keyFn: (subst: Subst) => string;
    if (typeof key === 'function') {
        keyFn = key as (subst: Subst) => string;
    } else if (typeof key === 'object' && key !== null && 'tag' in key && key.tag === 'var') {
        keyFn = (subst: Subst) => String(walk(key, subst));
    } else {
        const groundKey = String(key);
        keyFn = () => groundKey;
    }
    return (s: Subst) => uniqueBy(goal(s), keyFn);
}

/**
 * Helper: yield only unique solutions for a given logic variable or tuple (like SQL DISTINCT).
 */
export function distinctVar<T>(sourceVar: Term<T> | Term<T>[], subgoal: Goal): Goal {
    if (!Array.isArray(sourceVar)) {
        sourceVar = [sourceVar];
    }
    return async function* (s: Subst) {
        const seen = new Set<string>();
        for await (const subst of subgoal(s)) {
            const value = sourceVar.map(v => walk(v, subst));
            const key = JSON.stringify(value);
            if (!seen.has(key)) {
                seen.add(key);
                yield subst;
            }
        }
    };
}

/**
 * Remove duplicates from an async generator based on a key function.
 */
export async function* uniqueBy<T, K>(gen: AsyncGenerator<T>, keyFn: (item: T) => K): AsyncGenerator<T> {
    const seen = new Set<K>();
    for await (const item of gen) {
        const key = keyFn(item);
        if (!seen.has(key)) {
            seen.add(key);
            yield item;
        }
    }
}

/**
 * alldistincto(xs): true if all elements of xs are distinct.
 */
export function alldistincto(xs: Term): Goal {
    return async function* (s: Subst) {
        const arr = walk(xs, s);
        let jsArr: any[] = [];
        if (arr && typeof arr === 'object' && 'tag' in arr) {
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

/**
 * removeFirsto(xs, x, ys): ys is xs with the first occurrence of x removed
 */
export function removeFirsto(xs: Term, x: Term, ys: Term): Goal {
    return async function* (s: Subst) {
        const xsVal = walk(xs, s);
        if (isNil(xsVal)) {
            // Removing from empty list yields empty list
            yield* eq(ys, nil)(s);
            return;
        }
        if (isCons(xsVal)) {
            if (xsVal.head === x) {
                // Remove first occurrence
                yield* eq(ys, xsVal.tail)(s);
            } else {
                // Recurse on tail
                const rest = lvar();
                for await (const s1 of and(eq(ys, cons(xsVal.head, rest)), removeFirsto(xsVal.tail, x, rest))(s)) {
                    yield s1;
                }
            }
        }
    };
}

/**
 * Helper: convert a logic list to a JS array (if ground)
 */
export function logicListToArray(list: Term): Term[] {
    const out = [];
    let cur = list;
    while (cur && typeof cur === 'object' && 'tag' in cur && (cur as any).tag === 'cons') {
        out.push((cur as any).head);
        cur = (cur as any).tail;
    }
    return out;
}

/**
 * permuteo(xs, ys): ys is a permutation of logic list xs
 */
export function permuteo(xs: Term, ys: Term): Goal {
    return async function* (s: Subst) {
        const xsVal = walk(xs, s);
        if (isNil(xsVal)) {
            yield* eq(ys, nil)(s);
            return;
        }
        if (isCons(xsVal)) {
            // Convert xsVal to array for iteration
            const arr = logicListToArray(xsVal);
            for (const head of arr) {
                const rest = lvar();
                for await (const s1 of and(removeFirsto(xsVal, head, rest),
                    permuteo(rest, lvar()),
                    eq(ys, cons(head, lvar())))(s)) {
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

/**
 * collectall(x, goal, xs): xs is the list of all values x can take under goal (logic relation version)
 * Usage: collectall(x, membero(x, ...), xs)
 */
export function collecto(x: Term, goal: Goal, xs: Term): Goal {
    return async function* (s: Subst) {
        // Collect all values of x under goal
        const results: Term[] = [];
        for await (const s1 of goal(s)) {
            results.push(walk(x, s1));
        }
        // Convert results to a logic list
        let logicList: LogicList = nil;
        for (let i = results.length - 1; i >= 0; --i) {
            logicList = cons(results[i], logicList);
        }
        // Unify xs with the collected list
        yield* eq(xs, logicList)(s);
    };
}

/**
 * mapo(rel, xs, ys): ys is the result of mapping rel over xs.
 * rel is a binary relation (rel(x, y): Goal)
 * Usage: mapo(rel, xs, ys)
 */
export function mapo(rel: (x: Term, y: Term) => Goal, xs: Term, ys: Term): Goal {
    return async function* (s: Subst) {
        const xsVal = walk(xs, s);
        const ysVal = walk(ys, s);
        if (isNil(xsVal)) {
            // xs is empty, ys must be empty
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
                mapo(rel, xTail, yTail)
            )(s)) {
                yield s1;
            }
        }
    };
}

// Helper to call aggregateVarMulti with goal function arguments
export function aggregateRel(goalFn: (...args: Var[]) => Goal): (...args: Var[]) => Goal {
    return (...args: Var[]) => {
        if (args.length < 2) throw new Error("aggregateRel requires at least two arguments");
        const out = args.at(-1);
        return collecto(
            out,
            goalFn(...args),
            out
        );
    };
}
