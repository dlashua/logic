// Relation helpers for MiniKanren-style logic programming
import { Term, Subst, Var, walk, isVar, unify, lvar, isCons, isNil, nil, cons, LogicList } from './core.ts';

/**
 * A logic goal: a function from a substitution to an async generator of substitutions.
 */
export type Goal = (s: Subst) => AsyncGenerator<Subst>;

/**
 * Succeeds if u and v unify.
 */
export function eq(u: Term, v: Term): Goal {
    return async function* (s: Subst) {
        const s2 = unify(u, v, s);
        if (s2) yield s2;
    };
}

/**
 * Introduces fresh logic variables for a subgoal.
 */
export function fresh(f: (...vars: Var[]) => Goal | [Record<string, Var>, Goal] | [Goal]): Goal {
    const n = f.length;
    return async function* (s: Subst) {
        const vars = Array.from({ length: n }, () => lvar());
        const result = f(...vars);
        let goal: Goal;
        if (Array.isArray(result)) {
            if (result.length === 2 && typeof result[1] === 'function') {
                goal = result[1];
            } else if (result.length === 1 && typeof result[0] === 'function') {
                goal = result[0];
            } else {
                throw new Error('Invalid array structure returned from fresh subgoal');
            }
        } else if (typeof result === 'function') {
            goal = result;
        } else {
            throw new Error('Invalid result from fresh subgoal');
        }
        yield* await goal(s);
    };
}

/**
 * Logical OR: succeeds if either g1 or g2 succeeds.
 */
export function disj(g1: Goal, g2: Goal): Goal {
    return async function* (s: Subst) {
        yield* await g1(s);
        yield* await g2(s);
    };
}

/**
 * Logical AND: succeeds if both g1 and g2 succeed in sequence.
 */
export function conj(g1: Goal, g2: Goal): Goal {
    return async function* (s: Subst) {
        for await (const s1 of g1(s)) {
            yield* await g2(s1);
        }
    };
}

/**
 * Logic programming conditional (multi-statement AND per clause).
 */
export function conde(...clauses: Goal[][]): Goal {
    return async function* (s: Subst) {
        for (const clause of clauses) {
            const goal = clause.reduce((a, b) => (ss: Subst) => conj(a, b)(ss));
            yield* await goal(s);
        }
    };
}

/**
 * Logical AND for multiple goals.
 */
export const and = (...goals: Goal[]) => goals.reduce((a, b) => conj(a, b));
export const all = and;

/**
 * Logical OR for multiple goals.
 */
export const or = (...goals: Goal[]) => {
    if (goals.length === 0) throw new Error("or requires at least one goal");
    return goals.reduce((a, b) => disj(a, b));
};

// --- Relation Constructors ---

/**
 * Create a relation from a predicate that takes any number of arguments.
 */
export function filterRel(pred: (...args: any[]) => boolean): (...args: Term[]) => Goal {
    return (...args: Term[]) =>
        async function* (s: Subst) {
            const vals = args.map(arg => walk(arg, s));
            if (pred(...vals)) yield s;
        };
}

/**
 * Create a relation from a function mapping input terms to an output term.
 */
export function mapRel<F extends (...args: any) => any>(
    fn: F
) {
    return function (...args: Parameters<TermedArgs<F>>) {
        return async function* (s: Subst) {
            const vals = args.map(arg => walk(arg, s));
            const inVals = vals.slice(0, -1);
            const outVal = vals[vals.length - 1];
            if (inVals.every(v => typeof v !== 'undefined' && !isVar(v))) {
                const result = fn(...(inVals as Parameters<F>));
                const s2 = unify(outVal, result, s);
                if (s2) yield s2;
            }
        };
    };
};

export const mapInline = <F extends (...args: any) => any>(fn: F, ...args: Parameters<TermedArgs<F>>) => {
    const mr = mapRel(fn);
    return mr(...args);
}

/**
 * mapInlineLazy: Like mapInline, but stores the mapping as a thunk for lazy evaluation.
 */
export const mapInlineLazy = <F extends (...args: any) => any>(fn: F, ...args: Parameters<TermedArgs<F>>) => {
    return async function* (s: Subst) {
        const inArgs = args.slice(0, -1);
        const outVar = args[args.length - 1];
        // Only store a thunk if outVar is a logic variable
        if (isVar(outVar)) {
            s.set(outVar.id, () => fn(...inArgs.map(arg => walk(arg, s))));
            yield s;
        } else {
            // If outVar is not a variable, behave like mapInline
            const result = fn(...inArgs.map(arg => walk(arg, s)));
            const s2 = unify(outVar, result, s);
            if (s2) yield s2;
        }
    };
}

/**
 * Type helper for mapping function signatures to relation signatures.
 */
export type TermedArgs<T extends (...args: any) => any> =
    T extends (...args: infer A) => infer R
    ? (...args: [...{ [I in keyof A]: (Term<A[I]> | A[I]) }, out: Term<R>]) => Goal
    : never;

/**
 * Type helper for defining a relation with argument inference.
 */
export function Rel(fn: (...args: any[]) => Goal): (...args: any[]) => Goal {
    return fn;
}

/**
 * True if x is a member of a logic list (cons/nil) or JS array.
 */
export function membero(x: Term, list: Term): Goal {
    return async function* (s: Subst) {
        const l = walk(list, s);
        if (l && typeof l === 'object' && 'tag' in l) {
            if ((l as any).tag === 'cons') {
                const s1 = unify(x, (l as any).head, s);
                if (s1) yield s1;
                yield* await membero(x, (l as any).tail)(s);
            }
        } else if (Array.isArray(l)) {
            for (const item of l) {
                const s2 = unify(x, item, s);
                if (s2) yield s2;
            }
        }
    };
}

/**
 * Wrap a rule so it always deduplicates by the first argument (logic var or ground term).
 * 
 * @reconsider
 */
export function RelUnique(
    rule: (x: Term, ...rest: any[]) => Goal
): (x: Term, ...rest: any[]) => Goal {
    return (x: Term, ...rest: any[]) =>
        uniqueGoalBy(x, rule(x, ...rest));
}

/**
 * Wrap a Goal to deduplicate solutions by a key function or a logic variable/ground term.
 * 
 * @reconsider
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
 * 
 * @reconsider
 * @like uniqueGoalBy
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
 * firsto(x, xs): x is the first element (head) of logic list xs.
 * Usage: firsto(x, xs)
 */
export function firsto(x: Term, xs: Term): Goal {
    return async function* (s: Subst) {
        const l = walk(xs, s);
        if (isCons(l)) {
            const consNode = l as { tag: 'cons', head: Term, tail: Term };
            const s1 = unify(x, consNode.head, s);
            if (s1) yield s1;
        }
    };
}

/**
 * resto(xs, tail): tail is the rest (tail) of logic list xs.
 * Usage: resto(xs, tail)
 */
export function resto(xs: Term, tail: Term): Goal {
    return async function* (s: Subst) {
        const l = walk(xs, s);
        if (isCons(l)) {
            const consNode = l as { tag: 'cons', head: Term, tail: Term };
            const s1 = unify(tail, consNode.tail, s);
            if (s1) yield s1;
        }
    };
}

/**
 * appendo(xs, ys, zs): zs is the result of appending logic lists xs and ys.
 * Usage: appendo(xs, ys, zs)
 */
export function appendo(xs: Term, ys: Term, zs: Term): Goal {
    return async function* (s: Subst) {
        const xsVal = walk(xs, s);
        if (isCons(xsVal)) {
            const consNode = xsVal as { tag: 'cons', head: Term, tail: Term };
            // xs = cons(head, tail)
            const head = consNode.head;
            const tail = consNode.tail;
            // zs = cons(head, rest)
            const rest = lvar();
            for await (const s1 of unify(zs, { tag: 'cons', head, tail: rest }, s) ? [unify(zs, { tag: 'cons', head, tail: rest }, s)!] : []) {
                yield* await appendo(tail, ys, rest)(s1);
            }
        } else if (isNil(xsVal)) {
            // xs = nil, so zs = ys
            const s1 = unify(ys, zs, s);
            if (s1) yield s1;
        }
    };
}

/**
 * Helper: convert a logic list to a JS array (if ground)
 */
function logicListToArray(list: Term): Term[] {
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

/**
 * not(goal): Succeeds if the given goal fails (negation as failure).
 */
export function not(goal: Goal): Goal {
    return async function* (s: Subst) {
        const gen = goal(s)[Symbol.asyncIterator]();
        const { done } = await gen.next();
        if (done) yield s;
    };
}

/**
 * pluso(x, y, z): x + y = z
 */
export const pluso = mapRel((x: number, y: number) => x + y);
/**
 * subo(x, y, z): x - y = z
 */
export const subo = mapRel((x: number, y: number) => x - y);
/**
 * multo(x, y, z): x * y = z
 */
export const multo = mapRel((x: number, y: number) => x * y);
/**
 * divo(x, y, z): x / y = z (integer division)
 */
export const divo = mapRel((x: number, y: number) => Math.floor(x / y));
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
 * ifte: if-then-else logic goal.
 * Usage: ifte(cond, thenGoal, elseGoal)
 * - cond: a goal function (async generator)
 * - thenGoal: a goal function (async generator)
 * - elseGoal: a goal function (async generator)
 */
export function ifte(cond: Goal, thenGoal: Goal, elseGoal: Goal): Goal {
    return async function* (s: Subst) {
        let found = false;
        for await (const s1 of cond(s)) {
            found = true;
            for await (const s2 of thenGoal(s1)) {
                yield s2;
            }
        }
        if (!found) {
            for await (const s3 of elseGoal(s)) {
                yield s3;
            }
        }
    };
}

