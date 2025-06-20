// Relation helpers for MiniKanren-style logic programming
import { Term, Subst, Var, walk, isVar, unify, lvar } from './core.ts';

export type Goal = (s: Subst) => AsyncGenerator<Subst>;

export function eq(u: Term, v: Term): Goal {
    return async function* (s: Subst) {
        const s2 = unify(u, v, s);
        if (s2) yield s2;
    };
}

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

export function disj(g1: Goal, g2: Goal): Goal {
    return async function* (s: Subst) {
        yield* await g1(s);
        yield* await g2(s);
    };
}

export function conj(g1: Goal, g2: Goal): Goal {
    return async function* (s: Subst) {
        for await (const s1 of g1(s)) {
            yield* await g2(s1);
        }
    };
}

export function conde(...clauses: Goal[][]): Goal {
    return async function* (s: Subst) {
        for (const clause of clauses) {
            const goal = clause.reduce((a, b) => (ss: Subst) => conj(a, b)(ss));
            yield* await goal(s);
        }
    };
}

export const oldand = (...goals: Goal[]) => conde(goals);
export const and = (...goals: Goal[]) => goals.reduce((a, b) => conj(a, b));
export const or = (...goals: Goal[]) => {
    if (goals.length === 0) throw new Error("or requires at least one goal");
    return goals.reduce((a, b) => disj(a, b));
};

// filterRel, mapRel, Rel, TermedArgs, membero, RelUnique, uniqueGoalBy, distinctVar, uniqueBy
export function filterRel(pred: (...args: any[]) => boolean): (...args: Term[]) => Goal {
    return (...args: Term[]) =>
        async function* (s: Subst) {
            const vals = args.map(arg => walk(arg, s));
            if (pred(...vals)) yield s;
        };
}

export function mapRel<F extends (...args: any) => any>(fn: F): (...args: any[]) => Goal {
    return function (...args: Term<any>[]) {
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
}

export type TermedArgs<T extends (...args: any) => any> =
    T extends (...args: infer A) => any
    ? (...args: [...{ [I in keyof A]: (Term<A[I]> | A[I]) }, Term<any>]) => Goal
    : never;

export function Rel(fn: (...args: any[]) => Goal): (...args: any[]) => Goal {
    return fn;
}

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

export function RelUnique(
    rule: (x: Term, ...rest: any[]) => Goal
): (x: Term, ...rest: any[]) => Goal {
    return (x: Term, ...rest: any[]) =>
        uniqueGoalBy(x, rule(x, ...rest));
}

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
