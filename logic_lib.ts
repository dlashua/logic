// MiniKanren-style logic programming core in TypeScript

import { set } from "lodash";

// Logic variable representation
export type Var = { tag: 'var', id: number };
let varCounter = 0;
function lvar(): Var {
    return { tag: 'var', id: varCounter++ };
}

// Term type with generics for better type hinting
export type Term<T = unknown> = Var | T | Term<T>[] | null | undefined;

// Substitution: mapping from variable id to value
export type Subst = Map<number, Term>;

// Walk: find the value a variable is bound to
export function walk(u: Term, s: Subst): Term {
    if (isVar(u) && s.has(u.id)) {
        return walk(s.get(u.id)!, s);
    }
    return u;
}

export function isVar(x: Term): x is Var {
    return typeof x === 'object' && x !== null && 'tag' in x && (x as Var).tag === 'var';
}

// Unification
export function unify(u: Term, v: Term, s: Subst): Subst | null {
    u = walk(u, s);
    v = walk(v, s);
    if (isVar(u)) {
        return extS(u, v, s);
    } else if (isVar(v)) {
        return extS(v, u, s);
    } else if (Array.isArray(u) && Array.isArray(v) && u.length === v.length) {
        for (let i = 0; i < u.length; i++) {
            const sNext = unify(u[i], v[i], s);
            if (!sNext) return null;
            s = sNext;
        }
        return s;
    } else if (u === v) {
        return s;
    } else {
        return null;
    }
}

function extS(v: Var, val: Term, s: Subst): Subst | null {
    if (occursCheck(v, val, s)) return null;
    const s2 = new Map(s);
    s2.set(v.id, val);
    return s2;
}

function occursCheck(v: Var, x: Term, s: Subst): boolean {
    x = walk(x, s);
    if (isVar(x)) return v.id === x.id;
    if (Array.isArray(x)) return x.some(e => occursCheck(v, e, s));
    return false;
}

// Stream type for results (now a generator)
export type Stream = Generator<Subst>;

// Goal type (returns a generator)
export type Goal = (s: Subst) => Stream;

// eq goal
export function eq(u: Term, v: Term): Goal {
    return function* (s: Subst) {
        const s2 = unify(u, v, s);
        if (s2) yield s2;
    };
}

// fresh: introduce new logic variables (infer count from callback arity only)
export function fresh(f: (...vars: Var[]) => Goal | [Record<string, Var>, Goal] | [Goal]): Goal {
    const n = f.length;
    return function* (s: Subst) {
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
        yield* goal(s);
    };
}

// disjunction (logical or)
export function disj(g1: Goal, g2: Goal): Goal {
    return function* (s: Subst) {
        yield* g1(s);
        yield* g2(s);
    };
}

// conjunction (logical and)
export function conj(g1: Goal, g2: Goal): Goal {
    return function* (s: Subst) {
        for (const s1 of g1(s)) {
            yield* g2(s1);
        }
    };
}

// conde: logic programming conditional (multi-statement AND per clause)
export function conde(...clauses: Goal[][]): Goal {
    return function* (s: Subst) {
        for (const clause of clauses) {
            const goal = clause.reduce((a, b) => (ss: Subst) => conj(a, b)(ss));
            yield* goal(s);
        }
    };
}

// 'and' is now an alias for conde with a single clause
export const oldand = (...goals: Goal[]) => conde(goals);
export const and = (...goals: Goal[]) => goals.reduce((a, b) => conj(a, b));
export const or = (...goals: Goal[]) => {
    if (goals.length === 0) throw new Error("or requires at least one goal");
    return goals.reduce((a, b) => disj(a, b));
};


// Run: get results for a goal as a generator, with ergonomic object mapping using logic variable IDs
export function run<Fmt extends Record<string, Term<any>> = Record<string, Term<any>>>(
    f: (...vars: Term<any>[]) => [Fmt, Goal] | Goal | [Goal],
    n: number = Infinity
) {
    const s0: Subst = new Map();
    const vars = Array.from({ length: f.length }, () => lvar());
    const result = f(...vars);
    function* substToObjGen(substs: Generator<Subst>, formatter: Fmt): Generator<{ [K in keyof Fmt]: Term }> {
        let count = 0;
        for (const s of substs) {
            if (count++ >= n) break;
            const out: Partial<{ [K in keyof Fmt]: Term }> = {};
            for (const key in formatter) {
                const v = formatter[key];
                if (v && typeof v === 'object' && 'id' in v) {
                    out[key] = walk(v, s);
                } else {
                    out[key] = v;
                }
            }
            yield out as { [K in keyof Fmt]: Term };
        }
    }
    function* substToObjGenVars(substs: Generator<Subst>): Generator<{ [K in keyof Fmt]: Term }> {
        let count = 0;
        for (const s of substs) {
            if (count++ >= n) break;
            const out: Record<string, Term> = {};
            vars.forEach(v => {
                out[v.id] = walk(v, s);
            });
            yield out as { [K in keyof Fmt]: Term };
        }
    }
    let gen: Generator<{ [K in keyof Fmt]: Term }>;
    if (Array.isArray(result)) {
        if (result.length === 2 && typeof result[0] === 'object' && result[0] !== null) {
            const formatter = result[0] as Fmt;
            const goal = result[1] as Goal;
            gen = substToObjGen(goal(s0), formatter);
        } else if (result.length === 1 && typeof result[0] === 'function') {
            const goal = result[0] as Goal;
            gen = substToObjGenVars(goal(s0));
        } else {
            throw new Error("Invalid array structure returned from goal function");
        }
    } else {
        const goal = result as Goal;
        gen = substToObjGenVars(goal(s0));
    }
    return withFluentGen(gen);
}

export function makeFacts() {
    const facts: Term[][] = [];
    // Use Map/Set for indexes (default approach)
    const indexes = new Map<number, Map<any, Set<number>>>();

    function intersect<F>(set_a: Set<F>, set_b: Set<F>) {
        const set_n = new Set<F>();
        set_a.forEach(item => {
            if (set_b.has(item)) {
                set_n.add(item);
            }
        });
        return set_n;
    }

    function goalFn(...query: Term[]): Goal {
        return function* (s: Subst) {
            const walkedQuery = query.map(term => walk(term, s));

            let intersection: Set<number> = new Set<number>();
            let found = false;
            let i = -1;
            for (const wq of walkedQuery) {
                i++;
                if (isVar(wq)) continue;
                if (!indexes.has(i)) continue;
                const index = indexes.get(i);
                if (!index) continue;
                const factNums = index.get(wq);
                if (!factNums) continue;

                if (!found) {
                    found = true;
                    intersection = new Set(factNums);
                    continue;
                }

                intersection = intersect(intersection, factNums);
                if (intersection.size === 0) break;
            }

            if (!found) {
                for (const fact of facts) {
                    const s1 = unify(query, fact, s);
                    if (s1) {
                        yield s1;
                    }
                }
                return;
            }

            for (const factIndex of intersection) {
                const fact = facts[factIndex];
                const s1 = unify(query, fact, s);
                if (s1) {
                    yield s1;
                }
            }
        };
    }

    const isIndexable = (v: any) =>
        typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null;

    const wrapper = (...query: Term[]): Goal => goalFn(...query);

    wrapper.set = (...fact: Term[]) => {
        const factIndex = facts.length;
        facts.push(fact);
        fact.forEach((term, i) => {
            if (isIndexable(term)) {
                let index = indexes.get(i);
                if (!index) {
                    index = new Map<any, Set<number>>();
                    indexes.set(i, index);
                }
                let set = index.get(term);
                if (!set) {
                    set = new Set<number>();
                    index.set(term, set);
                }
                set.add(factIndex);
            }
        });
    };

    wrapper.raw = facts;
    wrapper.indexes = indexes;
    return wrapper;
}

// makeFactsObj: object-based fact storage and querying
export function makeFactsObj(keys: string[]) {
    const facts: Record<string, Term>[] = [];
    // Indexes: Map<key, Map<value, Set<factIndex>>>
    const indexes = new Map<string, Map<any, Set<number>>>();

    function intersect<F>(set_a: Set<F>, set_b: Set<F>) {
        const set_n = new Set<F>();
        set_a.forEach(item => {
            if (set_b.has(item)) {
                set_n.add(item);
            }
        });
        return set_n;
    }

    function goalFn(queryObj: Record<string, Term>): Goal {
        const keys = Object.keys(queryObj);
        return function* (s: Subst) {
            // Walk all query terms
            const walkedQuery: Record<string, Term> = {};
            for (const k of keys) {
                walkedQuery[k] = walk(queryObj[k], s);
            }

            // Index intersection logic
            let intersection: Set<number> = new Set<number>();
            let found = false;
            for (const k of keys) {
                const wq = walkedQuery[k];
                if (isVar(wq)) continue;
                const index = indexes.get(k);
                if (!index) continue;
                const factNums = index.get(wq);
                if (!factNums) continue;
                if (!found) {
                    found = true;
                    intersection = new Set(factNums);
                    continue;
                }
                intersection = intersect(intersection, factNums);
                if (intersection.size === 0) break;
            }
            // console.log("intersection", queryObj, intersection);

            if (!found) {
                for (const fact of facts) {
                    const s1 = unify(keys.map(k => queryObj[k]), keys.map(k => fact[k]), s);
                    if (s1) yield s1;
                }
                return;
            }

            for (const factIndex of intersection) {
                const fact = facts[factIndex];
                const s1 = unify(keys.map(k => queryObj[k]), keys.map(k => fact[k]), s);
                if (s1) yield s1;
            }
        };
    }

    const isIndexable = (v: any) =>
        typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null;

    // Wrapper function for querying
    function wrapper(queryObj: Record<string, Term>): Goal {
        return goalFn(queryObj);
    }

    // Set method for adding facts
    wrapper.set = (factObj: Record<string, Term>) => {
        const keys = Object.keys(factObj);
        const factIndex = facts.length;
        // Ensure all keys are present
        const fact: Record<string, Term> = {};
        for (const k of keys) {
            fact[k] = factObj[k];
        }
        facts.push(fact);
        for (const k of keys) {
            const term = fact[k];
            if (isIndexable(term)) {
                let index = indexes.get(k);
                if (!index) {
                    index = new Map<any, Set<number>>();
                    indexes.set(k, index);
                }
                let set = index.get(term);
                if (!set) {
                    set = new Set<number>();
                    index.set(term, set);
                }
                set.add(factIndex);
            }
        }
    };

    wrapper.raw = facts;
    wrapper.indexes = indexes;
    wrapper.keys = keys;
    return wrapper;
}

// aggregator: collect all possible values of a logic variable into an array and bind to sourceVar in a single solution
export function aggregateVar(sourceVar: Var, subgoal: Goal): Goal {
    return function* (s: Subst) {
        const results: Term[] = [];
        for (const subst of subgoal(s)) {
            results.push(walk(sourceVar, subst));
        }
        const s2 = new Map(s);
        s2.set(sourceVar.id, results);
        yield s2;
    };
}

// aggregator: for each unique combination of groupVars, aggregate all values of each aggVar in aggVars, and yield a substitution with arrays bound to each aggVar
export function aggregateVarMulti(groupVars: Var[], aggVars: Var[], subgoal: Goal): Goal {
    return function* (s: Subst) {
        // Map from group key to arrays for each aggVar
        const groupMap = new Map<string, Term[][]>();
        for (const subst of subgoal(s)) {
            // Compute group key
            const groupKey = JSON.stringify(groupVars.map(v => walk(v, subst)));
            // Get or create arrays for each aggVar
            let aggArrays = groupMap.get(groupKey);
            if (!aggArrays) {
                aggArrays = aggVars.map(() => []);
                groupMap.set(groupKey, aggArrays);
            }
            // Push each aggVar value
            aggVars.forEach((v, i) => {
                aggArrays[i].push(walk(v, subst));
            });
        }
        if (groupMap.size === 0) {
            const s2 = new Map(s);
            aggVars.forEach((v, i) => s2.set(v.id, []));
            yield s2;
            return;
        }
        // Yield one subst per group, with arrays bound to each aggVar
        for (const [groupKey, aggArrays] of groupMap.entries()) {
            const groupValues = JSON.parse(groupKey);
            const s2 = new Map(s);
            groupVars.forEach((v, i) => s2.set(v.id, groupValues[i]));
            aggVars.forEach((v, i) => s2.set(v.id, aggArrays[i]));
            yield s2;
        }
    };
}

// Helper: yield only unique solutions for a given logic variable or tuple (like SQL DISTINCT)
export function distinctVar<T>(sourceVar: Term<T> | Term<T>[], subgoal: Goal): Goal {
    if (!Array.isArray(sourceVar)) {
        sourceVar = [sourceVar];
    }
    return function* (s: Subst) {
        const seen = new Set<string>();
        for (const subst of subgoal(s)) {
            const value = sourceVar.map(v => walk(v, subst));
            const key = JSON.stringify(value);
            if (!seen.has(key)) {
                seen.add(key);
                yield subst;
            }
        }
    };
}



// Aliases for commonly used names
export const not = (g: Goal): Goal => function* (s: Subst) {
    const it = g(s);
    if (it.next().done) yield s;
};

// Export lvar for user
export { lvar };

// Utility: keyBy for generators/iterables (like lodash's keyBy)
export function groupBy<T, K, V>(
    gen: Iterable<T>,
    keyFn: (item: T) => K,
    valueFn: (item: T) => V
): Map<K, V[]> {
    const map = new Map<K, V[]>();
    for (const item of gen) {
        const key = keyFn(item);
        const value = valueFn(item);
        if (!map.has(key)) map.set(key, []);
        map.get(key)!.push(value);
    }
    return map;
}

// Helper: remove duplicates from a generator based on a key function
export function* uniqueBy<T, K>(gen: Iterable<T>, keyFn: (item: T) => K): Generator<T> {
    const seen = new Set<K>();
    for (const item of gen) {
        const key = keyFn(item);
        if (!seen.has(key)) {
            seen.add(key);
            yield item;
        }
    }
}

// Helper: wrap a Goal to deduplicate solutions by a key function or a logic variable/ground term
export function uniqueGoalBy(key: ((subst: Subst) => string) | Term, goal: Goal): Goal {
    let keyFn: (subst: Subst) => string;
    if (typeof key === 'function') {
        keyFn = key as (subst: Subst) => string;
    } else if (typeof key === 'object' && key !== null && 'tag' in key && key.tag === 'var') {
        keyFn = (subst: Subst) => String(walk(key, subst));
    } else {
        // ground term: always the same key
        const groundKey = String(key);
        keyFn = () => groundKey;
    }
    return (s: Subst) => uniqueBy(goal(s), keyFn);
}

// Helper: wrap a rule so it always deduplicates by the first argument (logic var or ground term)
export function RelUnique(
    rule: (x: Term, ...rest: any[]) => Goal
): (x: Term, ...rest: any[]) => Goal {
    return (x: Term, ...rest: any[]) =>
        uniqueGoalBy(x, rule(x, ...rest));
}

// Helper to type user-defined rules with argument inference
export function Rel(fn: (...args: any[]) => Goal): (...args: any[]) => Goal {
    return fn;
}

function freshGoal(args: Term[], freshCount: number, fn: (...args: any[]) => Goal): Goal {
    return fresh(
        (...freshVars: Term[]) => fn(...args, freshVars)
    );
}

// Utility to add fluent methods to a generator
function withFluentGen<T>(gen: Generator<T>) {
    type FluentGen<R> = Generator<R> & {
        toArray(): R[];
        forEach(cb: (item: R) => void): void;
        map<U>(cb: (item: R) => U): FluentGen<U>;
        groupBy<K, V>(keyFn: (item: R) => K, valueFn: (item: R) => V): FluentGen<[K, V[]]>;
    };

    function attachFluent<R>(g: Generator<R>): FluentGen<R> {
        const fluent = g as FluentGen<R>;

        fluent.toArray = () => Array.from(g);

        fluent.forEach = (cb: (item: R) => void): void => {
            for (const item of g) {
                cb(item);
            }
        };

        fluent.map = <U>(cb: (item: R) => U): FluentGen<U> => {
            function* mapped(gen: Generator<R>) {
                for (const item of gen) {
                    yield cb(item);
                }
            }
            return attachFluent<U>(mapped(g));
        };

        fluent.groupBy = <K, V>(keyFn: (item: R) => K, valueFn: (item: R) => V): FluentGen<[K, V[]]> => {
            function* groupGen(gen: Generator<R>): Generator<[K, V[]]> {
                const map = new Map<K, V[]>();
                for (const item of gen) {
                    const key = keyFn(item);
                    const value = valueFn(item);
                    if (!map.has(key)) map.set(key, []);
                    map.get(key)!.push(value);
                }
                for (const entry of map.entries()) {
                    yield entry;
                }
            }
            return attachFluent<[K, V[]]>(groupGen(g));
        };

        return fluent;
    }

    return attachFluent<T>(gen);
}

// filterRel: create a relation from a predicate that takes any number of arguments
export function filterRel(pred: (...args: any[]) => boolean): (...args: Term[]) => Goal {
    return (...args: Term[]) =>
        function* (s: Subst) {
            const vals = args.map(arg => walk(arg, s));
            if (pred(...vals)) yield s;
            // If any are unbound, do not attempt to enumerate all possibilities
        };
}


export type TermedArgs<T extends (...args: any) => any> =
    T extends (...args: infer A) => any
    ? (...args: [...{ [I in keyof A]: (Term<A[I]> | A[I]) }, Term<any>]) => Goal
    : never;

export function mapRel<F extends (...args: any) => any>(
    fn: F
): (...args: any[]) => Goal {
    return function (...args: Term<any>[]) {
        return function* (s: Subst) {
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

// Example: add relation with correct type hinting
function addFn(a: number, b: number): number { return a + b; }
export const add: TermedArgs<typeof addFn> = mapRel(addFn);

// Example: mult relation with correct type hinting
function multFn(a: number, b: number): number { return a * b; }
export const mult: TermedArgs<typeof multFn> = mapRel(multFn);

// gt and lt using filterRel 
export const gt = filterRel((u: number, v: number) => u > v);
export const lt = filterRel((u: number, v: number) => u < v);

// Refactor arrayLength to use mapRel
export const arrayLength = Rel((arr, len) =>
    function* (s) {
        const arrVal = walk(arr, s);
        const lenVal = walk(len, s);
        if (Array.isArray(arrVal)) {
            const s2 = unify(len, arrVal.length, s);
            if (s2) yield s2;
        } else if (typeof lenVal === 'number' && isVar(arrVal)) {
            const freshArr = Array.from({ length: lenVal }, () => lvar());
            const s2 = unify(arr, freshArr, s);
            if (s2) yield s2;
        }
    }
);

// Helper to call aggregateVarMulti with goal function arguments
export function aggregateRel(goalFn: (...args: Var[]) => Goal): (...args: Var[]) => Goal {
    return (...args: Var[]) => {
        if (args.length < 2) throw new Error("aggregateRel requires at least two arguments");
        return aggregateVarMulti(
            [args[0]],
            args.slice(1),
            goalFn(...args),
        );
    };
}

// ife: if goal_if succeeds, run goal_then for the first solution; else run goal_else. (miniKanren's ife, not ifte)
export function ife(goal_if: Goal, goal_else: Goal): Goal {
    return function* (s: Subst) {
        let any = false;
        for (const s1 of goal_if(s)) {
            any = true;
            yield s1;
        }
        if (!any) {
            yield* goal_else(s);
        }
    };
}

// membero: true if x is a member of arr (like miniKanren's membero)
export function membero(x: Term, arr: Term): Goal {
    return function* (s: Subst) {
        const arrVal = walk(arr, s);
        if (Array.isArray(arrVal)) {
            for (const item of arrVal) {
                const s2 = unify(x, item, s);
                if (s2) yield s2;
            }
        }
    };
}

// ifte: if goal_if succeeds, run goal_then for each solution; else run goal_else. This matches the miniKanren ifte semantics.
export function ifte(goal_if: Goal, goal_then: Goal, goal_else: Goal): Goal {
    return function* (s: Subst) {
        let any = false;
        for (const s1 of goal_if(s)) {
            any = true;
            yield* goal_then(s1);
        }
        if (!any) {
            yield* goal_else(s);
        }
    };
}