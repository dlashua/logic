// Run handlers for MiniKanren-style logic programming
import { Term, Subst, Var, walk, isVar, lvar, logicListToArray } from './core.ts';
import { Goal } from './relation.ts';

/**
 * The output type for formatted substitutions.
 */
export type RunResult<Fmt extends Record<string, Term<any>>> = { [K in keyof Fmt]: Term };

/**
 * Formats substitutions into user-facing objects, converting logic lists to arrays.
 */
export async function* formatSubstitutions<Fmt extends Record<string, Term<any>>>(
    substs: AsyncGenerator<Subst>,
    formatter: Fmt,
    n: number
): AsyncGenerator<RunResult<Fmt>> {
    let count = 0;
    for await (const s of substs) {
        if (count++ >= n) break;
        const out: Partial<RunResult<Fmt>> = {};
        for (const key in formatter) {
            const v = formatter[key];
            if (v && typeof v === 'object' && 'id' in v) {
                out[key] = await walk(v, s);
            } else {
                out[key] = v;
            }
        }
        yield deepConvertLogicLists(out) as RunResult<Fmt>;
    }
}

/**
 * Create a Proxy for logic variables, with customizable key type.
 */
export function createLogicVarProxy<K extends string | symbol = string>(varMap?: Map<K, Var>): Record<K, Var> {
    if (!varMap) varMap = new Map<K, Var>();
    return new Proxy({}, {
        get(target, prop: K) {
            if (typeof prop !== "string") {
                return undefined;
            }
            if (prop === "_") {
                return lvar();
            }
            if (!varMap!.has(prop)) {
                varMap!.set(prop, lvar(prop));
            }
            return varMap!.get(prop);
        },
        has(target, prop: K) {
            return true;
        },
        ownKeys(target) {
            return Array.from(varMap!.keys());
        },
        getOwnPropertyDescriptor(target, prop: K) {
            return {
                enumerable: true,
                configurable: true
            };
        }
    }) as Record<K, Var>;
}

/**
 * Main run function for logic programs. Expects a function returning [formatter, goal].
 */
export async function* run<Fmt extends Record<string, Term<any>> = Record<string, Term<any>>>(
    f: (...vars: Term<any>[]) => [Fmt, Goal],
    n: number = Infinity
) {
    const s0: Subst = new Map();
    let vars: Term<any>[] = [];
    vars = Array.from({ length: f.length }, () => lvar());
    const result = f(...vars);
    if (!Array.isArray(result) || result.length !== 2 || typeof result[0] !== 'object' || typeof result[1] !== 'function') {
        throw new Error('run expects a function returning [formatter, goal]');
    }
    const formatter = result[0] as Fmt;
    const goal = result[1] as Goal;
    for await (const item of formatSubstitutions(goal(s0), formatter, n)) {
        yield item;
    }
}

/**
 * Main runEasy function for logic programs using a Proxy for logic variables.
 */
export async function* runEasy<Fmt extends Record<string, Term<any>> = Record<string, Term<any>>>(
    f: ($: Record<string, Var>) => [Fmt, Goal],
    n: number = Infinity
) {
    const s0: Subst = new Map();
    const $ = createLogicVarProxy();
    const result = f($);
    if (!Array.isArray(result) || result.length !== 2 || typeof result[0] !== 'object' || typeof result[1] !== 'function') {
        throw new Error('runEasy expects a function returning [formatter, goal]');
    }
    const formatter = result[0] as Fmt;
    const goal = result[1] as Goal;
    for await (const item of formatSubstitutions(goal(s0), formatter, n)) {
        yield item;
    }
}

// --- Helpers ---

/**
 * Returns true if the value is a logic list (cons/nil).
 */
export function isLogicList(val: any): boolean {
    return val && typeof val === 'object' && 'tag' in val && (val.tag === 'cons' || val.tag === 'nil');
}

/**
 * Recursively converts any logic lists in the value to JS arrays.
 */
export function deepConvertLogicLists(val: any): any {
    if (isLogicList(val)) {
        // Recursively convert logic list to array, and also convert elements
        const arr = logicListToArray(val).map(deepConvertLogicLists);
        return arr;
    } else if (Array.isArray(val)) {
        return val.map(deepConvertLogicLists);
    } else if (val && typeof val === 'object' && !isVar(val)) {
        const out: any = {};
        for (const k in val) {
            if (Object.prototype.hasOwnProperty.call(val, k)) {
                out[k] = deepConvertLogicLists(val[k]);
            }
        }
        return out;
    }
    return val;
}

// Utility to add fluent methods to a generator
export function withFluentGen<T>(gen: Generator<T>) {
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
