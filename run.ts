// Run handlers for MiniKanren-style logic programming
import { Term, Subst, Var, walk, isVar, lvar } from './core.ts';
import { Goal } from './relation.ts';

export async function* run<Fmt extends Record<string, Term<any>> = Record<string, Term<any>>>(
    f: (...vars: Term<any>[]) => [Fmt, Goal] | Goal | [Goal],
    n: number = Infinity
) {
    const s0: Subst = new Map();
    let result: [Fmt, Goal] | Goal | [Goal];
    let vars: Term<any>[] = [];
    vars = Array.from({ length: f.length }, () => lvar());
    result = f(...vars);

    async function* substToObjGen(substs: AsyncGenerator<Subst>, formatter: Fmt): AsyncGenerator<{ [K in keyof Fmt]: Term }> {
        let count = 0;
        for await (const s of substs) {
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
    async function* substToObjGenVars(substs: AsyncGenerator<Subst>): AsyncGenerator<{ [K in keyof Fmt]: Term }> {
        let count = 0;
        for await (const s of substs) {
            if (count++ >= n) break;
            const out: Record<string, Term> = {};
            vars.forEach(v => {
                out[(v as any).id] = walk(v, s);
            });
            yield out as { [K in keyof Fmt]: Term };
        }
    }
    let gen: AsyncGenerator<{ [K in keyof Fmt]: Term }>;
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
    for await (const item of gen) {
        yield item;
    }
}

export async function* runEasy<Fmt extends Record<string, Term<any>> = Record<string, Term<any>>>(
    f: ($: Record<string, Var>) => [Fmt, Goal] | Goal | [Goal],
    n: number = Infinity
) {
    const s0: Subst = new Map();
    const varMap = new Map<string, Var>();
    const $ = new Proxy({}, {
        get(target, prop: string) {
            if (prop === "_") {
                return lvar();
            }
            if (!varMap.has(prop)) {
                varMap.set(prop, lvar());
            }
            return varMap.get(prop);
        },
        has(target, prop: string) {
            return true;
        },
        ownKeys(target) {
            return Array.from(varMap.keys());
        },
        getOwnPropertyDescriptor(target, prop: string) {
            return {
                enumerable: true,
                configurable: true
            };
        }
    });
    const result = f($);
    const vars = Array.from(varMap.values());
    async function* substToObjGen(substs: AsyncGenerator<Subst>, formatter: Fmt): AsyncGenerator<{ [K in keyof Fmt]: Term }> {
        let count = 0;
        for await (const s of substs) {
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
    async function* substToObjGenVars(substs: AsyncGenerator<Subst>): AsyncGenerator<{ [K in keyof Fmt]: Term }> {
        let count = 0;
        for await (const s of substs) {
            if (count++ >= n) break;
            const out: Record<string, Term> = {};
            vars.forEach(v => {
                out[(v as any).id] = walk(v, s);
            });
            yield out as { [K in keyof Fmt]: Term };
        }
    }
    let gen: AsyncGenerator<{ [K in keyof Fmt]: Term }>;
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
    for await (const item of gen) {
        yield item;
    }
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
