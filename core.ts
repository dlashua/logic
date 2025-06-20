// Core logic functions for MiniKanren-style logic programming

// Logic variable representation
export type Var = { tag: 'var', id: number };
let varCounter = 0;
export function lvar(): Var {
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
    // Handle logic lists
    if (u && typeof u === 'object' && 'tag' in u) {
        if ((u as any).tag === 'cons') {
            return cons(walk((u as any).head, s), walk((u as any).tail, s));
        }
        if ((u as any).tag === 'nil') {
            return nil;
        }
    }
    if (Array.isArray(u)) {
        return u.map(x => walk(x, s));
    }
    if (u && typeof u === 'object' && !isVar(u)) {
        // Recursively walk object properties (but not null)
        const out: Record<string, Term> = {};
        for (const k in u) {
            if (Object.prototype.hasOwnProperty.call(u, k)) {
                out[k] = walk((u as any)[k], s);
            }
        }
        return out;
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
    } else if (u && typeof u === 'object' && v && typeof v === 'object' && 'tag' in u && 'tag' in v) {
        // Logic list unification
        if ((u as any).tag === 'cons' && (v as any).tag === 'cons') {
            const s1 = unify((u as any).head, (v as any).head, s);
            if (!s1) return null;
            return unify((u as any).tail, (v as any).tail, s1);
        }
        if ((u as any).tag === 'nil' && (v as any).tag === 'nil') {
            return s;
        }
        return null;
    } else if (u === v) {
        return s;
    } else {
        return null;
    }
}

export function extS(v: Var, val: Term, s: Subst): Subst | null {
    if (occursCheck(v, val, s)) return null;
    const s2 = new Map(s);
    s2.set(v.id, val);
    return s2;
}

export function occursCheck(v: Var, x: Term, s: Subst): boolean {
    x = walk(x, s);
    if (isVar(x)) return v.id === x.id;
    if (Array.isArray(x)) return x.some(e => occursCheck(v, e, s));
    return false;
}

// Logic list canonical representation
export type LogicList = { tag: 'cons', head: Term, tail: Term } | { tag: 'nil' };
export const nil: LogicList = { tag: 'nil' };
export function cons(head: Term, tail: Term): LogicList {
    return { tag: 'cons', head, tail };
}
// Convert JS array to logic list
export function arrayToLogicList(arr: Term[]): LogicList {
    return arr.reduceRight<LogicList>((tail, head) => cons(head, tail), nil);
}
// Convert logic list to JS array (grounded)
export function logicListToArray(list: Term): Term[] {
    const out = [];
    let cur = list;
    while (cur && typeof cur === 'object' && 'tag' in cur && (cur as any).tag === 'cons') {
        out.push((cur as any).head);
        cur = (cur as any).tail;
    }
    return out;
}
