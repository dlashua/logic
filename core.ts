// Core logic functions for MiniKanren-style logic programming

/**
 * Logic variable representation
 */
export type Var = { tag: 'var', id: string };
let varCounter = 0;

/**
 * Create a new logic variable.
 */
export function lvar(name = ""): Var {
    return { tag: 'var', id: `${name}_${varCounter++}` };
}
/**
 * Reset the logic variable counter (useful for tests/determinism).
 */
export function resetVarCounter() {
    varCounter = 0;
}

/**
 * Term type with generics for better type hinting
 */
export type Term<T = unknown> = Var | T | Term<T>[] | null | undefined;

/**
 * Substitution: mapping from variable id to value
 */
export type Subst = Map<string, Term>;

/**
 * Returns true if the value is a logic variable.
 */
export function isVar(x: Term): x is Var {
    return typeof x === 'object' && x !== null && 'tag' in x && (x as Var).tag === 'var';
}

/**
 * Walk: find the value a variable is bound to, recursively.
 */
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

/**
 * Unification: attempts to unify two terms under a substitution.
 */
export function unify(u: Term, v: Term, s: Subst): Subst | null {
    u = walk(u, s);
    v = walk(v, s);
    if (isVar(u)) {
        return extendSubst(u, v, s);
    } else if (isVar(v)) {
        return extendSubst(v, u, s);
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

/**
 * Extends a substitution with a new variable binding, with occurs check.
 */
export function extendSubst(v: Var, val: Term, s: Subst): Subst | null {
    if (occursCheck(v, val, s)) return null;
    const s2 = new Map(s);
    s2.set(v.id, val);
    return s2;
}

// --- Logic List Utilities ---

/**
 * A cons cell node for logic lists.
 */
export type ConsNode = { tag: 'cons', head: Term, tail: Term };
/**
 * A nil node for logic lists.
 */
export type NilNode = { tag: 'nil' };
/**
 * Logic list canonical representation.
 */
export type LogicList = ConsNode | NilNode;

/**
 * The canonical nil value for logic lists.
 */
export const nil: NilNode = { tag: 'nil' };
/**
 * Create a cons cell for a logic list.
 */
export function cons(head: Term, tail: Term): ConsNode {
    return { tag: 'cons', head, tail };
}
/**
 * Convert a JS array to a logic list.
 */
export function arrayToLogicList(arr: Term[]): LogicList {
    return arr.reduceRight<LogicList>((tail, head) => cons(head, tail), nil);
}
/**
 * Convert a logic list to a JS array (grounded).
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
 * logicList(...items): Shorthand for cons(1, cons(2, ... nil))
 */
export function logicList<T = unknown>(...items: T[]): Term {
    let list: Term = nil;
    for (let i = items.length - 1; i >= 0; --i) {
        list = cons(items[i], list);
    }
    return list;
}

// --- Helpers ---

/**
 * Returns true if variable v occurs anywhere in x (occurs check for unification).
 */
export function occursCheck(v: Var, x: Term, s: Subst): boolean {
    x = walk(x, s);
    if (isVar(x)) return v.id === x.id;
    if (Array.isArray(x)) return x.some(e => occursCheck(v, e, s));
    return false;
}

/**
 * Returns true if the value is a cons cell (logic list node).
 */
export function isCons(x: any): x is { tag: 'cons', head: Term, tail: Term } {
    return x && typeof x === 'object' && 'tag' in x && x.tag === 'cons';
}

/**
 * Returns true if the value is a nil node (logic list end).
 */
export function isNil(x: any): x is { tag: 'nil' } {
    return x && typeof x === 'object' && 'tag' in x && x.tag === 'nil';
}
