// -----------------------------------------------------------------------------
//
//                          Refactored Logic Engine
//
// -----------------------------------------------------------------------------
// This file contains a complete, refactored implementation of a MiniKanren-style
// logic programming engine in TypeScript. The original four files (core.ts,
// relations.ts, run.ts, query-builder.ts) have been consolidated and improved
// for clarity, performance, and developer experience.
//
// Key Improvements:
// - **Unified Structure:** All core components are now in a single file.
// - **List Relations:** Added full support for `cons`/`nil` logic lists and
//   the relations that operate on them (membero, appendo, etc.).
// - **Enhanced Readability:** Added extensive TSDoc comments and consistent style.
// - **Improved Type Safety:** Strengthened type definitions throughout.
// - **Simplified API:** The `query` builder is the primary, streamlined way
//   to interact with the engine.
// -----------------------------------------------------------------------------

// Section 1: Core Data Structures and Types
// -----------------------------------------------------------------------------

/**
 * Represents a logic variable, a placeholder for a value.
 */
export interface Var {
  readonly tag: "var";
  readonly id: string;
}

/**
 * A `cons` cell, the building block of a logic list.
 */
export interface ConsNode {
    readonly tag: "cons";
    readonly head: Term;
    readonly tail: Term;
    readonly id?: string;
}

/**
 * The end of a logic list.
 */
export interface NilNode {
    readonly tag: "nil";
}

/**
 * A logic list is either a `cons` cell or `nil`.
 */
export type LogicList = ConsNode | NilNode;

/**
 * A substitution map, holding variable bindings.
 */
export type Subst = Map<string | symbol, Term>;

/**
 * Represents any term in the logic system.
 */
export type Term<T = unknown> = Var | LogicList | T | Term<T>[] | null | undefined;

/**
 * A Goal is a function that takes a substitution and returns a stream of
 * possible resulting substitutions.
 */
export type Goal = (s: Subst) => AsyncGenerator<Subst>;


// Section 2: Core Engine Functions & List Utilities
// -----------------------------------------------------------------------------

let varCounter = 0;

/**
 * Creates a new, unique logic variable.
 * @param name An optional prefix for debugging.
 */
export function lvar(name = ""): Var {
  return {
    tag: "var",
    id: `${name}_${varCounter++}`,
  };
}

/**
 * Resets the global variable counter for deterministic tests.
 */
export function resetVarCounter(): void {
  varCounter = 0;
}

/**
 * The canonical `nil` value, representing an empty logic list.
 */
export const nil: NilNode = {
  tag: "nil" 
};

/**
 * Creates a `cons` cell (a node in a logic list).
 * @param head The value of the node.
 * @param tail The rest of the list.
 */
export function cons(head: Term, tail: Term): ConsNode {
  return {
    tag: "cons",
    head,
    tail 
  };
}

/**
 * Converts a JavaScript array into a logic list.
 * @param arr The array to convert.
 * @returns A logic list (`cons` cells ending in `nil`).
 */
export function arrayToLogicList(arr: Term[]): LogicList {
  return arr.reduceRight<LogicList>((tail, head) => cons(head, tail), nil);
}

/**
 * A convenience function to create a logic list from arguments.
 * @param items The items to include in the list.
 * @example logicList(1, 2, 3) // equivalent to cons(1, cons(2, cons(3, nil)))
 */
export function logicList<T = unknown>(...items: T[]): LogicList {
  return arrayToLogicList(items);
}

/**
 * Type guard to check if a term is a logic variable.
 */
export function isVar(x: Term): x is Var {
  return typeof x === "object" && x !== null && (x as Var).tag === "var";
}

/**
 * Type guard to check if a term is a `cons` cell.
 */
export function isCons(x: Term): x is ConsNode {
  return typeof x === "object" && x !== null && (x as ConsNode).tag === "cons";
}

/**
 * Type guard to check if a term is `nil`.
 */
export function isNil(x: Term): x is NilNode {
  return typeof x === "object" && x !== null && (x as NilNode).tag === "nil";
}

/**
 * Type guard to check if a term is a logic list.
 */
export function isLogicList(x: Term): x is LogicList {
  return isCons(x) || isNil(x);
}


/**
 * Recursively finds the ultimate binding of a term in a given substitution.
 * Optimized to use iteration for variable chains and avoid deep recursion.
 * @param u The term to resolve.
 * @param s The substitution map.
 */
export async function walk(u: Term, s: Subst): Promise<Term> {
  let current = u;
  
  // Fast path for variable chains - use iteration instead of recursion
  while (isVar(current) && s.has(current.id)) {
    current = s.get(current.id)!;
  }
  
  // If we ended up with a non-variable, check if it needs structural walking
  if (isCons(current)) {
    // Walk both parts of the cons cell
    return cons(await walk(current.head, s), await walk(current.tail, s));
  }
  
  if (Array.isArray(current)) {
    return Promise.all(current.map((x) => walk(x, s)));
  }
  
  if (current && typeof current === "object" && !isVar(current) && !isLogicList(current)) {
    const out: Record<string, Term> = {};
    for (const k in current) {
      if (Object.hasOwn(current, k)) {
        out[k] = await walk((current as any)[k], s);
      }
    }
    return out;
  }
  
  return current;
}

/**
 * Extends a substitution by binding a variable to a value, with an occurs check.
 */
export async function extendSubst(v: Var, val: Term, s: Subst): Promise<Subst | null> {
  if (await occursCheck(v, val, s)) {
    return null; // Occurs check failed
  }
  const s2 = new Map(s);
  s2.set(v.id, val);
  return s2;
}

/**
 * Checks if a variable `v` occurs within a term `x` to prevent infinite loops.
 */
async function occursCheck(v: Var, x: Term, s: Subst): Promise<boolean> {
  const resolvedX = await walk(x, s);
  if (isVar(resolvedX)) {
    return v.id === resolvedX.id;
  }
  if (isCons(resolvedX)) {
    return await occursCheck(v, resolvedX.head, s) || await occursCheck(v, resolvedX.tail, s);
  }
  if (Array.isArray(resolvedX)) {
    for (const item of resolvedX) {
      if (await occursCheck(v, item, s)) {
        return true;
      }
    }
  }
  return false;
}

/**
 * The core unification algorithm. It attempts to make two terms structurally equivalent.
 * Optimized with fast paths for common cases.
 */
export async function unify(u: Term, v: Term, s: Subst | null): Promise<Subst | null> {
  if (s === null) {
    return null;
  }

  // Fast path: if both terms are identical primitives, no walking needed
  if (u === v) {
    return s;
  }

  const uWalked = await walk(u, s);
  const vWalked = await walk(v, s);

  // Fast path: after walking, if they're still identical, succeed
  if (uWalked === vWalked) {
    return s;
  }

  if (isVar(uWalked)) return extendSubst(uWalked, vWalked, s);
  if (isVar(vWalked)) return extendSubst(vWalked, uWalked, s);

  // Fast paths for primitive types
  if (typeof uWalked === 'number' && typeof vWalked === 'number') {
    return uWalked === vWalked ? s : null;
  }
  
  if (typeof uWalked === 'string' && typeof vWalked === 'string') {
    return uWalked === vWalked ? s : null;
  }

  if (isNil(uWalked) && isNil(vWalked)) return s;
  if (isCons(uWalked) && isCons(vWalked)) {
    const s1 = await unify(uWalked.head, vWalked.head, s);
    if (s1 === null) return null;
    return unify(uWalked.tail, vWalked.tail, s1);
  }

  if (
    Array.isArray(uWalked) &&
    Array.isArray(vWalked) &&
    uWalked.length === vWalked.length
  ) {
    let currentSubst: Subst | null = s;
    for (let i = 0; i < uWalked.length; i++) {
      currentSubst = await unify(uWalked[i], vWalked[i], currentSubst);
      if (currentSubst === null) return null;
    }
    return currentSubst;
  }

  if (JSON.stringify(uWalked) === JSON.stringify(vWalked)) {
    return s;
  }

  return null;
}

// Section 3: Relational Operators (Goals)
// -----------------------------------------------------------------------------

/**
 * A goal that succeeds if two terms can be unified.
 */
export function eq(u: Term, v: Term): Goal {
  return async function* eqGoal(s: Subst | null) {
    const s_unified = await unify(u, v, s);
    if (s_unified !== null) {
      yield s_unified;
    }
  };
}

/**
 * Introduces new (fresh) logic variables into a sub-goal.
 */
export function fresh(f: (...vars: Var[]) => Goal): Goal {
  return async function* freshGoal(s: Subst) {
    const freshVars = Array.from({
      length: f.length 
    }, () => lvar());
    const subGoal = f(...freshVars);
    yield* subGoal(s);
  };
}

/**
 * Logical disjunction (OR).
 */
export function disj(g1: Goal, g2: Goal): Goal {
  return async function* disjGoal(s: Subst) {
    yield* g1(s);
    yield* g2(s);
  };
}

/**
 * Logical conjunction (AND).
 */
export function conj(g1: Goal, g2: Goal): Goal {
  return async function* conjGoal(s: Subst) {
    for await (const s1 of g1(s)) {
      yield* g2(s1);
    }
  };
}

/**
 * Helper for combining multiple goals with logical AND.
 */
export const and = (...goals: Goal[]): Goal => {
  if (goals.length === 0) return (s) => (async function*() { yield s; })();
  return goals.reduce(conj);
};

/**
 * Helper for combining multiple goals with logical OR.
 */
export const or = (...goals: Goal[]): Goal => {
  if (goals.length === 0) return (s) => (async function*() { /* pass */ })();
  return goals.reduce(disj);
};

/**
 * Multi-clause disjunction (OR).
 */
export function conde(...clauses: Goal[][]): Goal {
  const clauseGoals = clauses.map(clause => and(...clause));
  return or(...clauseGoals);
}


// Section 4: Query Execution and Result Formatting
// -----------------------------------------------------------------------------

/**
 * The shape of a single result from a query.
 */
export type RunResult<Fmt> = {
  [K in keyof Fmt]: Term;
};

/**
 * Converts a logic list to a JavaScript array.
 * @param list The logic list to convert.
 */
export function logicListToArray(list: Term): Term[] {
  const out = [];
  let cur = list;
  while (
    cur &&
    typeof cur === "object" &&
    "tag" in cur &&
    (cur as any).tag === "cons"
  ) {
    out.push((cur as any).head);
    cur = (cur as any).tail;
  }
  return out;
}

/**
 * Recursively walks a result object, converting any logic lists into JS arrays.
 */
function deepListWalk(val: any): any {
  if (isLogicList(val)) {
    return logicListToArray(val).map(deepListWalk);
  } else if (Array.isArray(val)) {
    return val.map(deepListWalk);
  } else if (val && typeof val === "object" && !isVar(val)) {
    const out: any = {};
    for (const k in val) {
      if (Object.hasOwn(val, k)) {
        out[k] = deepListWalk(val[k]);
      }
    }
    return out;
  }
  return val;
}


/**
 * Creates a proxy object that automatically creates logic variables on access.
 */
export function createLogicVarProxy<K extends string | symbol = string>(
  prefix = ""
): { proxy: Record<K, Var>; varMap: Map<K, Var> } {
  const varMap = new Map<K, Var>();
  const proxy = new Proxy({} as Record<K, Var>, {
    get(target, prop: K) {
      if (typeof prop !== "string") return undefined;
      if (prop === "_") return lvar();
      if (!varMap.has(prop)) {
        varMap.set(prop, lvar(`${prefix}${String(prop)}`));
      }
      return varMap.get(prop)!;
    },
    has: () => true,
    ownKeys: () => Array.from(varMap.keys()),
    getOwnPropertyDescriptor: () => ({
      enumerable: true,
      configurable: true 
    }),
  });
  return {
    proxy,
    varMap 
  };
}

/**
 * Formats the raw substitution streams into user-friendly result objects.
 */
async function* formatSubstitutions<Fmt>(
  substs: AsyncGenerator<Subst | null>,
  formatter: Fmt,
  limit: number
): AsyncGenerator<RunResult<Fmt>> {
  let count = 0;
  for await (const s of substs) {
    if (s === null) {
      continue;
    }
    if (count++ >= limit) break;
    const result: Partial<RunResult<Fmt>> = {};
    for (const key in formatter) {
      const term = formatter[key];
      result[key] = await walk(term, s);
    }
    // Convert logic lists to arrays before yielding the final result
    yield deepListWalk(result) as RunResult<Fmt>;
  }
}

// Section 5: The Fluent Query Builder
// -----------------------------------------------------------------------------


type QueryOutput<Fmt, Sel> = Sel extends ($: Record<string, Var>) => Fmt
  ? RunResult<Fmt>
  : Sel extends "*"
    ? RunResult<Record<string, any>>
    : any;

/**
 * A fluent interface for building and executing logic queries.
 */
class Query<Fmt = Record<string, Var>, Sel = "*"> {
  private _formatter: Fmt | Record<string, Var> | null = null;
  private _rawSelector: any = null;
  private _goals: Goal[] = [];
  private _limit = Infinity;
  private readonly _logicVarProxy: Record<string, Var>;
  private _selectAllVars = false;

  constructor() {
    const { proxy } = createLogicVarProxy("q_");
    this._logicVarProxy = proxy;
    this._selectAllVars = true;
  }

  /**
   * Specifies the shape of the desired output.
   */
  select<NewSel extends "*">(selector: NewSel): Query<Record<string, Var>, NewSel>;
  select<NewSel extends ($: Record<string, Var>) => any>(selector: NewSel): Query<ReturnType<NewSel>, NewSel>;
  select<NewSel extends any>(selector: NewSel): Query<any, any> {
    if (selector === "*") {
      this._formatter = null;
      this._rawSelector = null;
      this._selectAllVars = true;
    } else if (typeof selector === "function") {
      this._rawSelector = null;
      this._selectAllVars = false;
      this._formatter = selector(this._logicVarProxy);
    } else {
      this._formatter = null;
      this._selectAllVars = false;
      this._rawSelector = selector;
    }
    return this;
  }

  /**
   * Adds constraints (goals) to the query.
   */
  where(goalFn: (proxy: Record<string, Var>) => Goal | Goal[]): this {
    const result = goalFn(this._logicVarProxy);
    this._goals.push(...(Array.isArray(result) ? result : [result]));
    return this;
  }

  /**
   * Sets the maximum number of results.
   */
  limit(n: number): this {
    this._limit = n;
    return this;
  }

  private async *runQuery(): AsyncGenerator<any> {
    if (this._goals.length === 0) {
      throw new Error("Query must have at least one .where() clause.");
    }

    let formatter: Fmt | Record<string, Var> | any = this._formatter;
    if (this._selectAllVars) {
      formatter = {
        ...this._logicVarProxy 
      };
    } else if (this._rawSelector) {
      formatter = {
        result: this._rawSelector 
      };
    } else if (!formatter) {
      formatter = {
        ...this._logicVarProxy 
      };
    }

    const initialSubst: Subst = new Map();
    const combinedGoal = and(...this._goals);
    const substStream = combinedGoal(initialSubst);
    const results = formatSubstitutions(substStream, formatter, this._limit);

    for await (const result of results) {
      if (this._rawSelector) {
        yield result.result;
      } else {
        yield result;
      }
    }
  }

  /**
   * Makes the Query object itself an async iterable.
   */
  [Symbol.asyncIterator](): AsyncGenerator<QueryOutput<Fmt, Sel>> {
    return this.runQuery();
  }

  /**
   * Executes the query and returns all results as an array.
   */
  async toArray(): Promise<QueryOutput<Fmt, Sel>[]> {
    const results: QueryOutput<Fmt, Sel>[] = [];
    for await (const result of this.runQuery()) {
      results.push(result);
    }
    return results;
  }
}

/**
 * The main entry point for creating a new logic query.
 */
export function query<Fmt>(): Query<Fmt> {
  return new Query<Fmt>();
}

// Section 6: List Relations
// -----------------------------------------------------------------------------

/**
 * A goal that succeeds if `x` is a member of the logic `list`.
 * Optimized for both arrays and logic lists.
 */
export function membero(x: Term, list: Term): Goal {
  return async function* (s) {
    const l = await walk(list, s);
    
    // Fast path for arrays
    if (Array.isArray(l)) {
      for (const item of l) {
        const s2 = await unify(x, item, s);
        if (s2) yield s2;
      }
      return;
    }
    
    // Logic list traversal with iterative approach when possible
    if (l && typeof l === "object" && "tag" in l) {
      if ((l as any).tag === "cons") {
        const s1 = await unify(x, (l as any).head, s);
        if (s1) yield s1;
        // Recursive call for tail
        for await (const s2 of membero(x, (l as any).tail)(s)) yield s2;
      }
    }
  };
}

/**
 * A goal that succeeds if `h` is the head of the logic list `l`.
 */
export function firsto(x: Term, xs: Term): Goal {
  return async function* (s) {
    const l = await walk(xs, s);
    if (isCons(l)) {
      const consNode = l as { tag: "cons"; head: Term; tail: Term };
      const s1 = await unify(x, consNode.head, s);
      if (s1) yield s1;
    }
  };
}

/**
 * A goal that succeeds if `t` is the tail of the logic list `l`.
 */
export function resto(xs: Term, tail: Term): Goal {
  return async function* (s) {
    const l = await walk(xs, s);
    if (isCons(l)) {
      const consNode = l as { tag: "cons"; head: Term; tail: Term };
      const s1 = await unify(tail, consNode.tail, s);
      if (s1) yield s1;
    }
  };
}

/**
 * A goal that succeeds if logic list `zs` is the result of appending
 * logic list `ys` to `xs`.
 */
export function appendo(xs: Term, ys: Term, zs: Term): Goal {
  return async function* (s) {
    const xsVal = await walk(xs, s);
    if (isCons(xsVal)) {
      const consNode = xsVal as { tag: "cons"; head: Term; tail: Term };
      const head = consNode.head;
      const tail = consNode.tail;
      const rest = lvar();
      const s1 = await unify(
        zs,
        {
          tag: "cons",
          head,
          tail: rest,
        },
        s,
      );
      if (s1) {
        for await (const s2 of appendo(tail, ys, rest)(s1)) yield s2;
      }
    } else if (isNil(xsVal)) {
      const s1 = await unify(ys, zs, s);
      if (s1) yield s1;
    }
  };
}

/** Forgotten Items **/

export function Rel<F extends (...args: any) => any>(
  fn: F,
): (...args: Parameters<F>) => Goal {
  return (...args: Parameters<F>) => {
    const goal = async function* relGoal(s: Subst) {
      // Walk all arguments with the current substitution
      const walkedArgs = await Promise.all(args.map(arg => walk(arg, s)));
      // Call the underlying relation function with grounded arguments
      const subgoal = fn(...walkedArgs);
      for await (const s1 of subgoal(s)) {
        yield s1;
      }
    };
    // Always set a custom property for the logical name
    if (typeof goal === "function" && fn.name) {
      (goal as any).__logicName = fn.name;
    }
    return goal;
  };
}

export const distincto_G = Rel((t: Term, g: Goal) => 
  async function* distincto_G(s: Subst) {
    const seen = new Set();
    for await (const s2 of g(s)) {
      const w_t = await walk(t, s2);
      if (isVar(w_t)) {
        yield s2;
        continue;
      }
      const key = JSON.stringify(w_t);
      if (seen.has(key)) continue;
      seen.add(key);
      yield s2;
    }
  }
);

export function not(goal: Goal): Goal {
  const g = async function* not(s: Subst) {
    let found = false;
    for await (const _subst of goal(s)) {
      found = true;
      break;
    }
    if (!found) yield s;
  };
  return g;
}

export const neq_C = Rel((x: Term, y: Term) => not(eq(x, y)));

/**
 * A goal that succeeds if the numeric value in the first term is greater than
 * the numeric value in the second term.
 */
export function gtc(x: Term, y: Term): Goal {
  return async function* gtcGoal(s: Subst) {
    const xWalked = await walk(x, s);
    const yWalked = await walk(y, s);
    
    // Both must be grounded to numeric values
    if (typeof xWalked === 'number' && typeof yWalked === 'number') {
      if (xWalked > yWalked) {
        yield s;
      }
    }
    // If either is ungrounded, this constraint cannot be satisfied
  };
}



export type TermedArgs<T extends (...args: any) => any> = T extends (
  ...args: infer A
) => infer R
  ? (...args: [...{ [I in keyof A]: Term<A[I]> | A[I] }, out: Term<R>]) => Goal
  : never;