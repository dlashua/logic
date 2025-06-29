import { run } from "node:test";
import {
  Goal,
  and,
  enableLogicProfiling,
  disableLogicProfiling,
  printLogicProfileRecap
} from "./relations.ts"
import {
  Subst,
  Term,
  Var,
  CTX_SYM,
  EOSseen,
  EOSsent
} from "./core.ts"
import { createLogicVarProxy, formatSubstitutions, withFluentAsyncGen, RunResult } from "./run.ts"

type FluentAsyncGen<R> = AsyncGenerator<R> & {
  toArray(): Promise<R[]>;
  forEach(cb: (item: R) => Promise<void> | void): Promise<void>;
  map<U>(cb: (item: R) => Promise<U> | U): FluentAsyncGen<U>;
  groupBy<K, V>(
    keyFn: (item: R) => K,
    valueFn: (item: R) => V,
  ): FluentAsyncGen<[K, V[]]>;
};

// Map to assign unique display names to anonymous relations
const relationDisplayNames = new WeakMap<Goal, string>();
let relationIdCounter = 1;

function getRelationDisplayName(goal: Goal): string {
  if (goal.name && goal.name !== "anonymous") return goal.name;
  if (!relationDisplayNames.has(goal)) {
    relationDisplayNames.set(goal, `anonymous_${relationIdCounter++}`);
  }
  return relationDisplayNames.get(goal)!;
}

type SelectorType<Fmt> = (($: Record<string, Var>) => Fmt) | "*" | any;

type QueryOutput<Fmt, Sel> = Sel extends ($: Record<string, Var>) => Fmt
  ? RunResult<Fmt>
  : Sel extends "*"
    ? RunResult<Record<string, any>>
    : any;

/**
 * A builder for creating and running logic queries.
 */
class Query<Fmt, Sel = ($: Record<string, Var>) => Fmt> {
  private _formatter: Fmt | Record<string, Var> | null = null;
  private _rawSelector: any = null;
  private _goals: Goal[] = [];
  private _limit = Infinity;
  private readonly _logicVarProxy: Record<string, Var>;
  private _profiling = false;
  private _profileLogs: { relation: string; time: number }[] = [];
  private _profileData: Map<Goal, { count: number; totalTime: number }> = new Map<Goal, { count: number; totalTime: number }>();
  private _relationCounter = 0;
  private _selectAllVars = false;

  constructor() {
    const { proxy } = createLogicVarProxy("q");
    this._logicVarProxy = proxy;
  }

  /**
   * Specifies the shape of the desired output.
   * @param selector A function, "*", or any value (string, number, Var, object, array, etc.) referencing logic vars.
   */
  select<NewSel extends SelectorType<Fmt>>(selector: NewSel): Query<Fmt, NewSel> {
    if (selector === "*") {
      this._selectAllVars = true;
      this._formatter = null;
      this._rawSelector = null;
    } else if (typeof selector === "function") {
      this._formatter = selector(this._logicVarProxy);
      this._selectAllVars = false;
      this._rawSelector = null;
    } else {
      this._formatter = null;
      this._selectAllVars = false;
      this._rawSelector = selector;
    }
    return this as unknown as Query<Fmt, NewSel>;
  }

  /**
   * Adds conditions to the query. Can be called multiple times.
   * Only accepts a function that receives the logic var proxy and returns a goal or array of goals.
   * @param goalFn A function that receives a logic variable proxy and returns a goal or an array of goals.
   */
  where(goalFn: (proxy: Record<string, Var>) => Goal | Goal[]): this {
    const result = goalFn(this._logicVarProxy);
    const newGoals = Array.isArray(result) ? result : [result];
    if (this._profiling) {
      this._goals.push(...newGoals.map(g => wrapRelationForProfiling(g, this._profileData)));
    } else {
      this._goals.push(...newGoals);
    }
    return this;
  }

  /**
   * Sets the maximum number of results to return.
   * @param n The maximum number of results.
   */
  limit(n: number): this {
    this._limit = n;
    return this;
  }

  /**
   * Enables profiling to trace relations and their execution time.
   */
  public enableProfiling(): this {
    this._profiling = true;
    enableLogicProfiling();
    return this;
  }
  public disableProfiling(): this {
    this._profiling = false;
    disableLogicProfiling();
    return this;
  }
  public printProfileRecap(): void {
    if (this._profiling) {
      printLogicProfileRecap();
    }
  }

  private _getGoal(): Goal {
    if (this._goals.length === 0) {
      throw new Error("Query must have at least one where clause.");
    }
    // Validate all are functions (Goals)
    for (const g of this._goals) {
      if (typeof g !== 'function') {
        throw new Error(`Invalid goal in query: ${g}. All goals must be functions.`);
      }
    }
    return this._goals.length > 1 ? and(...this._goals) : this._goals[0];
  }

  private async *runQuery(): AsyncGenerator<any> {
    let formatter: Fmt | Record<string, Var> | any = this._formatter;
    if (!formatter && (this._selectAllVars || (!this._rawSelector && !this._selectAllVars))) {
      formatter = Object.fromEntries(
        Object.keys(this._logicVarProxy).map(k => [k, this._logicVarProxy[k]])
      ) as Record<string, Var>;
    } else if (this._rawSelector) {
      formatter = this._rawSelector;
    }
    const goal = this._getGoal();

    const Ctx = {
      mode: "collect",
      patterns: []
    };

    Ctx.mode = "run";
    const s0Run: Subst = new Map();
    s0Run.set(CTX_SYM, Ctx);
    const gen = formatSubstitutions(goal(s0Run), formatter, this._limit);
    for await (const result of gen) {
      yield result;
    }
    EOSsent("runQuery");
    yield* goal(null);
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
  public toArray(): Promise<QueryOutput<Fmt, Sel>[]> {
    return withFluentAsyncGen(this.runQuery()).toArray();
  }

  /**
   * Executes the query and calls a function for each result.
   */
  public forEach(
    cb: (item: QueryOutput<Fmt, Sel>) => Promise<void> | void,
  ): Promise<void> {
    return withFluentAsyncGen(this.runQuery()).forEach(cb);
  }

  /**
   * Maps each result of the query to a new value.
   */
  public map<U>(
    cb: (item: QueryOutput<Fmt, Sel>) => Promise<U> | U,
  ): FluentAsyncGen<U> {
    return withFluentAsyncGen(this.runQuery()).map(cb);
  }

  /**
   * Groups the results of the query by a key.
   */
  public groupBy<K, V>(
    keyFn: (item: QueryOutput<Fmt, Sel>) => K,
    valueFn: (item: QueryOutput<Fmt, Sel>) => V,
  ): FluentAsyncGen<[K, V[]]> {
    return withFluentAsyncGen(this.runQuery()).groupBy(keyFn, valueFn);
  }
}

/**
 * Utility to wrap a Goal (relation) for profiling. Recursively wraps nested relations.
 */
function wrapRelationForProfiling(
  goal: Goal,
  profileData: Map<Goal, { count: number; totalTime: number }>,
): Goal {
  if ((goal as any).__isProfiled) return goal;

  const wrapped: Goal = (s0: Subst) => {
    const start = Date.now();
    const gen = goal(s0);
    let finished = false;
    async function* profiledGen() {
      try {
        for await (const v of gen) {
          if (v === null) {
            EOSseen("wrapRelationForProfiling");
          }
          yield v;
        }
        finished = true;
      } finally {
        if (finished) {
          const elapsed = Date.now() - start;
          const entry = profileData.get(goal) ?? {
            count: 0,
            totalTime: 0,
          };
          profileData.set(goal, {
            count: entry.count + 1,
            totalTime: entry.totalTime + elapsed,
          });
        }
      }
    }
    return profiledGen();
  };
  Object.defineProperty(wrapped, "name", {
    value: goal.name,
    writable: false,
  });
  (wrapped as any).__isProfiled = true;
  return wrapped;
}

/**
 * Creates a new logic query builder.
 * Optionally accepts a function as a shorthand for .where(fn).
 */
export function query<Fmt>(whereFn?: (proxy: Record<string, Var>) => Goal | Goal[]) {
  const q = new Query<Fmt>();
  if (whereFn) {
    q.where(whereFn);
  }
  return q;
}

function walkAndReplaceVars(value: any, subst: Subst): any {
  if (typeof value === "object" && value !== null) {
    if (typeof value.deref === "function") {
      // Likely a logic Var
      return value.deref(subst);
    }
    if (Array.isArray(value)) {
      return value.map(v => walkAndReplaceVars(v, subst));
    }
    const result: any = {};
    for (const k in value) {
      result[k] = walkAndReplaceVars(value[k], subst);
    }
    return result;
  }
  return value;
}
