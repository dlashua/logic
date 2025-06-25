import {
  Goal,
  and,
  enableLogicProfiling,
  disableLogicProfiling,
  printLogicProfileRecap
} from "./relations.ts"
import { Subst, Term, Var } from "./core.ts";
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

/**
 * A builder for creating and running logic queries.
 */
class Query<Fmt extends Record<string, Term<any>>, Out extends Record<string, any> = Fmt | Record<string, any>> {
  private _formatter: Fmt | null = null;
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
   * @param selector A function that receives a logic variable proxy and returns a formatter object, or "*" to select all logic vars.
   */
  select(selector: (($: Record<string, Var>) => Fmt) | "*"): Query<Fmt, Record<string, any>> | this {
    if (selector === "*") {
      this._selectAllVars = true;
      this._formatter = null;
      // Return a new Query instance with Out = Record<string, any>
      return (this as unknown) as Query<Fmt, Record<string, any>>;
    } else {
      this._formatter = selector(this._logicVarProxy);
      this._selectAllVars = false;
      return this;
    }
  }

  /**
   * Adds conditions to the query. Can be called multiple times.
   * If an array of goals is returned, they will be implicitly wrapped in an 'and'.
   * @param goalFn A function that receives a logic variable proxy and returns a goal or an array of goals.
   */
  where(goalFn: ($: Record<string, Var>) => Goal | Goal[]): this {
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
    return this._goals.length > 1 ? and(...this._goals) : this._goals[0];
  }

  private async *runQuery(): AsyncGenerator<RunResult<Fmt>> {
    let formatter: Fmt | null = this._formatter;
    if (!formatter || this._selectAllVars) {
      formatter = Object.fromEntries(
        Object.keys(this._logicVarProxy).map(k => [k, this._logicVarProxy[k]])
      ) as Fmt;
    }
    const goal = this._getGoal();
    const s0: Subst = new Map();
    const gen = formatSubstitutions(goal(s0), formatter, this._limit);
    for await (const result of gen) {
      yield result;
    }
  }

  /**
   * Makes the Query object itself an async iterable.
   */
  [Symbol.asyncIterator](): AsyncGenerator<RunResult<Fmt>> {
    return this.runQuery();
  }

  /**
   * Executes the query and returns all results as an array.
   */
  public toArray(): Promise<RunResult<Fmt>[]> {
    return withFluentAsyncGen(this.runQuery()).toArray();
  }

  /**
   * Executes the query and calls a function for each result.
   */
  public forEach(
    cb: (item: RunResult<Fmt>) => Promise<void> | void,
  ): Promise<void> {
    return withFluentAsyncGen(this.runQuery()).forEach(cb);
  }

  /**
   * Maps each result of the query to a new value.
   */
  public map<U>(
    cb: (item: RunResult<Fmt>) => Promise<U> | U,
  ): FluentAsyncGen<U> {
    return withFluentAsyncGen(this.runQuery()).map(cb);
  }

  /**
   * Groups the results of the query by a key.
   */
  public groupBy<K, V>(
    keyFn: (item: RunResult<Fmt>) => K,
    valueFn: (item: RunResult<Fmt>) => V,
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
 */
export function query<Fmt extends Record<string, Term<any>>>() {
  return new Query<Fmt>();
}
