import { Goal, and } from "./relations.ts";
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

/**
 * A builder for creating and running logic queries.
 */
class Query<Fmt extends Record<string, Term<any>>> {
  private _formatter: Fmt | null = null;
  private _goals: Goal[] = [];
  private _limit = Infinity;
  private readonly _logicVarProxy: Record<string, Var>;

  constructor() {
    const { proxy } = createLogicVarProxy("q");
    this._logicVarProxy = proxy;
  }

  /**
   * Specifies the shape of the desired output.
   * @param selector A function that receives a logic variable proxy and returns a formatter object.
   */
  select(selector: ($: Record<string, Var>) => Fmt): this {
    this._formatter = selector(this._logicVarProxy);
    return this;
  }

  /**
   * Adds conditions to the query. Can be called multiple times.
   * If an array of goals is returned, they will be implicitly wrapped in an 'and'.
   * @param goalFn A function that receives a logic variable proxy and returns a goal or an array of goals.
   */
  where(goalFn: ($: Record<string, Var>) => Goal | Goal[]): this {
    const result = goalFn(this._logicVarProxy);
    const newGoals = Array.isArray(result) ? result : [result];
    this._goals.push(...newGoals);
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

  private _getGoal(): Goal {
    if (this._goals.length === 0) {
      throw new Error("Query must have at least one where clause.");
    }
    return this._goals.length > 1 ? and(...this._goals) : this._goals[0];
  }

  private async *runQuery(): AsyncGenerator<RunResult<Fmt>> {
    if (!this._formatter) {
      throw new Error("Query must have a select clause.");
    }
    const goal = this._getGoal();
    const s0: Subst = new Map();
    const gen = formatSubstitutions(goal(s0), this._formatter, this._limit);
    yield* gen;
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
 * Creates a new logic query builder.
 */
export function query<Fmt extends Record<string, Term<any>>>() {
  return new Query<Fmt>();
}
