import {
  Goal,
  RunResult,
  Subst,
  Term,
  Var
} from "./core/types.ts"
import {
  walk,
  lvar,
  isLogicList,
  isVar,
  logicListToArray
} from "./core/kernel.ts"
import { and } from "./core/combinators.ts";

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
