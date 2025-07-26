import { and } from "./combinators.js";
import { isLogicList, isVar, logicListToArray, lvar, walk } from "./kernel.js";
import { SimpleObservable } from "./observable.js";
import type {
	Goal,
	Observable,
	RunResult,
	Subst,
	Var,
} from "./types.js";

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
	prefix = "",
): { proxy: Record<K, Var>; varMap: Map<K, Var> } {
	const varMap = new Map<K, Var>();
	const proxy = new Proxy({} as Record<K, Var>, {
		get(_target, prop: K) {
			if (typeof prop !== "string") return undefined;
			if (prop === "_") return lvar();
			if (!varMap.has(prop)) {
				varMap.set(prop, lvar(`${prefix}${String(prop)}`));
			}
			return varMap.get(prop);
		},
		has: () => true,
		ownKeys: () => Array.from(varMap.keys()),
		getOwnPropertyDescriptor: () => ({
			enumerable: true,
			configurable: true,
		}),
	});
	return {
		proxy,
		varMap,
	};
}

/**
 * Formats the raw substitution streams into user-friendly result objects.
 */
function formatSubstitutions<Fmt>(
	substs: Observable<Subst>,
	formatter: Fmt,
	limit: number,
): Observable<RunResult<Fmt>> {
	// Use the built-in take operator which properly handles cleanup
	const limitedSubsts =
		limit === Infinity ? substs : (substs as any).take(limit);
	return {
		subscribe(observer) {
			const unsub = limitedSubsts.subscribe({
				next: (s: Subst) => {
					const result: Partial<RunResult<Fmt>> = {};
					for (const key in formatter) {
						if (key.startsWith("_")) continue;
						const term = formatter[key];
						result[key] = walk(term, s);
					}
					// Convert logic lists to arrays before yielding the final result
					observer.next(deepListWalk(result) as RunResult<Fmt>);
				},
				error: observer.error,
				complete: observer.complete,
			});
			if (typeof unsub === "function") return unsub;
			if (unsub && typeof unsub.unsubscribe === "function")
				return () => unsub.unsubscribe();
			return function noop() {
				/* pass */
			};
		},
	};
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
	select<NewSel extends "*">(
		selector: NewSel,
	): Query<Record<string, Var>, NewSel>;
	select<NewSel extends ($: Record<string, Var>) => any>(
		selector: NewSel,
	): Query<ReturnType<NewSel>, NewSel>;
	select<NewSel>(selector: NewSel): Query<any, any> {
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

	getSubstObservale() {
		const initialSubst: Subst = new Map();
		const combinedGoal = and(...this._goals);
		// Updated for streaming protocol: pass Observable<Subst> to the goal
		const substStream = combinedGoal(SimpleObservable.of(initialSubst));
		return substStream;
	}

	private getObservable(): Observable<any> {
		if (this._goals.length === 0) {
			throw new Error("Query must have at least one .where() clause.");
		}

		let formatter: Fmt | Record<string, Var> | any = this._formatter;
		if (this._selectAllVars) {
			formatter = {
				...this._logicVarProxy,
			};
		} else if (this._rawSelector) {
			formatter = {
				result: this._rawSelector,
			};
		} else if (!formatter) {
			formatter = {
				...this._logicVarProxy,
			};
		}

		const initialSubst: Subst = new Map();
		const combinedGoal = and(...this._goals);
		// Updated for streaming protocol: pass Observable<Subst> to the goal
		const substStream = combinedGoal(SimpleObservable.of(initialSubst));
		const results = formatSubstitutions(substStream, formatter, this._limit);

		const rawSelector = this._rawSelector;
		return {
			subscribe(observer) {
				return results.subscribe({
					next: (result) => {
						if (rawSelector) {
							observer.next(result.result);
						} else {
							observer.next(result);
						}
					},
					error: observer.error,
					complete: observer.complete,
				});
			},
		};
	}

	/**
	 * Makes the Query object itself an async iterable.
	 * Properly propagates cancellation upstream when the consumer stops early.
	 */
	async *[Symbol.asyncIterator](): AsyncGenerator<QueryOutput<Fmt, Sel>> {
		const observable = this.getObservable();
		const queue: QueryOutput<Fmt, Sel>[] = [];
		let completed = false;
		let error: any = null;
		let resolveNext: (() => void) | null = null;
		// let unsub: (() => void) | null = null;

		const nextPromise = () =>
			new Promise<void>((resolve) => {
				resolveNext = resolve;
			});

		const subcription = observable.subscribe({
			next: (result) => {
				queue.push(result);
				if (resolveNext) {
					resolveNext();
					resolveNext = null;
				}
			},
			error: (err) => {
				error = err;
				completed = true;
				if (resolveNext) {
					resolveNext();
					resolveNext = null;
				}
			},
			complete: () => {
				completed = true;
				if (resolveNext) {
					resolveNext();
					resolveNext = null;
				}
			},
		});

		try {
			while (!completed || queue.length > 0) {
				if (queue.length === 0) {
					await nextPromise();
				}
				while (queue.length > 0) {
					const item = queue.shift();
					if (item !== undefined) {
						yield item;
					}
				}
				if (error) throw error;
			}
		} finally {
			subcription.unsubscribe?.();
		}
	}

	/**
	 * Executes the query and returns all results as an array.
	 */
	async toArray(): Promise<QueryOutput<Fmt, Sel>[]> {
		const observable = this.getObservable();
		const results: QueryOutput<Fmt, Sel>[] = [];

		return new Promise((resolve, reject) => {
			observable.subscribe({
				next: (result) => {
					results.push(result);
				},
				error: reject,
				complete: () => resolve(results),
			});
		});
	}

	/**
	 * Returns the observable stream directly for reactive programming.
	 */
	toObservable(): Observable<QueryOutput<Fmt, Sel>> {
		return this.getObservable();
	}
}

/**
 * The main entry point for creating a new logic query.
 */
export function query<Fmt>(): Query<Fmt> {
	return new Query<Fmt>();
}
