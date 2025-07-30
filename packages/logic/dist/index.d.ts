type OperatorFunction<T, R> = (source: SimpleObservable<T>) => SimpleObservable<R>;
declare class SimpleObservable<T> implements Observable<T> {
    private producer;
    constructor(producer: (observer: Observer<T>) => (() => void) | void | Promise<(() => void) | undefined>);
    subscribe(observer: Observer<T>): Subscription;
    static of<T>(...values: T[]): SimpleObservable<T>;
    static from<T>(values: T[]): SimpleObservable<T>;
    static empty<T>(): SimpleObservable<T>;
    static fromAsyncGenerator<T>(generator: AsyncGenerator<T>): Observable<T>;
    static fromPromise<T>(promise: Promise<T>): Observable<T>;
    toArray(): Promise<T[]>;
    firstFrom(): Promise<T>;
    lift<V>(next_observable: (input$: SimpleObservable<T>) => SimpleObservable<V>): SimpleObservable<V>;
    lastFrom(): Promise<T>;
    filter(predicate: (value: T) => boolean): SimpleObservable<T>;
    flatMap<U>(transform: (value: T) => SimpleObservable<U>): SimpleObservable<U>;
    map<U>(transform: (value: T) => U): SimpleObservable<U>;
    merge<R>(other: SimpleObservable<R>): SimpleObservable<T | R>;
    pipe(): SimpleObservable<T>;
    pipe<A>(op1: OperatorFunction<T, A>): SimpleObservable<A>;
    pipe<A, B>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>): SimpleObservable<B>;
    pipe<A, B, C>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>): SimpleObservable<C>;
    pipe<A, B, C, D>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>): SimpleObservable<D>;
    pipe<A, B, C, D, E>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>): SimpleObservable<E>;
    pipe<A, B, C, D, E, F>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>): SimpleObservable<F>;
    pipe<A, B, C, D, E, F, G>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>, op7: OperatorFunction<F, G>): SimpleObservable<G>;
    pipe<A, B, C, D, E, F, G, H>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>, op7: OperatorFunction<F, G>, op8: OperatorFunction<G, H>): SimpleObservable<H>;
    pipe<A, B, C, D, E, F, G, H, I>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>, op7: OperatorFunction<F, G>, op8: OperatorFunction<G, H>, op9: OperatorFunction<H, I>): SimpleObservable<I>;
    reduce<Q>(reducer: (accumulator: Q, value: unknown) => Q, initalValue: unknown): SimpleObservable<Q>;
    share(bufferSize?: number): SimpleObservable<T>;
    take(count: number): SimpleObservable<T>;
}
declare const observable: <T>(producer: (observer: Observer<T>) => (() => void) | undefined) => SimpleObservable<T>;

/**
 * Represents a logic variable, a placeholder for a value.
 */
interface Var {
    readonly tag: "var";
    readonly id: string;
}
/**
 * A `cons` cell, the building block of a logic list.
 */
interface ConsNode {
    readonly tag: "cons";
    readonly head: Term;
    readonly tail: Term;
    readonly id?: string;
}
/**
 * The end of a logic list.
 */
interface NilNode {
    readonly tag: "nil";
}
/**
 * A logic list is either a `cons` cell or `nil`.
 */
type LogicList = ConsNode | NilNode;
/**
 * A substitution map, holding variable bindings.
 */
type Subst = Map<string | symbol, Term>;
/**
 * Represents any term in the logic system.
 */
type Term<T = unknown> = Var | LogicList | T | Term<T>[] | null | undefined;
/**
 * Observable interface for lazy evaluation and backpressure control
 */
interface Observable<T> {
    subscribe(observer: Observer<T>): Subscription;
}
/**
 * Observer interface for consuming observable streams
 */
interface Observer<T> {
    next(value: T): void;
    error?(error: any): void;
    complete?(): void;
}
/**
 * Subscription interface for managing observable lifecycle
 */
interface Subscription {
    unsubscribe(): void;
    readonly closed: boolean;
}
/**
 * A Goal is a function that takes an Observable stream of substitutions and returns
 * an Observable stream of possible resulting substitutions.
 */
type Goal = (input$: SimpleObservable<Subst>) => SimpleObservable<Subst>;
/**
 * The shape of a single result from a query.
 */
type RunResult<Fmt> = {
    [K in keyof Fmt]: Term;
};
/**
 * Type for lifted function arguments
 */
type LiftableFunction<T> = (...args: unknown[]) => T;
/**
 * Type for lifted function arguments
 */
type LiftedArgs<T extends LiftableFunction<U>, U> = T extends (...args: infer A) => U ? (...args: [...{
    [I in keyof A]: Term<A[I]> | A[I];
}, out: Term<U>]) => Goal : never;
/**
 * Stream configuration for controlling evaluation behavior
 */
interface StreamConfig {
    /** Maximum number of results to produce */
    maxResults?: number;
    /** Timeout in milliseconds */
    timeout?: number;
    /** Enable lazy evaluation (default: true) */
    lazy?: boolean;
}
/**
 * Result of stream evaluation
 */
interface StreamResult<T> {
    values: T[];
    completed: boolean;
    error?: any;
}

/**
 * A goal that succeeds if two terms can be unified.
 */
declare function eq(x: Term, y: Term): Goal;
/**
 * Introduces new (fresh) logic variables into a sub-goal.
 */
declare function fresh(f: (...vars: Var[]) => Goal): Goal;
/**
 * Logical disjunction (OR).
 */
declare function disj(g1: Goal, g2: Goal): Goal;
/**
 * Logical conjunction (AND).
 */
declare function conj(g1: Goal, g2: Goal): Goal;
/**
 * Helper for combining multiple goals with logical AND.
 * Creates a single group containing all goals for optimal SQL merging.
 */
declare const and: (...goals: Goal[]) => Goal;
/**
 * Helper for combining multiple goals with logical OR.
 * Creates a single group containing all goals for optimal SQL merging.
 */
declare const or: (...goals: Goal[]) => Goal;
/**
 * Multi-clause disjunction (OR).
 */
declare function conde(...clauses: Goal[][]): Goal;
/**
 * Lifts a pure JavaScript function into a Goal function.
 */
declare function lift<U, T extends LiftableFunction<U>>(fn: T): LiftedArgs<T, U>;
/**
 * Either-or combinator: tries the first goal, and only if it produces no results,
 * tries the second goal. This is different from `or` which tries both goals.
 */
declare function eitherOr(firstGoal: Goal, secondGoal: Goal): Goal;
/**
 * Soft-cut if-then-else combinator.
 */
declare function ifte(ifGoal: Goal, thenGoal: Goal, elseGoal: Goal): Goal;
/**
 * Succeeds exactly once with the given substitution (useful for cut-like behavior)
 */
declare function once(goal: Goal): Goal;
/**
 * Apply a goal with a timeout
 */
declare function timeout(goal: Goal, timeoutMs: number): Goal;
/**
 * Run a goal and collect results with optional limits
 */
declare function run(goal: Goal, maxResults?: number, timeoutMs?: number): Promise<{
    results: Subst[];
    completed: boolean;
    error?: string | Error;
}>;
declare function project(inputVar: Term, pathOrMap: string | Record<string, string>, outputVar: Term): Goal;
/**
 * projectJsonata: Declarative data transformation using JSONata expressions.
 *
 * @param inputVars - An object mapping keys to logic vars, or a single logic var.
 * @param jsonataExpr - The JSONata template string.
 * @param outputVars - An object mapping output keys to logic vars, or a single logic var.
 *
 * Example:
 *   projectJsonata({ x: $.some_var, y: $.some_other_var }, "{ thing: x, thang: y }", { thing: $.thing_here, thang: $.thang_here })
 *   projectJsonata($.input, "$value + 1", $.output)
 */
declare function projectJsonata(inputVars: Term | Record<string, Term>, jsonataExpr: string, outputVars: Term | Record<string, Term>): Goal;
/**
 * Subquery: Run a subgoal and bind its results to a variable in the main stream.
 * This is the universal bridge between goal-based and stream-based operations.
 *
 * @param goal - The subgoal to run
 * @param extractVar - Variable to extract from subgoal results
 * @param bindVar - Variable to bind the extracted results to in main stream
 * @param aggregator - How to combine multiple results (receives results and original substitution)
 *
 * Examples:
 * - Subquery(membero(x, [1,2,3]), x, $.items) // binds $.items to [1,2,3]
 * - Subquery(goal, x, $.count, (results, _) => results.length) // binds $.count to result count
 * - Subquery(goal, x, $.count, (results, s) => results.filter(r => r === walk(target, s)).length) // count matches
 */
declare function Subquery(goal: Goal, extractVar: Term, bindVar: Term, aggregator?: (results: unknown[], originalSubst: Subst) => unknown): Goal;
declare function branch(goal: Goal, aggregator: (observer: Observer<Subst>, substs: Subst[], originalSubst: Subst) => void): Goal;

declare const GOAL_GROUP_ID: unique symbol;
declare const GOAL_GROUP_PATH: unique symbol;
declare const GOAL_GROUP_CONJ_GOALS: unique symbol;
declare const GOAL_GROUP_ALL_GOALS: unique symbol;
declare function nextGroupId(): number;
/**
 * Creates a new, unique logic variable.
 * @param name An optional prefix for debugging.
 */
declare function lvar(name?: string): Var;
/**
 * Resets the global variable counter for deterministic tests.
 */
declare function resetVarCounter(): void;
/**
 * Recursively finds the ultimate binding of a term in a given substitution.
 * Optimized to use iteration for variable chains and avoid deep recursion.
 * @param u The term to resolve.
 * @param s The substitution map.
 */
declare function walk(u: Term, s: Subst): Term;
/**
 * Extends a substitution by binding a variable to a value, with an occurs check.
 */
declare function extendSubst(v: Var, val: Term, s: Subst): Subst | null;
/**
 * The core unification algorithm. It attempts to make two terms structurally equivalent.
 * Optimized with fast paths for common cases.
 */
declare function baseUnify(u: Term, v: Term, s: Subst | null): Subst | null;
/**
 * Constraint-aware unify that wakes up suspended constraints when variables are bound
 */
declare function unifyWithConstraints(u: Term, v: Term, s: Subst | null): Subst | null;
declare const unify: typeof unifyWithConstraints;
/**
 * Type guard to check if a term is a logic variable.
 */
declare function isVar(x: Term): x is Var;
/**
 * The canonical `nil` value, representing an empty logic list.
 */
declare const nil: NilNode;
/**
 * Creates a `cons` cell (a node in a logic list).
 */
declare function cons(head: Term, tail: Term): ConsNode;
/**
 * Converts a JavaScript array into a logic list.
 */
declare function arrayToLogicList(arr: Term[]): LogicList;
/**
 * A convenience function to create a logic list from arguments.
 */
declare function logicList<T = unknown>(...items: T[]): LogicList;
/**
 * Type guard to check if a term is a `cons` cell.
 */
declare function isCons(x: Term): x is ConsNode;
/**
 * Type guard to check if a term is `nil`.
 */
declare function isNil(x: Term): x is NilNode;
/**
 * Type guard to check if a term is a logic list.
 */
declare function isLogicList(x: Term): x is LogicList;
/**
 * Converts a logic list to a JavaScript array.
 */
declare function logicListToArray(list: Term): Term[];
declare function liftGoal(singleGoal: (s: Subst) => SimpleObservable<Subst>): Goal;
declare function chainGoals(goals: Goal[], initial$: SimpleObservable<Subst>): SimpleObservable<Subst>;
/**
 * Creates an enriched substitution with group metadata
 */
declare function createEnrichedSubst(s: Subst, type: string, conjGoals: Goal[], disjGoals: Goal[], branch?: number): Subst;
/**
 * Unified helper for enriching input with group metadata
 */
declare function enrichGroupInput(type: string, conjGoals: Goal[], disjGoals: Goal[], fn: (enrichedInput$: SimpleObservable<Subst>) => SimpleObservable<Subst>): (input$: SimpleObservable<Subst>) => SimpleObservable<Subst>;

type ObserverOperator<A, B> = (input$: SimpleObservable<A>) => SimpleObservable<B>;
declare function merge<A, B>(obsB: SimpleObservable<B>): (obsA: SimpleObservable<A>) => SimpleObservable<A | B>;
declare function reduce<V>(reducer: (accumulator: V, value: unknown) => V, initialValue: unknown): ObserverOperator<unknown, V>;
declare function map<T, U>(transform: (value: T) => U): (input$: SimpleObservable<T>) => SimpleObservable<U>;
declare function take<T>(count: number): (input$: SimpleObservable<T>) => SimpleObservable<T>;
declare function filter<T>(predicate: (value: T) => boolean): (input$: SimpleObservable<T>) => SimpleObservable<T>;
declare function flatMap<T, U>(transform: (value: T) => SimpleObservable<U>): (input$: SimpleObservable<T>) => SimpleObservable<U>;
declare function share<T>(bufferSize?: number): (input$: SimpleObservable<T>) => SimpleObservable<T>;
declare function pipe<T>(): OperatorFunction<T, T>;
declare function pipe<T, A>(op1: OperatorFunction<T, A>): OperatorFunction<T, A>;
declare function pipe<T, A, B>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>): OperatorFunction<T, B>;
declare function pipe<T, A, B, C>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>): OperatorFunction<T, C>;
declare function pipe<T, A, B, C, D>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>): OperatorFunction<T, D>;
declare function pipe<T, A, B, C, D, E>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>): OperatorFunction<T, E>;
declare function pipe<T, A, B, C, D, E, F>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>): OperatorFunction<T, F>;
declare function pipe<T, A, B, C, D, E, F, G>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>, op7: OperatorFunction<F, G>): OperatorFunction<T, G>;
declare function pipe<T, A, B, C, D, E, F, G, H>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>, op7: OperatorFunction<F, G>, op8: OperatorFunction<G, H>): OperatorFunction<T, H>;
declare function pipe<T, A, B, C, D, E, F, G, H, I>(op1: OperatorFunction<T, A>, op2: OperatorFunction<A, B>, op3: OperatorFunction<B, C>, op4: OperatorFunction<C, D>, op5: OperatorFunction<D, E>, op6: OperatorFunction<E, F>, op7: OperatorFunction<F, G>, op8: OperatorFunction<G, H>, op9: OperatorFunction<H, I>): OperatorFunction<T, I>;

/**
 * Creates a proxy object that automatically creates logic variables on access.
 */
declare function createLogicVarProxy<K extends string | symbol = string>(prefix?: string): {
    proxy: Record<K, Var>;
    varMap: Map<K, Var>;
};
type QueryOutput<Fmt, Sel> = Sel extends ($: Record<string, Var>) => Fmt ? RunResult<Fmt> : Sel extends "*" ? RunResult<Record<string, any>> : any;
/**
 * A fluent interface for building and executing logic queries.
 */
declare class Query<Fmt = Record<string, Var>, Sel = "*"> {
    private _formatter;
    private _rawSelector;
    private _goals;
    private _limit;
    private readonly _logicVarProxy;
    private _selectAllVars;
    constructor();
    /**
     * Specifies the shape of the desired output.
     */
    select<NewSel extends "*">(selector: NewSel): Query<Record<string, Var>, NewSel>;
    select<NewSel extends ($: Record<string, Var>) => any>(selector: NewSel): Query<ReturnType<NewSel>, NewSel>;
    /**
     * Adds constraints (goals) to the query.
     */
    where(goalFn: (proxy: Record<string, Var>) => Goal | Goal[]): this;
    /**
     * Sets the maximum number of results.
     */
    limit(n: number): this;
    getSubstObservale(): SimpleObservable<Subst>;
    private getObservable;
    /**
     * Makes the Query object itself an async iterable.
     * Properly propagates cancellation upstream when the consumer stops early.
     */
    [Symbol.asyncIterator](): AsyncGenerator<QueryOutput<Fmt, Sel>>;
    /**
     * Executes the query and returns all results as an array.
     */
    toArray(): Promise<QueryOutput<Fmt, Sel>[]>;
    /**
     * Returns the observable stream directly for reactive programming.
     */
    toObservable(): Observable<QueryOutput<Fmt, Sel>>;
}
/**
 * The main entry point for creating a new logic query.
 */
declare function query<Fmt>(): Query<Fmt>;

declare const CHECK_LATER: unique symbol;
/**
 * Generic constraint helper that handles suspension automatically
 */
declare function makeSuspendHandler(vars: Term[], evaluator: (values: Term[], subst: Subst) => Subst | null | typeof CHECK_LATER, minGrounded: number): (subst: Subst) => Subst | null;
declare function suspendable<T extends Term[]>(vars: T, evaluator: (values: Term[], subst: Subst) => Subst | null | typeof CHECK_LATER, minGrounded?: number): Goal;

declare const SUSPENDED_CONSTRAINTS: unique symbol;
interface SuspendedConstraint {
    id: string;
    resumeFn: (subst: Subst) => Subst | null | typeof CHECK_LATER;
    watchedVars: string[];
}
declare function addSuspendToSubst(subst: Subst, resumeFn: (subst: Subst) => Subst | null | typeof CHECK_LATER, watchedVars: string[]): Subst;
declare function getSuspendsFromSubst(subst: Subst): SuspendedConstraint[];
declare function removeSuspendFromSubst(subst: Subst, suspendIds: string[]): Subst;
declare function wakeUpSuspends(subst: Subst, newlyBoundVars: string[]): Subst | null;

/**
 * count_value_streamo(x, value, count):
 *   count is the number of times x == value in the current stream of substitutions.
 *   (Stream-based version: aggregates over the current stream, like maxo/mino.)
 *
 * Usage: count_value_streamo(x, value, count)
 */
declare function count_value_streamo(x: Term, value: Term, count: Term): Goal;
/**
 * group_by_count_streamo(x, count, drop?):
 *   Groups the input stream by values of x and counts each group.
 *   - If drop=false (default): Preserves all variables from original substitutions,
 *     emitting one result for EACH substitution in each group with the count added.
 *   - If drop=true: Creates fresh substitutions with ONLY x and count variables.
 *   Example: if stream is x=A,y=1; x=A,y=2; x=B,y=3
 *   - drop=false: emits x=A,y=1,count=2; x=A,y=2,count=2; x=B,y=3,count=1
 *   - drop=true: emits x=A,count=2; x=B,count=1
 */
declare function group_by_count_streamo(x: Term, count: Term, drop?: boolean): Goal;
/**
 * sort_by_streamo(x, orderOrFn?):
 *   Sorts the stream of substitutions by the value of x.
 *   - If orderOrFn is 'asc' (default), sorts ascending.
 *   - If orderOrFn is 'desc', sorts descending.
 *   - If orderOrFn is a function (a, b) => number, uses it as the comparator on walked x values.
 *   Emits the same substitutions, but in sorted order by x.
 *   Example: if stream is x=3, x=1, x=2, emits x=1, x=2, x=3 (asc)
 */
declare function sort_by_streamo(x: Term, orderOrFn?: "asc" | "desc" | ((a: any, b: any) => number)): Goal;
/**
 * take_streamo(n):
 *   Allows only the first n substitutions to pass through the stream.
 *   Example: take_streamo(3) will emit only the first 3 substitutions.
 */
declare function take_streamo(n: number): Goal;
/**
 * group_by_collect_streamo(keyVar, valueVar, outList, drop?):
 *   Groups the input stream by keyVar and collects valueVar values into lists.
 *   The keyVar is preserved in the output (no need for separate outKey parameter).
 *   - If drop=false (default): Preserves all variables from original substitutions,
 *     emitting one result for EACH substitution in each group with the collected list added.
 *   - If drop=true: Creates fresh substitutions with ONLY keyVar and outList variables.
 *   Example: if stream is x=A,y=1; x=A,y=2; x=B,y=3
 *   - drop=false: emits x=A,y=1,list=[1,2]; x=A,y=2,list=[1,2]; x=B,y=3,list=[3]
 *   - drop=true: emits x=A,list=[1,2]; x=B,list=[3]
 */
declare function group_by_collect_streamo<T>(keyVar: Term, valueVar: Term<T>, outList: Term<T[]>, drop?: boolean): Goal;
declare function group_by_collect_distinct_streamo<T>(keyVar: Term, valueVar: Term<T>, outList: Term<T[]>, drop?: boolean): Goal;
declare function collect_streamo(valueVar: Term, outList: Term, drop?: boolean): Goal;

/**
 * Base functions for building aggregation operations.
 *
 * These are foundational building blocks intended for creating new aggregation
 * relations, not for direct use by end users. They handle the low-level
 * subscription, buffering, grouping, and cleanup patterns that most
 * aggregation functions need.
 *
 * Functions ending in _base are infrastructure - use the public aggregation
 * functions in aggregates.ts instead.
 */

/**
 * Helper: collect all substitutions from a stream, then process them all at once.
 * Handles subscription, buffering, cleanup, and error management.
 * This is a foundational building block for aggregation functions that need
 * to see all data before processing (like sorting).
 *
 * @param processor - Function that receives all buffered substitutions and observer to emit results
 */
declare function collect_and_process_base(processor: (buffer: Subst[], observer: {
    next: (s: Subst) => void;
}) => void): Goal;
/**
 * Generic stream-based grouping function - the foundation for all group_by_*_streamo functions.
 * Groups substitutions by keyVar and applies an aggregator function to each group.
 * This is a foundational building block for all grouping operations.
 *
 * @param keyVar - Variable to group by
 * @param valueVar - Variable to extract values from (null for count-only operations)
 * @param outVar - Variable to bind the aggregated result to
 * @param drop - If true, create fresh substitutions; if false, preserve original variables
 * @param aggregator - Function that takes (values, substitutions) and returns aggregated result
 */
declare function group_by_streamo_base(keyVar: Term, valueVar: Term | null, outVar: Term, drop: boolean, aggregator: (values: any[], substitutions: Subst[]) => any): Goal;

/**
 * aggregateRelFactory: generic helper for collecto, collect_distincto, counto.
 * - x: variable to collect
 * - goal: logic goal
 * - out: output variable
 * - aggFn: aggregation function (receives array of results)
 * - dedup: if true, deduplicate results
 */
declare function aggregateRelFactory(aggFn: (results: Term[]) => any, dedup?: boolean): (x: Term, goal: Goal, out: Term) => Goal;
/**
 * collecto(x, goal, xs): xs is the list of all values x can take under goal (logic relation version)
 * Usage: collecto(x, membero(x, ...), xs)
 */
declare const collecto: (x: Term, goal: Goal, out: Term) => Goal;
/**
 * collect_distincto(x, goal, xs): xs is the list of distinct values of x under goal.
 * Usage: collect_distincto(x, goal, xs)
 */
declare const collect_distincto: (x: Term, goal: Goal, out: Term) => Goal;
/**
 * counto(x, goal, n): n is the number of (distinct) values of x under goal.
 * Usage: counto(x, goal, n)
 */
declare const counto: (x: Term, goal: Goal, out: Term) => Goal;
declare const count_distincto: (x: Term, goal: Goal, out: Term) => Goal;
/**
 * count_valueo(x, goal, value, count):
 *   count is the number of times x == value in the stream of substitutions from goal.
 *   (Canonical, goal-wrapping version: aggregates over all solutions to goal.)
 *
 *   This is implemented using Subquery with a custom aggregator that counts
 *   how many times the extracted value equals the target value (walked in context).
 */
declare function count_valueo(x: Term, goal: Goal, value: Term, count: Term): Goal;
/**
 * groupAggregateRelFactory(aggFn): returns a group-by aggregation goal constructor.
 * The returned function has signature (keyVar, valueVar, goal, outKey, outAgg, dedup?) => Goal
 * Example: const group_by_collecto = groupAggregateRelFactory(arrayToLogicList)
 */
declare function groupAggregateRelFactory(aggFn: (items: any[]) => any, dedup?: boolean): (keyVar: Term, valueVar: Term, goal: Goal, outValueAgg: Term) => Goal;
declare const group_by_collecto: (keyVar: Term, valueVar: Term, goal: Goal, outValueAgg: Term) => Goal;
declare const group_by_counto: (keyVar: Term, valueVar: Term, goal: Goal, outValueAgg: Term) => Goal;

declare const uniqueo: (t: Term, g: Goal) => Goal;
declare function not(goal: Goal): Goal;
declare function gv1_not(goal: Goal): Goal;
declare function old_not(goal: Goal): Goal;
declare function neqo(x: Term<any>, y: Term<any>): Goal;
declare function old_neqo(x: Term<any>, y: Term<any>): Goal;
/**
 * A goal that succeeds if the given goal succeeds exactly once.
 * Useful for cut-like behavior.
 */
declare function onceo(goal: Goal): Goal;
/**
 * A goal that always succeeds with the given substitution.
 * Useful as a base case or for testing.
 */
declare function succeedo(): Goal;
/**
 * A goal that always fails.
 * Useful for testing or as a base case.
 */
declare function failo(): Goal;
/**
 * A goal that succeeds if the term is ground (contains no unbound variables).
 */
declare function groundo(term: Term): Goal;
/**
 * A goal that succeeds if the term is not ground (contains unbound variables).
 */
declare function nonGroundo(term: Term): Goal;
/**
 * A goal that logs each substitution it sees along with a message.
 */
declare function substLog(msg: string, onlyVars?: boolean): Goal;
declare function thruCount(msg: string, level?: number): Goal;
declare function fail(): Goal;

declare function membero(x: Term, list: Term): Goal;
/**
 * A goal that succeeds if `h` is the head of the logic list `l`.
 */
declare function firsto(x: Term, xs: Term): Goal;
/**
 * A goal that succeeds if `t` is the tail of the logic list `l`.
 */
declare function resto(xs: Term, tail: Term): Goal;
/**
 * A goal that succeeds if logic list `zs` is the result of appending
 * logic list `ys` to `xs`.
 */
declare function appendo(xs: Term, ys: Term, zs: Term): Goal;
/**
 * A goal that unifies the length of an array or logic list with a numeric value.
 * @param arrayOrList The array or logic list to measure
 * @param length The length to unify with
 */
declare function lengtho(arrayOrList: Term, length: Term): Goal;
declare function permuteo(xs: Term, ys: Term): Goal;
declare function mapo(rel: (x: Term, y: Term) => Goal, xs: Term, ys: Term): Goal;
declare function removeFirsto(xs: Term, x: Term, ys: Term): Goal;
/**
 * alldistincto(xs): true if all elements of xs are distinct.
 */
declare function alldistincto(xs: Term): Goal;

/**
 * A goal that succeeds if the numeric value in the first term is greater than
 * the numeric value in the second term.
 */
declare function gto(x: Term<number>, y: Term<number>): Goal;
/**
 * A goal that succeeds if the numeric value in the first term is less than
 * the numeric value in the second term.
 */
declare function lto(x: Term<number>, y: Term<number>): Goal;
/**
 * A goal that succeeds if the numeric value in the first term is greater than or equal to
 * the numeric value in the second term.
 */
declare function gteo(x: Term<number>, y: Term<number>): Goal;
/**
 * A goal that succeeds if the numeric value in the first term is less than or equal to
 * the numeric value in the second term.
 */
declare function lteo(x: Term<number>, y: Term<number>): Goal;
/**
 * A goal that succeeds if z is the sum of x and y.
 * Can work in multiple directions if some variables are grounded.
 */
declare function pluso(x: Term<number>, y: Term<number>, z: Term<number>): Goal;
declare const minuso: (x: Term<number>, y: Term<number>, z: Term<number>) => Goal;
/**
 * A goal that succeeds if z is the product of x and y.
 * Can work in multiple directions if some variables are grounded.
 */
declare function multo(x: Term<number>, y: Term<number>, z: Term<number>): Goal;
declare const dividebyo: (x: Term<number>, y: Term<number>, z: Term<number>) => Goal;
/**
 * A goal that succeeds only for the substitution(s) that have the maximum value
 * for the given variable across all input substitutions.
 *
 * Usage: maxo($.movie_popularity) - selects the substitution with highest movie_popularity
 */
declare function maxo(variable: Term): Goal;
/**
 * A goal that succeeds only for the substitution(s) that have the minimum value
 * for the given variable across all input substitutions.
 *
 * Usage: mino($.movie_popularity) - selects the substitution with lowest movie_popularity
 */
declare function mino(variable: Term): Goal;

/**
 * A goal that extracts specific keys from an object and unifies them with logic variables.
 * This is a simpler alternative to projectJsonata for basic object key extraction.
 *
 * Usage:
 *   extract($.input_object, {
 *     name: $.output_name,
 *     age: $.output_age,
 *     nested: {
 *       city: $.output_city,
 *       country: $.output_country
 *     }
 *   })
 *
 * If the mapping value is a logic variable, it unifies directly.
 * If the mapping value is an object/array, it recursively extracts from nested structures.
 */
declare function extract(inputVar: Term, mapping: Record<string, Term>): Goal;
/**
 * A goal that combines membero() and extract() - iterates over an array and extracts
 * specific keys from each element, creating one substitution per array element.
 *
 * Usage:
 *   extractEach($.array_of_objects, {
 *     name: $.item_name,
 *     age: $.item_age,
 *     email: $.item_email
 *   })
 *
 * This is equivalent to:
 *   membero($.item, $.array_of_objects),
 *   extract($.item, { name: $.item_name, age: $.item_age, email: $.item_email })
 *
 * But more concise and clearer in intent.
 */
declare function extractEach(arrayVar: Term, mapping: Record<string, Term>): Goal;

interface LoggerConfig {
    enabled: boolean;
    allowedIds: Set<string>;
    deniedIds: Set<string>;
}
declare class Logger {
    private config;
    constructor(config: LoggerConfig);
    log(id: string, data: Record<string, any> | string | (() => Record<string, any> | string)): void;
}
declare function getDefaultLogger(): Logger;

interface BaseConfig {
    readonly cache: CacheConfig;
    readonly logging: LogConfig;
}
interface CacheConfig {
    readonly patternCacheEnabled: boolean;
    readonly maxSize?: number;
    readonly ttlMs?: number;
    readonly cleanupIntervalMs?: number;
}
interface LogConfig {
    readonly enabled: boolean;
    readonly ignoredIds: Set<string>;
    readonly criticalIds: Set<string>;
}
interface QueryResult<T = any> {
    readonly rows: readonly T[];
    readonly fromCache: boolean;
    readonly cacheType?: "pattern" | "row" | "query";
    readonly source?: string;
}
type CacheType = "pattern" | "row" | "query";
interface WhereClause {
    readonly column: string;
    readonly value: Term;
    readonly operator?: "eq" | "gt" | "lt" | "in";
}
interface QueryParts {
    readonly selectCols: string[];
    readonly whereClauses: WhereClause[];
    readonly walkedQ: Record<string, Term>;
}
interface IndexMap<T = any> {
    get(key: T): Set<number> | undefined;
    set(key: T, value: Set<number>): void;
    has(key: T): boolean;
}
interface IndexManager<K = any> {
    get(position: K): IndexMap | undefined;
    set(position: K, index: IndexMap): void;
    has(position: K): boolean;
}
type GoalFunction = (s: Subst) => AsyncGenerator<any, void, unknown>;

declare const queryUtils: {
    /**
     * Walk all keys of an object with a substitution and return a new object
     */
    walkAllKeys<T extends Record<string, Term>>(obj: T, subst: Subst): Record<string, Term>;
    /**
     * Walk all values in an array with a substitution
     */
    walkAllArray(arr: Term[], subst: Subst): Term[];
    /**
     * Check if all query parameters are grounded (no variables)
     */
    allParamsGrounded(params: Record<string, Term>): boolean;
    /**
     * Check if all array elements are grounded (no variables)
     */
    allArrayGrounded(arr: Term[]): boolean;
    /**
     * Build query parts from parameters and substitution
     */
    buildQueryParts(params: Record<string, Term>, subst: Subst): {
        selectCols: string[];
        whereClauses: WhereClause[];
        walkedQ: Record<string, unknown>;
    };
    onlyGrounded<T>(params: Record<string, Term<T>>): Record<string, T>;
    onlyVars(params: Record<string, Term>): Record<string, Var>;
};
declare const unificationUtils: {
    /**
     * Unify all selectCols in a row with walkedQ and subst
     */
    unifyRowWithWalkedQ(selectCols: string[], walkedQ: Record<string, Term>, row: Record<string, any>, subst: Subst): Subst | null;
    /**
     * Unify arrays element by element
     */
    unifyArrays(queryArray: Term[], factArray: Term[], subst: Subst): Subst | null;
};
declare const patternUtils: {
    /**
     * Check if all select columns are tags (have id property)
     */
    allSelectColsAreTags(cols: Record<string, Term>): boolean;
    /**
     * Separate query object into select and where columns
     */
    separateQueryColumns(queryObj: Record<string, Term>): {
        selectCols: Record<string, unknown>;
        whereCols: Record<string, unknown>;
    };
    /**
     * Separate array query into select and where terms
     */
    separateArrayQuery(queryArray: Term[]): {
        selectTerms: unknown[];
        whereTerms: unknown[];
        positions: number[];
    };
    /**
     * Separate symmetric query values into select and where - optimized
     */
    separateSymmetricColumns(queryObj: Record<string, Term>): {
        selectCols: unknown[];
        whereCols: unknown[];
    };
};
declare const indexUtils: {
    /**
     * Returns the intersection of two sets
     */
    intersect<T>(setA: Set<T>, setB: Set<T>): Set<T>;
    /**
     * Returns true if a value is indexable (string, number, boolean, or null)
     */
    isIndexable(v: any): boolean;
    /**
     * Create an index for a specific position/key
     */
    createIndex<T>(): Map<T, Set<number>>;
    /**
     * Add a value to an index
     */
    addToIndex<T>(index: Map<T, Set<number>>, key: T, factIndex: number): void;
};
declare const intersect: <T>(setA: Set<T>, setB: Set<T>) => Set<T>;
declare const isIndexable: (v: any) => boolean;

/**
 * Aggregates all possible values of a logic variable into an array and binds to sourceVar in a single solution.
 */
declare function aggregateVar(sourceVar: Var, subgoal: Goal): Goal;
/**
 * For each unique combination of groupVars, aggregate all values of each aggVar in aggVars, and yield a substitution with arrays bound to each aggVar.
 */
declare function aggregateVarMulti(groupVars: Var[], aggVars: Var[], subgoal: Goal): Goal;

export { type BaseConfig, CHECK_LATER, type CacheConfig, type CacheType, type ConsNode, GOAL_GROUP_ALL_GOALS, GOAL_GROUP_CONJ_GOALS, GOAL_GROUP_ID, GOAL_GROUP_PATH, type Goal, type GoalFunction, type IndexManager, type IndexMap, type LiftableFunction, type LiftedArgs, type LogConfig, Logger, type LoggerConfig, type LogicList, type NilNode, type Observable, type Observer, type OperatorFunction, type QueryParts, type QueryResult, type RunResult, SUSPENDED_CONSTRAINTS, SimpleObservable, type StreamConfig, type StreamResult, Subquery, type Subscription, type Subst, type SuspendedConstraint, type Term, type Var, type WhereClause, addSuspendToSubst, aggregateRelFactory, aggregateVar, aggregateVarMulti, alldistincto, and, appendo, arrayToLogicList, baseUnify, branch, chainGoals, collect_and_process_base, collect_distincto, collect_streamo, collecto, conde, conj, cons, count_distincto, count_value_streamo, count_valueo, counto, createEnrichedSubst, createLogicVarProxy, disj, dividebyo, eitherOr, enrichGroupInput, eq, extendSubst, extract, extractEach, fail, failo, filter, firsto, flatMap, fresh, getDefaultLogger, getSuspendsFromSubst, groundo, groupAggregateRelFactory, group_by_collect_distinct_streamo, group_by_collect_streamo, group_by_collecto, group_by_count_streamo, group_by_counto, group_by_streamo_base, gteo, gto, gv1_not, ifte, indexUtils, intersect, isCons, isIndexable, isLogicList, isNil, isVar, lengtho, lift, liftGoal, logicList, logicListToArray, lteo, lto, lvar, makeSuspendHandler, map, mapo, maxo, membero, merge, mino, minuso, multo, neqo, nextGroupId, nil, nonGroundo, not, observable, old_neqo, old_not, once, onceo, or, patternUtils, permuteo, pipe, pluso, project, projectJsonata, query, queryUtils, reduce, removeFirsto, removeSuspendFromSubst, resetVarCounter, resto, run, share, sort_by_streamo, substLog, succeedo, suspendable, take, take_streamo, thruCount, timeout, unificationUtils, unify, unifyWithConstraints, uniqueo, wakeUpSuspends, walk };
