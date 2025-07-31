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

export { type Observable, type Observer, type OperatorFunction, SimpleObservable, type Subscription, filter, flatMap, map, merge, observable, pipe, reduce, share, take };
