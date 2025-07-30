// Observable-based Core Types for Logic Programming
// -----------------------------------------------------------------------------

import type { SimpleObservable } from "./observable.js";

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
export type Term<T = unknown> =
  | Var
  | LogicList
  | T
  | Term<T>[]
  | null
  | undefined;

/**
 * Observable interface for lazy evaluation and backpressure control
 */
export interface Observable<T> {
  subscribe(observer: Observer<T>): Subscription;
}

/**
 * Observer interface for consuming observable streams
 */
export interface Observer<T> {
  next(value: T): void;
  error?(error: any): void;
  complete?(): void;
}

/**
 * Subscription interface for managing observable lifecycle
 */
export interface Subscription {
  unsubscribe(): void;
  readonly closed: boolean;
}

/**
 * A Goal is a function that takes an Observable stream of substitutions and returns
 * an Observable stream of possible resulting substitutions.
 */
export type Goal = (input$: SimpleObservable<Subst>) => SimpleObservable<Subst>;

/**
 * The shape of a single result from a query.
 */
export type RunResult<Fmt> = {
  [K in keyof Fmt]: Term;
};

/**
 * Type for lifted function arguments
 */
export type LiftableFunction<T> = (...args: unknown[]) => T;

/**
 * Type for lifted function arguments
 */
export type LiftedArgs<T extends LiftableFunction<U>, U> = T extends (
  ...args: infer A
) => U
  ? (...args: [...{ [I in keyof A]: Term<A[I]> | A[I] }, out: Term<U>]) => Goal
  : never;

/**
 * Stream configuration for controlling evaluation behavior
 */
export interface StreamConfig {
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
export interface StreamResult<T> {
  values: T[];
  completed: boolean;
  error?: any;
}
