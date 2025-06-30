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

/**
 * The shape of a single result from a query.
 */
export type RunResult<Fmt> = {
  [K in keyof Fmt]: Term;
};

export type TermedArgs<T extends (...args: any) => any> = T extends (
  ...args: infer A
) => infer R
  ? (...args: [...{ [I in keyof A]: Term<A[I]> | A[I] }, out: Term<R>]) => Goal
  : never;
