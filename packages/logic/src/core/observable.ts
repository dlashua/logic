import {
  filter as filterOperator,
  flatMap as flatMapOperator,
  map as mapOperator,
  merge as mergeOperator,
  reduce as reduceOperator,
  share as shareOperator,
  take as takeOperator,
} from "./operators.js";
import type { Observable, Observer, Subscription } from "./types.js";

export type OperatorFunction<T, R> = (
  source: SimpleObservable<T>,
) => SimpleObservable<R>;

const isPromise = (v: unknown): v is Promise<unknown> =>
  !!v && typeof (v as { then?: unknown }).then === "function";

export class SimpleObservable<T> implements Observable<T> {
  private producer: (
    observer: Observer<T>,
  ) => (() => void) | void | Promise<(() => void) | undefined>;

  constructor(
    producer: (
      observer: Observer<T>,
    ) => (() => void) | void | Promise<(() => void) | undefined>,
  ) {
    this.producer = producer;
  }

  subscribe(observer: Observer<T>): Subscription {
    let closed = false;
    let cleanup: (() => void) | undefined = () =>
      console.log("I HAVE NO CLEANUP");

    const safeObserver = {
      next: (value: T) => {
        if (!closed && observer.next) {
          if (isPromise(value)) {
            value.then((v) => observer.next(v as T));
          } else {
            observer.next(value);
          }
        }
      },
      error: (error: Error) => {
        if (!closed && observer.error) {
          observer.error(error);
          closed = true;
        }
      },
      complete: () => {
        if (!closed && observer.complete) {
          observer.complete();
          closed = true;
        }
      },
    };

    try {
      const result = this.producer(safeObserver);

      if (result && typeof result === "object" && "then" in result) {
        result
          .then((asyncCleanup) => {
            cleanup = asyncCleanup;
            if (unsubbed && cleanup) {
              cleanup();
            }
          })
          .catch((error) => {
            safeObserver.error(
              error instanceof Error ? error : new Error(String(error)),
            );
          });
      } else {
        cleanup = result as (() => void) | undefined;
      }
    } catch (error) {
      if (!closed) {
        safeObserver.error(
          error instanceof Error ? error : new Error(String(error)),
        );
      }
    }

    let unsubbed = false;
    return {
      unsubscribe: () => {
        if (!unsubbed) {
          closed = true;
          unsubbed = true;
          if (cleanup) {
            cleanup();
          }
        }
      },
      get closed() {
        return closed;
      },
    };
  }

  static of<T>(...values: T[]): SimpleObservable<T> {
    return new SimpleObservable<T>((observer) => {
      for (const value of values) {
        observer.next(value);
      }
      observer.complete?.();
    });
  }

  static from<T>(values: T[]): SimpleObservable<T> {
    return SimpleObservable.of(...values);
  }

  static empty<T>(): SimpleObservable<T> {
    return new SimpleObservable<T>((observer) => {
      observer.complete?.();
    });
  }

  static fromAsyncGenerator<T>(generator: AsyncGenerator<T>): Observable<T> {
    return new SimpleObservable<T>((observer) => {
      let cancelled = false;

      (async () => {
        try {
          for await (const value of generator) {
            if (cancelled) break;
            observer.next(value);
          }
          if (!cancelled) {
            observer.complete?.();
          }
        } catch (error) {
          if (!cancelled) {
            observer.error?.(error);
          }
        }
      })();

      return () => {
        cancelled = true;
        generator.return?.(undefined as unknown);
      };
    });
  }

  static fromPromise<T>(promise: Promise<T>): Observable<T> {
    return new SimpleObservable<T>((observer) => {
      promise
        .then((value) => {
          observer.next(value);
          observer.complete?.();
        })
        .catch((error) => observer.error?.(error));
    });
  }

  toArray(): Promise<T[]> {
    let sub: Subscription;
    return new Promise<T[]>((resolve, reject) => {
      const values: T[] = [];
      sub = this.subscribe({
        next: (value) => values.push(value),
        error: reject,
        complete: () => {
          setTimeout(() => sub.unsubscribe(), 0);
          resolve(values);
        },
      });
    });
  }

  firstFrom(): Promise<T> {
    let sub: Subscription;
    let settled = false;
    return new Promise<T>((resolve, reject) => {
      sub = this.subscribe({
        next: (value) => {
          settled = true;
          resolve(value);
          setTimeout(() => sub.unsubscribe(), 0);
        },
        error: (e) => {
          settled = true;
          reject(e);
        },
        complete: () => {
          if (!settled) {
            reject(new Error("NO_VALUE_EMITTED"));
          }
          setTimeout(() => sub.unsubscribe(), 0);
        },
      });
    });
  }

  lift<V>(
    next_observable: (input$: SimpleObservable<T>) => SimpleObservable<V>,
  ): SimpleObservable<V> {
    return next_observable(this);
  }

  lastFrom(): Promise<T> {
    let sub: Subscription;
    let settled = false;
    let valueRecevied = false;
    let finalValue: T;
    return new Promise<T>((resolve, reject) => {
      sub = this.subscribe({
        next: (value) => {
          valueRecevied = true;
          finalValue = value;
        },
        error: (e) => {
          settled = true;
          reject(e);
        },
        complete: () => {
          if (!settled) {
            if (valueRecevied) {
              settled = true;
              resolve(finalValue);
            } else {
              settled = true;
              reject(new Error("NO_VALUE_EMITTED"));
            }
          }
          setTimeout(() => sub.unsubscribe(), 0);
        },
      });
    });
  }

  // Fluent Operators
  filter(predicate: (value: T) => boolean) {
    return filterOperator(predicate)(this);
  }

  flatMap<U>(transform: (value: T) => SimpleObservable<U>) {
    return flatMapOperator(transform)(this);
  }

  map<U>(transform: (value: T) => U) {
    return mapOperator(transform)(this);
  }

  merge<R>(other: SimpleObservable<R>): SimpleObservable<T | R> {
    return mergeOperator<T, R>(other)(this);
  }

  pipe(): SimpleObservable<T>;
  pipe<A>(op1: OperatorFunction<T, A>): SimpleObservable<A>;
  pipe<A, B>(
    op1: OperatorFunction<T, A>,
    op2: OperatorFunction<A, B>,
  ): SimpleObservable<B>;
  pipe<A, B, C>(
    op1: OperatorFunction<T, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
  ): SimpleObservable<C>;
  pipe<A, B, C, D>(
    op1: OperatorFunction<T, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
    op4: OperatorFunction<C, D>,
  ): SimpleObservable<D>;
  pipe<A, B, C, D, E>(
    op1: OperatorFunction<T, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
    op4: OperatorFunction<C, D>,
    op5: OperatorFunction<D, E>,
  ): SimpleObservable<E>;
  pipe<A, B, C, D, E, F>(
    op1: OperatorFunction<T, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
    op4: OperatorFunction<C, D>,
    op5: OperatorFunction<D, E>,
    op6: OperatorFunction<E, F>,
  ): SimpleObservable<F>;
  pipe<A, B, C, D, E, F, G>(
    op1: OperatorFunction<T, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
    op4: OperatorFunction<C, D>,
    op5: OperatorFunction<D, E>,
    op6: OperatorFunction<E, F>,
    op7: OperatorFunction<F, G>,
  ): SimpleObservable<G>;
  pipe<A, B, C, D, E, F, G, H>(
    op1: OperatorFunction<T, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
    op4: OperatorFunction<C, D>,
    op5: OperatorFunction<D, E>,
    op6: OperatorFunction<E, F>,
    op7: OperatorFunction<F, G>,
    op8: OperatorFunction<G, H>,
  ): SimpleObservable<H>;
  pipe<A, B, C, D, E, F, G, H, I>(
    op1: OperatorFunction<T, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
    op4: OperatorFunction<C, D>,
    op5: OperatorFunction<D, E>,
    op6: OperatorFunction<E, F>,
    op7: OperatorFunction<F, G>,
    op8: OperatorFunction<G, H>,
    op9: OperatorFunction<H, I>,
  ): SimpleObservable<I>;
  // biome-ignore lint/suspicious/noExplicitAny: <unknown type produces bad DX>
  pipe(...operators: OperatorFunction<any, any>[]): SimpleObservable<any> {
    return operators.length === 0
      ? this
      : // biome-ignore lint/suspicious/noExplicitAny: <unknown type produces bad DX>
        (operators as Array<OperatorFunction<any, any>>).reduce<
          // biome-ignore lint/suspicious/noExplicitAny: <unknown type produces bad DX>
          SimpleObservable<any>
        >((prev$, op) => op(prev$), this);
  }

  reduce<Q>(
    reducer: (accumulator: Q, value: unknown) => Q,
    initalValue: unknown,
  ) {
    return reduceOperator<Q>(reducer, initalValue)(this);
  }

  share(bufferSize: number = Number.POSITIVE_INFINITY) {
    return shareOperator<T>(bufferSize)(this);
  }

  take(count: number) {
    return takeOperator<T>(count)(this);
  }
}

export const observable = <T>(
  producer: (observer: Observer<T>) => (() => void) | undefined,
) => new SimpleObservable(producer);
