// Simple Observable Implementation
// -----------------------------------------------------------------------------
import {
  merge as mergeOperator,
  reduce as reduceOperator,
  map as mapOperator,
  take as takeOperator,
  filter as filterOperator,
  flatMap as flatMapOperator,
  share as shareOperator,
} from "./operators.js";
import type { Observable, Observer, Subscription } from "./types.js";

const isPromise = (v: any): v is Promise<any> =>
  !!v && typeof v.then === "function";

/**
 * Simple observable implementation focused on the needs of logic programming
 */
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
            value.then((v) => observer.next(v));
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

  // Static factory methods
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
        generator.return?.(undefined as any);
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

  // Operators

  // Utility to collect all values into an array
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

  pipe<V>(
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

export type ObsToObs = (input: SimpleObservable<any>) => SimpleObservable<any>;
// Export the factory function for convenience
export const observable = <T>(
  producer: (observer: Observer<T>) => (() => void) | undefined,
) => new SimpleObservable(producer);
