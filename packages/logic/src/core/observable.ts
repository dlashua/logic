// Simple Observable Implementation
// -----------------------------------------------------------------------------

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

  static fromAsync<T>(
    asyncProducer: (observer: Observer<T>) => Promise<(() => void) | undefined>,
  ): Observable<T> {
    return new SimpleObservable<T>(asyncProducer);
  }

  // Operators
  map<U>(transform: (value: T) => U): SimpleObservable<U> {
    return new SimpleObservable<U>((observer) => {
      const subscription = this.subscribe({
        next: (value) => observer.next(transform(value)),
        error: observer.error,
        complete: observer.complete,
      });
      return () => subscription.unsubscribe();
    });
  }

  flatMap<U>(
    transform: (value: T) => SimpleObservable<U>,
  ): SimpleObservable<U> {
    return new SimpleObservable<U>((observer) => {
      const subscriptions: Subscription[] = [];
      let outerCompleted = false;
      let activeInnerCount = 0;
      let scheduledCompletion = false;
      let fullyComplete = false;

      const checkCompletion = () => {
        if (outerCompleted && activeInnerCount === 0 && !scheduledCompletion) {
          scheduledCompletion = true;
          setTimeout(() => {
            observer.complete?.();
            fullyComplete = true;
          }, 0);
        }
      };

      const outerSubscription = this.subscribe({
        next: (value) => {
          activeInnerCount++;
          const innerObservable = transform(value);
          const innerSubscription = innerObservable.subscribe({
            next: observer.next,
            error: observer.error,
            complete: () => {
              activeInnerCount--;
              checkCompletion();
            },
          });
          subscriptions.push(innerSubscription);
        },
        error: observer.error,
        complete: () => {
          outerCompleted = true;
          // Always defer completion check to next microtask
          setTimeout(() => checkCompletion(), 0);
        },
      });

      subscriptions.push(outerSubscription);

      return () => {
        if (!fullyComplete) {
          subscriptions.forEach((sub) => sub.unsubscribe());
        }
      };
    });
  }

  filter(predicate: (value: T) => boolean): SimpleObservable<T> {
    return new SimpleObservable<T>((observer) => {
      const subscription = this.subscribe({
        next: (value) => {
          if (predicate(value)) {
            observer.next(value);
          }
        },
        error: observer.error,
        complete: observer.complete,
      });
      return () => subscription.unsubscribe();
    });
  }

  take(count: number): SimpleObservable<T> {
    return new SimpleObservable<T>((observer) => {
      let taken = 0;
      let upstreamUnsubscribed = false;
      const subscription = this.subscribe({
        next: (value) => {
          if (taken < count) {
            observer.next(value);
            taken++;
            if (taken >= count) {
              observer.complete?.();
              setTimeout(() => {
                subscription.unsubscribe();
                upstreamUnsubscribed = true;
              }, 0);
            }
          }
        },
        error: (err) => {
          observer.error?.(err);
        },
        complete: () => {
          observer.complete?.();
        },
      });
      return () => {
        if (!upstreamUnsubscribed) {
          subscription.unsubscribe();
        }
        upstreamUnsubscribed = true;
      };
    });
  }

  reduce<V>(
    reducer: (accumulator: V, value: T) => V,
    initialValue: V,
  ): SimpleObservable<V> {
    return new SimpleObservable<V>((observer) => {
      let value: V = initialValue;
      const sub = this.subscribe({
        next: (v: T) => {
          value = reducer(value, v);
        },
        complete: () => {
          observer.next(value);
          observer.complete?.();
        },
        error: (e) => observer.error?.(e),
      });

      return () => sub.unsubscribe();
    });
  }

  share(bufferSize: number = Number.POSITIVE_INFINITY): SimpleObservable<T> {
    let observers: Observer<T>[] = [];
    let subscription: Subscription | null = null;
    let refCount = 0;
    let completed = false;
    let lastError: any = null;
    const buffer: T[] = [];

    return new SimpleObservable<T>((observer) => {
      // Replay all buffered values to new subscribers (logic programming needs deterministic behavior)
      buffer.forEach((value) => observer.next?.(value));

      if (completed) {
        if (lastError !== null) {
          observer.error?.(lastError);
        } else {
          observer.complete?.();
        }
        return;
      }

      observers.push(observer);
      refCount++;

      if (subscription === null) {
        subscription = this.subscribe({
          next: (value) => {
            buffer.push(value);
            // Limit buffer size if specified (for memory optimization)
            if (buffer.length > bufferSize) {
              buffer.shift();
            }
            observers.slice().forEach((o) => o.next?.(value));
          },
          error: (err) => {
            lastError = err;
            completed = true;
            observers.slice().forEach((o) => o.error?.(err));
            observers = [];
          },
          complete: () => {
            completed = true;
            observers.slice().forEach((o) => o.complete?.());
            observers = [];
          },
        });
      }

      return () => {
        observers = observers.filter((o) => o !== observer);
        refCount--;
        if (refCount === 0 && subscription) {
          subscription.unsubscribe();
          subscription = null;
          completed = false;
          lastError = null;
          buffer.length = 0; // Clean up buffer when no more subscribers
        }
      };
    });
  }

  merge(other: SimpleObservable<T>): SimpleObservable<T> {
    return new SimpleObservable<T>((observer) => {
      let completed = 0;
      const subscriptions = [
        this.subscribe({
          next: (r) => {
            observer.next(r);
          },
          error: observer.error,
          complete: () => {
            completed++;
            if (completed === 2) {
              observer.complete?.();
            }
          },
        }),
        other.subscribe({
          next: (r) => {
            observer.next(r);
          },
          error: observer.error,
          complete: () => {
            completed++;
            if (completed === 2) {
              observer.complete?.();
            }
          },
        }),
      ];

      return () => {
        subscriptions.forEach((sub) => sub.unsubscribe());
      };
    });
  }

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
}

export type ObsToObs = (input: SimpleObservable<any>) => SimpleObservable<any>;
// Export the factory function for convenience
export const observable = <T>(
  producer: (observer: Observer<T>) => (() => void) | undefined,
) => new SimpleObservable(producer);
