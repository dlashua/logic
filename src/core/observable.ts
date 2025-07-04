// Simple Observable Implementation
// -----------------------------------------------------------------------------

import { Observable, Observer, Subscription } from './types.ts';

/**
 * Simple observable implementation focused on the needs of logic programming
 */
export class SimpleObservable<T> implements Observable<T> {
  private producer: (observer: Observer<T>) => (() => void) | void | Promise<(() => void) | void>;

  constructor(producer: (observer: Observer<T>) => (() => void) | void | Promise<(() => void) | void>) {
    this.producer = producer;
  }

  subscribe(observer: Observer<T>): Subscription {
    let closed = false;
    let cleanup: (() => void) | void;

    const safeObserver = {
      next: (value: T) => {
        if (!closed && observer.next) {
          observer.next(value);
        }
      },
      error: (error: any) => {
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
      }
    };

    try {
      const result = this.producer(safeObserver);
      
      // Handle both sync and async producers
      if (result && typeof result === 'object' && 'then' in result) {
        // Async producer
        result.then(asyncCleanup => {
          if (!closed) {
            cleanup = asyncCleanup;
          }
        }).catch(error => {
          if (!closed) {
            safeObserver.error(error);
          }
        });
      } else {
        // Sync producer
        cleanup = result as (() => void) | void;
      }
    } catch (error) {
      if (!closed) {
        safeObserver.error(error);
      }
    }

    return {
      unsubscribe: () => {
        if (!closed) {
          closed = true;
          if (cleanup) {
            cleanup();
          }
        }
      },
      get closed() {
        return closed;
      }
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
        .then(value => {
          observer.next(value);
          observer.complete?.();
        })
        .catch(error => observer.error?.(error));
    });
  }

  static fromAsync<T>(asyncProducer: (observer: Observer<T>) => Promise<(() => void) | void>): Observable<T> {
    return new SimpleObservable<T>(asyncProducer);
  }

  // Operators
  map<U>(transform: (value: T) => U): Observable<U> {
    return new SimpleObservable<U>((observer) => {
      const subscription = this.subscribe({
        next: (value) => observer.next(transform(value)),
        error: observer.error,
        complete: observer.complete
      });
      return () => subscription.unsubscribe();
    });
  }

  flatMap<U>(transform: (value: T) => SimpleObservable<U>): SimpleObservable<U> {
    return new SimpleObservable<U>((observer) => {
      const subscriptions: Subscription[] = [];
      let outerCompleted = false;
      let activeInnerCount = 0;

      const checkCompletion = () => {
        if (outerCompleted && activeInnerCount === 0) {
          observer.complete?.();
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
            }
          });
          subscriptions.push(innerSubscription);
        },
        error: observer.error,
        complete: () => {
          outerCompleted = true;
          checkCompletion();
        }
      });

      subscriptions.push(outerSubscription);

      return () => {
        subscriptions.forEach(sub => sub.unsubscribe());
      };
    });
  }

  filter(predicate: (value: T) => boolean): Observable<T> {
    return new SimpleObservable<T>((observer) => {
      const subscription = this.subscribe({
        next: (value) => {
          if (predicate(value)) {
            observer.next(value);
          }
        },
        error: observer.error,
        complete: observer.complete
      });
      return () => subscription.unsubscribe();
    });
  }

  take(count: number): SimpleObservable<T> {
    return new SimpleObservable<T>((observer) => {
      let taken = 0;
      const subscription = this.subscribe({
        next: (value) => {
          if (taken < count) {
            observer.next(value);
            taken++;
            if (taken >= count) {
              observer.complete?.();
              subscription.unsubscribe();
            }
          }
        },
        error: observer.error,
        complete: observer.complete
      });
      return () => subscription.unsubscribe();
    });
  }

  merge(other: SimpleObservable<T>): SimpleObservable<T> {
    return new SimpleObservable<T>((observer) => {
      let completed = 0;
      const subscriptions = [
        this.subscribe({
          next: observer.next,
          error: observer.error,
          complete: () => {
            completed++;
            if (completed === 2) {
              observer.complete?.();
            }
          }
        }),
        other.subscribe({
          next: observer.next,
          error: observer.error,
          complete: () => {
            completed++;
            if (completed === 2) {
              observer.complete?.();
            }
          }
        })
      ];

      return () => {
        subscriptions.forEach(sub => sub.unsubscribe());
      };
    });
  }

  // Utility to collect all values into an array
  toArray(): Promise<T[]> {
    return new Promise((resolve, reject) => {
      const values: T[] = [];
      this.subscribe({
        next: (value) => values.push(value),
        error: reject,
        complete: () => resolve(values)
      });
    });
  }
}

// Export the factory function for convenience
export const observable = <T>(producer: (observer: Observer<T>) => (() => void) | void) => 
  new SimpleObservable(producer);