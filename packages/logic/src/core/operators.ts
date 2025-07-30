import { SimpleObservable } from "./observable.js";
import type { Subscription, Observer } from "./types.js";

type ObserverOperator<A, B> = (
  input$: SimpleObservable<A>,
) => SimpleObservable<B>;

export function merge<A, B>(obsB: SimpleObservable<B>) {
  return (obsA: SimpleObservable<A>) =>
    new SimpleObservable<A | B>((observer) => {
      let completed = 0;
      const subscriptions = [
        obsA.subscribe({
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
        obsB.subscribe({
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

export function reduce<V>(
  reducer: (accumulator: V, value: unknown) => V,
  initialValue: unknown,
): ObserverOperator<unknown, V> {
  return (input$: SimpleObservable<unknown>): SimpleObservable<V> =>
    new SimpleObservable<V>((observer) => {
      let value: V = initialValue as V;
      const sub = input$.subscribe({
        next: (v: unknown) => {
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

export function map<T, U>(transform: (value: T) => U) {
  return (input$: SimpleObservable<T>) =>
    new SimpleObservable<U>((observer) => {
      const subscription = input$.subscribe({
        next: (value) => observer.next(transform(value)),
        error: observer.error,
        complete: observer.complete,
      });
      return () => subscription.unsubscribe();
    });
}

export function take<T>(count: number) {
  return (input$: SimpleObservable<T>) =>
    new SimpleObservable<T>((observer) => {
      let taken = 0;
      let upstreamUnsubscribed = false;
      const subscription = input$.subscribe({
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

export function filter<T>(predicate: (value: T) => boolean) {
  return (input$: SimpleObservable<T>) =>
    new SimpleObservable<T>((observer) => {
      const subscription = input$.subscribe({
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

export function flatMap<T, U>(transform: (value: T) => SimpleObservable<U>) {
  return (input$: SimpleObservable<T>) =>
    new SimpleObservable<U>((observer) => {
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

      const outerSubscription = input$.subscribe({
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

export function share<T>(bufferSize: number = Number.POSITIVE_INFINITY) {
  let observers: Observer<T>[] = [];
  let subscription: Subscription | null = null;
  let refCount = 0;
  let completed = false;
  let lastError: any = null;
  const buffer: T[] = [];

  return (input$: SimpleObservable<T>) =>
    new SimpleObservable<T>((observer) => {
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
        subscription = input$.subscribe({
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
