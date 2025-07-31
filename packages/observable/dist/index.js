// src/operators.ts
function merge(obsB) {
  return (obsA) => new SimpleObservable((observer) => {
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
        }
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
        }
      })
    ];
    return () => {
      subscriptions.forEach((sub) => sub.unsubscribe());
    };
  });
}
function reduce(reducer, initialValue) {
  return (input$) => new SimpleObservable((observer) => {
    let value = initialValue;
    const sub = input$.subscribe({
      next: (v) => {
        value = reducer(value, v);
      },
      complete: () => {
        observer.next(value);
        observer.complete?.();
      },
      error: (e) => observer.error?.(e)
    });
    return () => sub.unsubscribe();
  });
}
function map(transform) {
  return (input$) => new SimpleObservable((observer) => {
    const subscription = input$.subscribe({
      next: (value) => observer.next(transform(value)),
      error: observer.error,
      complete: observer.complete
    });
    return () => subscription.unsubscribe();
  });
}
function take(count) {
  return (input$) => new SimpleObservable((observer) => {
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
      }
    });
    return () => {
      if (!upstreamUnsubscribed) {
        subscription.unsubscribe();
      }
      upstreamUnsubscribed = true;
    };
  });
}
function filter(predicate) {
  return (input$) => new SimpleObservable((observer) => {
    const subscription = input$.subscribe({
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
function flatMap(transform) {
  return (input$) => new SimpleObservable((observer) => {
    const subscriptions = [];
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
          }
        });
        subscriptions.push(innerSubscription);
      },
      error: observer.error,
      complete: () => {
        outerCompleted = true;
        setTimeout(() => checkCompletion(), 0);
      }
    });
    subscriptions.push(outerSubscription);
    return () => {
      if (!fullyComplete) {
        subscriptions.forEach((sub) => sub.unsubscribe());
      }
    };
  });
}
function share(bufferSize = Number.POSITIVE_INFINITY) {
  let observers = [];
  let subscription = null;
  let refCount = 0;
  let completed = false;
  let lastError = null;
  const buffer = [];
  return (input$) => new SimpleObservable((observer) => {
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
        }
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
        buffer.length = 0;
      }
    };
  });
}
function pipe(...operators) {
  return (input$) => operators.reduce((prev$, op) => op(prev$), input$);
}

// src/observable.ts
var isPromise = (v) => !!v && typeof v.then === "function";
var SimpleObservable = class _SimpleObservable {
  producer;
  constructor(producer) {
    this.producer = producer;
  }
  subscribe(observer) {
    let closed = false;
    let cleanup = () => console.log("I HAVE NO CLEANUP");
    const safeObserver = {
      next: (value) => {
        if (!closed && observer.next) {
          if (isPromise(value)) {
            value.then((v) => observer.next(v));
          } else {
            observer.next(value);
          }
        }
      },
      error: (error) => {
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
      if (result && typeof result === "object" && "then" in result) {
        result.then((asyncCleanup) => {
          cleanup = asyncCleanup;
          if (unsubbed && cleanup) {
            cleanup();
          }
        }).catch((error) => {
          safeObserver.error(
            error instanceof Error ? error : new Error(String(error))
          );
        });
      } else {
        cleanup = result;
      }
    } catch (error) {
      if (!closed) {
        safeObserver.error(
          error instanceof Error ? error : new Error(String(error))
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
      }
    };
  }
  static of(...values) {
    return new _SimpleObservable((observer) => {
      for (const value of values) {
        observer.next(value);
      }
      observer.complete?.();
    });
  }
  static from(values) {
    return _SimpleObservable.of(...values);
  }
  static empty() {
    return new _SimpleObservable((observer) => {
      observer.complete?.();
    });
  }
  static fromAsyncGenerator(generator) {
    return new _SimpleObservable((observer) => {
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
        generator.return?.(void 0);
      };
    });
  }
  static fromPromise(promise) {
    return new _SimpleObservable((observer) => {
      promise.then((value) => {
        observer.next(value);
        observer.complete?.();
      }).catch((error) => observer.error?.(error));
    });
  }
  toArray() {
    let sub;
    return new Promise((resolve, reject) => {
      const values = [];
      sub = this.subscribe({
        next: (value) => values.push(value),
        error: reject,
        complete: () => {
          setTimeout(() => sub.unsubscribe(), 0);
          resolve(values);
        }
      });
    });
  }
  firstFrom() {
    let sub;
    let settled = false;
    return new Promise((resolve, reject) => {
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
        }
      });
    });
  }
  lift(next_observable) {
    return next_observable(this);
  }
  lastFrom() {
    let sub;
    let settled = false;
    let valueRecevied = false;
    let finalValue;
    return new Promise((resolve, reject) => {
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
        }
      });
    });
  }
  // Fluent Operators
  filter(predicate) {
    return filter(predicate)(this);
  }
  flatMap(transform) {
    return flatMap(transform)(this);
  }
  map(transform) {
    return map(transform)(this);
  }
  merge(other) {
    return merge(other)(this);
  }
  // biome-ignore lint/suspicious/noExplicitAny: <unknown type produces bad DX>
  pipe(...operators) {
    return operators.length === 0 ? this : (
      // biome-ignore lint/suspicious/noExplicitAny: <unknown type produces bad DX>
      operators.reduce((prev$, op) => op(prev$), this)
    );
  }
  reduce(reducer, initalValue) {
    return reduce(reducer, initalValue)(this);
  }
  share(bufferSize = Number.POSITIVE_INFINITY) {
    return share(bufferSize)(this);
  }
  take(count) {
    return take(count)(this);
  }
};
var observable = (producer) => new SimpleObservable(producer);
export {
  SimpleObservable,
  filter,
  flatMap,
  map,
  merge,
  observable,
  pipe,
  reduce,
  share,
  take
};
//# sourceMappingURL=index.js.map