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
