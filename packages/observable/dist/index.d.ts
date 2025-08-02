import { Observable, of, empty } from 'rxjs';
export { Observable, Observer, Subscribable, Subscriber, Subscription, empty, flatMap, map, of, reduce, share, take } from 'rxjs';

declare class SimpleObservable<T> extends Observable<T> {
    static of: typeof of;
    static empty: typeof empty;
}

export { SimpleObservable };
