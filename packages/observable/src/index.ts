// export * from "./observable.js";
// export * from "./operators.js";
// export * from "./types.js";
import { Observable, of, map, flatMap, empty, take, share } from "rxjs";
export {
  type Observer,
  reduce,
  type Subscribable,
  Subscriber,
  Subscription,
} from "rxjs";

export { Observable, of, map, flatMap, empty, take, share };

export class SimpleObservable<T> extends Observable<T> {
  static of = of;
  static empty = empty;
}
