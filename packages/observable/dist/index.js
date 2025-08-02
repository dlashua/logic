// src/index.ts
import { Observable, of, map, flatMap, empty, take, share } from "rxjs";
import {
  reduce,
  Subscriber,
  Subscription
} from "rxjs";
var SimpleObservable = class extends Observable {
  static of = of;
  static empty = empty;
};
export {
  Observable,
  SimpleObservable,
  Subscriber,
  Subscription,
  empty,
  flatMap,
  map,
  of,
  reduce,
  share,
  take
};
//# sourceMappingURL=index.js.map