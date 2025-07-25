import { SimpleObservable } from "../core/observable.ts";

const START = 0;
const COUNT = 100000000;
const isEven = (x) => x % 2 == 0;
const double = (x) => x * 2;


function myRangeObs(start, count) {
  const end = count + start;
  return new SimpleObservable(
    (subscriber) => {
      let n = start;
      while (n < end) {
        subscriber.next(n++);
      }
      subscriber.complete?.();
    }
  );
}

function goObs() {
  const start = Date.now();

  const I$ = myRangeObs(START, COUNT).filter(isEven).map(double);
  let cnt = 0;
  I$.subscribe({
    next: (v) => cnt++,
    complete: () => {
      const elapsed = Date.now() - start;

      console.log("obs", cnt, elapsed)
    }
  });
}
goObs();