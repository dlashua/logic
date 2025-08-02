import { pipe, SimpleObservable } from "@codespiral/observable";
import * as RX from "rxjs";

const RXdoubleMap =
  () =>
  (input$: RX.Observable<number>): RX.Observable<number> =>
    new RX.Observable((observer) => {
      const sub = input$.subscribe({
        next: (v) => observer.next(v * 2),
        complete: () => observer.complete(),
        error: (e: Error) => observer.error(e),
      });

      return () => sub.unsubscribe();
    });

const RXdoubleMapToString =
  () =>
  (input$: RX.Observable<number>): RX.Observable<string> =>
    new RX.Observable((observer) => {
      const sub = input$.subscribe({
        next: (v) => observer.next(String(v * 2)),
        complete: () => observer.complete(),
        error: (e: Error) => observer.error(e),
      });

      return () => sub.unsubscribe();
    });

const RXaddMsg =
  (msg: string) =>
  (input$: RX.Observable<string>): RX.Observable<string> =>
    new RX.Observable((observer) => {
      const sub = input$.subscribe({
        next: (v) => observer.next(`${msg} ${v}`),
        complete: () => observer.complete(),
        error: (e: Error) => observer.error(e),
      });

      return () => sub.unsubscribe();
    });

const doubleMap =
  () =>
  (input$: SimpleObservable<number>): SimpleObservable<number> =>
    new SimpleObservable((observer) => {
      const sub = input$.subscribe({
        next: (v) => observer.next(v * 2),
        complete: () => observer.complete(),
        error: (e: Error) => observer.error(e),
      });

      return () => sub.unsubscribe();
    });

const doubleMapToString =
  () =>
  (input$: SimpleObservable<number>): SimpleObservable<string> =>
    new SimpleObservable((observer) => {
      const sub = input$.subscribe({
        next: (v) => observer.next(String(v * 2)),
        complete: () => observer.complete(),
        error: (e: Error) => observer.error(e),
      });

      return () => sub.unsubscribe();
    });

const addMsg =
  (msg: string) =>
  (input$: SimpleObservable<string>): SimpleObservable<string> =>
    new SimpleObservable((observer) => {
      const sub = input$.subscribe({
        next: (v) => observer.next(`${msg} ${v}`),
        complete: () => observer.complete(),
        error: (e: Error) => observer.error(e),
      });

      return () => sub.unsubscribe();
    });

const obs = SimpleObservable.of<number>(1, 2, 3, 4, 5).pipe(
  doubleMap(),
  doubleMap(),
  doubleMap(),
  doubleMapToString(),
  doubleMap(),
  addMsg("test 123"),
  addMsg("more"),
);

console.log(await obs.toArray());

const testPipe = pipe(
  doubleMap(),
  doubleMap(),
  doubleMap(),
  doubleMapToString(),
  doubleMap(),
  addMsg("test 123"),
  addMsg("more"),
);

const obs2 = SimpleObservable.of<number>(1, 2, 3, 4, 5);

console.log(await testPipe(obs2).toArray());

const rxobs = RX.of(1, 2, 3, 4, 5).pipe(
  RXdoubleMap(),
  RXdoubleMap(),
  RXdoubleMap(),
  RXdoubleMapToString(),
  RXdoubleMap(),
  RXaddMsg("test 123"),
  RXaddMsg("more"),
);
