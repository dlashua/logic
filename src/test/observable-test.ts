// @ts-expect-error
import { setTimeout as sleep } from "timers/promises";
import { SimpleObservable } from "../core/observable.ts";
import type { Subscription } from "../core/types.ts";

async function dumpObsArray(name: string, obs: SimpleObservable<any>) {
	console.log(name, "-----");
	for (const row of await obs.toArray()) {
		console.log(row);
	}
	console.log("-----");
}

function dumpObsSub(name: string, obs: SimpleObservable<any>) {
	let sub: Subscription;
	return new Promise((resolve, reject) => {
		console.log(name, "-----");
		sub = obs.subscribe({
			next: (row) => console.log(row),
			error: (e) => reject(e),
			complete: () => {
				console.log("-----");
				resolve(null);
			},
		});
	}).then(() => sub.unsubscribe());
}

async function dumpObsSubAsync(name: string, obs: SimpleObservable<any>) {
	let sub: Subscription;
	return new Promise((resolve, reject) => {
		console.log(name, "-----");
		sub = obs.subscribe({
			next: async (row) => sleep(10).then(() => console.log(row)),
			error: (e) => reject(e),
			complete: () => {
				console.log("-----");
				resolve(null);
			},
		});
	}).then(() => sub.unsubscribe());
}

const dumpObs = dumpObsArray;

let boId = 0;
const BaseObservable = new SimpleObservable<number>((observer) => {
	const myId = ++boId;
	console.log("NEW SUB", myId);
	let cancelled = false;

	async function action() {
		for (let i = 1; i < 10; i++) {
			if (cancelled) break;
			console.log("MADE S", i);
			observer.next(i);
		}
	}

	action().finally(() => observer.complete?.());

	return () => {
		console.log("UNSUB", myId);
		cancelled = true;
	};
});

const BaseAsyncObservable = new SimpleObservable<number>(async (observer) => {
	const myId = ++boId;

	console.log("NEW ASYNC SUB", myId);
	let cancelled = false;
	async function action() {
		for (let i = 1; i < 10; i++) {
			if (cancelled) return;
			console.log("MADE A", i);
			observer.next(i);
			await new Promise((resolve) => setTimeout(resolve, 1));
		}
		observer.complete?.();
	}

	action();

	return () => {
		console.log("ASYNC UNSUB", myId);
		cancelled = true;
	};
});

await dumpObs("base", BaseObservable);

await dumpObs(
	"map 1",
	BaseObservable.map((x) => x + 5),
);
await dumpObs(
	"map array",
	BaseObservable.map((x) => [x + 5, x + 105]),
);

await dumpObs(
	"filter",
	BaseObservable.filter((x) => x % 2 === 0),
);

await dumpObs("take 3", BaseObservable.take(3));

const shared = BaseObservable.share();
await dumpObs("share 1 take", shared.take(3));
await dumpObs(
	"share 2 filter",
	shared.filter((x) => x % 2 === 0),
);
await dumpObs("share 3 take", shared.take(3));

await dumpObs(
	"flatmap 1",
	BaseObservable.flatMap((x) => SimpleObservable.of(x + 5)),
);
await dumpObs(
	"flatmap 2",
	BaseObservable.flatMap((x) => SimpleObservable.of(x + 5, x + 105)),
);
await dumpObs(
	"flatmap array",
	BaseObservable.flatMap((x) => SimpleObservable.of([x + 5, x + 105])),
);

await dumpObs("merge", BaseObservable.merge(BaseObservable));

await dumpObs("base async", BaseAsyncObservable);
await dumpObs("async take 3", BaseAsyncObservable.take(3));
await dumpObs(
	"async filter",
	BaseAsyncObservable.filter((x) => x % 2 === 0),
);

await dumpObs("merge sync + async", BaseObservable.merge(BaseAsyncObservable));
await dumpObs("merge async + sync", BaseAsyncObservable.merge(BaseObservable));

function slowMap(obs: SimpleObservable<number>) {
	return new SimpleObservable<number>((observer) => {
		let inProgress = 0;
		let completed = false;
		const sub = obs.subscribe({
			next: async (v) => {
				inProgress++;
				await sleep(15);
				observer.next(v + 1000);

				inProgress--;
				if (completed && inProgress === 0) {
					observer.complete?.();
				}
			},
			complete: () => {
				completed = true;
				if (inProgress === 0) {
					observer.complete?.();
				}
			},
			error: observer.error,
		});

		return () => sub.unsubscribe();
	});
}

await dumpObs("slow map", slowMap(slowMap(slowMap(BaseObservable))));
await dumpObs("async slow map", slowMap(slowMap(slowMap(BaseAsyncObservable))));
