import { walk } from "../core/kernel.js";
import { SimpleObservable } from "../core/observable.js";
import type { Goal, Subst, Term, Var } from "../core/types.js";

/**
 * Aggregates all possible values of a logic variable into an array and binds to sourceVar in a single solution.
 */
export function aggregateVar(sourceVar: Var, subgoal: Goal): Goal {
	return (input$) =>
		new SimpleObservable((observer) => {
			let active = 0;
			let completed = false;
			const subscription = input$.subscribe({
				next: (s) => {
					active++;
					const results: Term[] = [];
					let subgoalEmitted = false;
					subgoal(SimpleObservable.of(s)).subscribe({
						next: (subst) => {
							subgoalEmitted = true;
							results.push(walk(sourceVar, subst));
						},
						error: observer.error,
						complete: () => {
							const s2 = new Map(s);
							s2.set(sourceVar.id, results);
							observer.next(s2);
							active--;
							if (completed && active === 0) observer.complete?.();
						},
					});
				},
				error: observer.error,
				complete: () => {
					completed = true;
					if (active === 0) observer.complete?.();
				},
			});
			return () => subscription.unsubscribe?.();
		});
}

/**
 * For each unique combination of groupVars, aggregate all values of each aggVar in aggVars, and yield a substitution with arrays bound to each aggVar.
 */
export function aggregateVarMulti(
	groupVars: Var[],
	aggVars: Var[],
	subgoal: Goal,
): Goal {
	return (input$) =>
		new SimpleObservable((observer) => {
			let active = 0;
			let completed = false;
			const subscription = input$.subscribe({
				next: (s) => {
					active++;
					const groupMap = new Map<string, Term[][]>();
					subgoal(SimpleObservable.of(s)).subscribe({
						next: (subst) => {
							const groupKey = JSON.stringify(
								groupVars.map((v) => walk(v, subst)),
							);
							let aggArrays = groupMap.get(groupKey);
							if (!aggArrays) {
								aggArrays = aggVars.map(() => []);
								groupMap.set(groupKey, aggArrays);
							}
							for (let i = 0; i < aggVars.length; i++) {
								const value = walk(aggVars[i], subst);
								aggArrays[i].push(value);
							}
						},
						error: observer.error,
						complete: () => {
							if (groupMap.size === 0) {
								const s2 = new Map(s);
								aggVars.forEach((v, i) => s2.set(v.id, []));
								observer.next(s2);
							} else {
								for (const [groupKey, aggArrays] of groupMap.entries()) {
									const groupValues = JSON.parse(groupKey);
									const s2 = new Map(s);
									groupVars.forEach((v, index) =>
										s2.set(v.id, groupValues[index]),
									);
									aggVars.forEach((v, index) => s2.set(v.id, aggArrays[index]));
									observer.next(s2);
								}
							}
							active--;
							if (completed && active === 0) observer.complete?.();
						},
					});
				},
				error: observer.error,
				complete: () => {
					completed = true;
					if (active === 0) observer.complete?.();
				},
			});
			return () => subscription.unsubscribe?.();
		});
}
