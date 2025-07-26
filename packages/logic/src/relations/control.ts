import util from "node:util";
import { eq } from "../core/combinators.js";
import { enrichGroupInput, isVar, unify, walk } from "../core/kernel.js";
import { SimpleObservable } from "../core/observable.js";
import {
	getSuspendsFromSubst,
	SUSPENDED_CONSTRAINTS,
} from "../core/subst-suspends.js";
import { CHECK_LATER, suspendable } from "../core/suspend-helper.js";
import type { ConsNode, Goal, Subst, Term } from "../core/types.js";

export const uniqueo = (t: Term, g: Goal): Goal =>
	enrichGroupInput("uniqueo", [g], [], (input$: SimpleObservable<Subst>) =>
		input$.flatMap((s: Subst) => {
			const seen = new Set();
			return g(SimpleObservable.of(s)).flatMap((s2: Subst) => {
				const w_t = walk(t, s2);
				if (isVar(w_t)) {
					return SimpleObservable.of(s2);
				}
				const key = JSON.stringify(w_t);
				if (seen.has(key)) return SimpleObservable.empty();
				seen.add(key);
				return SimpleObservable.of(s2);
			});
		}),
	);

export function not(goal: Goal): Goal {
	return enrichGroupInput("not", [], [goal], (input$) =>
		input$.flatMap((s: Subst) => {
			return new SimpleObservable<Subst>((observer) => {
				let hasSolutions = false;
				const sub = goal(SimpleObservable.of(s)).subscribe({
					next: (subst) => {
						if (!subst.has(SUSPENDED_CONSTRAINTS)) {
							hasSolutions = true;
						}
					},
					error: (err) => observer.error?.(err),
					complete: () => {
						if (!hasSolutions) {
							observer.next(s);
						}
						observer.complete?.();
					},
				});
				return () => sub.unsubscribe();
			});
		}),
	);
}

export function gv1_not(goal: Goal): Goal {
	return enrichGroupInput(
		"not",
		[],
		[goal],
		(input$: SimpleObservable<Subst>) =>
			input$.flatMap((s: Subst) => {
				return new SimpleObservable<Subst>((observer) => {
					let hasSolutions = false;
					const sub = goal(SimpleObservable.of(s)).subscribe({
						next: () => {
							hasSolutions = true; // Any solution means the goal succeeds, so not fails
						},
						error: (err) => observer.error?.(err),
						complete: () => {
							if (!hasSolutions) {
								observer.next(s); // No solutions means not succeeds
							}
							observer.complete?.();
						},
					});
					return () => sub.unsubscribe();
				});
			}),
	);
}

export function old_not(goal: Goal): Goal {
	return enrichGroupInput(
		"not",
		[],
		[goal],
		(input$: SimpleObservable<Subst>) =>
			input$.flatMap((s: Subst) => {
				let found = false;
				return new SimpleObservable<Subst>((observer) => {
					goal(SimpleObservable.of(s)).subscribe({
						next: (subst) => {
							let addedNewBindings = false;
							for (const [key, value] of subst) {
								if (!s.has(key)) {
									addedNewBindings = true;
									break;
								}
							}
							if (!addedNewBindings) {
								found = true;
							}
						},
						error: observer.error,
						complete: () => {
							if (!found) observer.next(s);
							observer.complete?.();
						},
					});
				});
			}),
	);
}

export function neqo(x: Term<any>, y: Term<any>): Goal {
	return suspendable(
		[x, y],
		(values, subst) => {
			const [xVal, yVal] = values;
			const xGrounded = !isVar(xVal);
			const yGrounded = !isVar(yVal);

			if (xGrounded && yGrounded) {
				// Both terms are ground, check inequality
				return xVal !== yVal ? subst : null;
			}

			if (!xGrounded && !yGrounded) {
				if (xVal.id === yVal.id) {
					return null;
				}
			}

			// if(xGrounded) {
			//   const newsubst = unify(yVal, xVal, subst)
			//   console.log(newsubst);
			//   if(newsubst === null) return subst;
			//   return CHECK_LATER;
			// }

			// if(yGrounded) {
			//   const newsubst = unify(xVal, yVal, subst)
			//   console.log(newsubst);
			//   if(newsubst === null) return subst;
			//   return CHECK_LATER;
			// }
			return CHECK_LATER;
		},
		0,
	);
}

// export const neqo = (x: Term, y: Term): Goal => not(eq(x, y));
export function old_neqo(x: Term<any>, y: Term<any>): Goal {
	return suspendable(
		[x, y],
		(values, subst) => {
			return CHECK_LATER;
			const [xVal, yVal] = values;
			const xGrounded = !isVar(xVal);
			const yGrounded = !isVar(yVal);

			// All grounded - check constraint
			if (xGrounded && yGrounded) {
				return xVal !== yVal ? subst : null;
			}

			// if(xGrounded) {
			//   const s2 = unify(xVal, yVal, subst);
			//   if(s2) return CHECK_LATER;
			//   return subst;
			// }

			// if(yGrounded) {
			//   const s2 = unify(yVal, xVal, subst);
			//   if(s2) return CHECK_LATER;
			//   return subst;
			// }

			return CHECK_LATER; // Still not enough variables bound
		},
		0,
	);
}

/**
 * A goal that succeeds if the given goal succeeds exactly once.
 * Useful for cut-like behavior.
 */
export function onceo(goal: Goal): Goal {
	return (input$: SimpleObservable<Subst>) => goal(input$).take(1);
}

/**
 * A goal that always succeeds with the given substitution.
 * Useful as a base case or for testing.
 */
export function succeedo(): Goal {
	return (input$: SimpleObservable<Subst>) =>
		input$.flatMap(
			(s: Subst) =>
				new SimpleObservable<Subst>((observer) => {
					observer.next(s);
					observer.complete?.();
				}),
		);
}

/**
 * A goal that always fails.
 * Useful for testing or as a base case.
 */
export function failo(): Goal {
	return (_input$: SimpleObservable<Subst>) => SimpleObservable.empty<Subst>();
}

/**
 * A goal that succeeds if the term is ground (contains no unbound variables).
 */
export function groundo(term: Term): Goal {
	return (input$: SimpleObservable<Subst>) =>
		input$.flatMap(
			(s: Subst) =>
				new SimpleObservable<Subst>((observer) => {
					const walked = walk(term, s);
					function isGround(t: Term): boolean {
						if (isVar(t)) return false;
						if (Array.isArray(t)) {
							return t.every(isGround);
						}
						if (t && typeof t === "object" && "tag" in t) {
							if (t.tag === "cons") {
								const l = t as ConsNode;
								return isGround(l.head) && isGround(l.tail);
							}
							if (t.tag === "nil") {
								return true;
							}
						}
						if (t && typeof t === "object" && !("tag" in t)) {
							return Object.values(t).every(isGround);
						}
						return true; // primitives are ground
					}
					if (isGround(walked)) {
						observer.next(s);
					}
					observer.complete?.();
				}),
		);
}

/**
 * A goal that succeeds if the term is not ground (contains unbound variables).
 */
export function nonGroundo(term: Term): Goal {
	return not(groundo(term));
}

/**
 * A goal that logs each substitution it sees along with a message.
 */
export function substLog(msg: string, onlyVars = false): Goal {
	return enrichGroupInput(
		"substLog",
		[],
		[],
		(input$: SimpleObservable<Subst>) =>
			new SimpleObservable<Subst>((observer) => {
				const sub = input$.subscribe({
					next: (s) => {
						const ns = onlyVars
							? Object.fromEntries(
									[...s.entries()].filter(([k, v]) => typeof k === "string"),
								)
							: s;
						console.log(
							`[substLog] ${msg}:`,
							util.inspect(ns, {
								depth: null,
								colors: true,
							}),
						);
						observer.next(s);
					},
					error: observer.error,
					complete: observer.complete,
				});
				return () => sub.unsubscribe();
			}),
	);
}

let thruCountId = 0;
export function thruCount(msg: string, level = 1000): Goal {
	const id = ++thruCountId;
	return enrichGroupInput(
		"thruCount",
		[],
		[],
		(input$: SimpleObservable<Subst>) =>
			new SimpleObservable<Subst>((observer) => {
				let cnt = 0;
				const sub = input$.subscribe({
					next: (s) => {
						cnt++;

						// Determine current level based on count
						let currentLevel = 1;
						if (cnt >= 10) currentLevel = 10;
						if (cnt >= 100) currentLevel = 100;
						if (cnt >= 1000) currentLevel = 1000;
						// if (cnt >= 10000) currentLevel = 10000;
						// if (cnt >= 100000) currentLevel = 100000;

						if (cnt % currentLevel === 0) {
							let nonSymbolKeyCount = 0;
							for (const key of s.keys()) {
								if (typeof key !== "symbol") nonSymbolKeyCount++;
							}
							const suspendedCount = getSuspendsFromSubst(s).length;
							console.log("THRU", id, msg, cnt, {
								nonSymbolKeyCount,
								suspendedCount,
							});
						}
						observer.next(s);
					},
					error: observer.error,
					complete: () => {
						console.log("THRU COMPLETE", id, msg, cnt);
						observer.complete?.();
					},
				});
				return () => sub.unsubscribe();
			}),
	);
}

export function fail(): Goal {
	return (input$: SimpleObservable<Subst>) =>
		new SimpleObservable<Subst>((observer) => {
			const sub = input$.subscribe({
				next: (s) => {
					/* pass */
				},
				error: observer.error,
				complete: observer.complete,
			});
			return () => sub.unsubscribe();
		});
}
