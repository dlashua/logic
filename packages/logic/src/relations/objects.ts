import { unify, walk, isVar } from "../core/kernel.js";
import { SimpleObservable } from "../core/observable.js";
import type { Goal, Subst, Term } from "../core/types.js";

/**
 * A goal that extracts specific keys from an object and unifies them with logic variables.
 * This is a simpler alternative to projectJsonata for basic object key extraction.
 *
 * Usage:
 *   extract($.input_object, {
 *     name: $.output_name,
 *     age: $.output_age,
 *     nested: {
 *       city: $.output_city,
 *       country: $.output_country
 *     }
 *   })
 *
 * If the mapping value is a logic variable, it unifies directly.
 * If the mapping value is an object/array, it recursively extracts from nested structures.
 */
export function extract(inputVar: Term, mapping: Record<string, Term>): Goal {
	return (input$: SimpleObservable<Subst>) =>
		input$.flatMap(
			(s: Subst) =>
				new SimpleObservable<Subst>((observer) => {
					const inputValue: Term<any> = walk(inputVar, s);

					// Input must be resolved to an object
					if (typeof inputValue !== "object" || inputValue === null) {
						observer.complete?.();
						return;
					}

					// Helper function to recursively extract values
					const extractRecursive = (sourceValue: any, targetMapping: any, currentSubst: Subst): Subst | null => {
						if (isVar(targetMapping)) {
							// If target is a logic variable, unify directly
							return unify(targetMapping, sourceValue, currentSubst);
						} else if (Array.isArray(targetMapping)) {
							// If target is an array, source should also be an array
							if (!Array.isArray(sourceValue) || sourceValue.length !== targetMapping.length) {
								return null;
							}
							let resultSubst = currentSubst;
							for (let i = 0; i < targetMapping.length; i++) {
								const nextSubst = extractRecursive(sourceValue[i], targetMapping[i], resultSubst);
								if (nextSubst === null) return null;
								resultSubst = nextSubst;
							}
							return resultSubst;
						} else if (typeof targetMapping === "object" && targetMapping !== null) {
							// If target is an object, recursively extract each key
							if (typeof sourceValue !== "object" || sourceValue === null) {
								return null;
							}
							let resultSubst = currentSubst;
							for (const [key, targetValue] of Object.entries(targetMapping)) {
								const sourceNestedValue = sourceValue[key];
								const nextSubst = extractRecursive(sourceNestedValue, targetValue, resultSubst);
								if (nextSubst === null) return null;
								resultSubst = nextSubst;
							}
							return resultSubst;
						} else {
							// If target is a literal value, check for equality
							return sourceValue === targetMapping ? currentSubst : null;
						}
					};

					// Extract each key and unify with corresponding variable/structure
					let currentSubst = s;
					for (const [key, outputMapping] of Object.entries(mapping)) {
						const value = inputValue[key];
						const nextSubst = extractRecursive(value, outputMapping, currentSubst);
						if (nextSubst === null) {
							// If any extraction fails, skip this result
							observer.complete?.();
							return;
						}
						currentSubst = nextSubst;
					}

					observer.next(currentSubst);
					observer.complete?.();
				}),
		);
}

/**
 * A goal that combines membero() and extract() - iterates over an array and extracts
 * specific keys from each element, creating one substitution per array element.
 *
 * Usage:
 *   extractEach($.array_of_objects, {
 *     name: $.item_name,
 *     age: $.item_age,
 *     email: $.item_email
 *   })
 *
 * This is equivalent to:
 *   membero($.item, $.array_of_objects),
 *   extract($.item, { name: $.item_name, age: $.item_age, email: $.item_email })
 *
 * But more concise and clearer in intent.
 */
export function extractEach(
	arrayVar: Term,
	mapping: Record<string, Term>,
): Goal {
	return (input$: SimpleObservable<Subst>) =>
		input$.flatMap(
			(s: Subst) =>
				new SimpleObservable<Subst>((observer) => {
					const arrayValue = walk(arrayVar, s);

					// Input must be resolved to an array
					if (!Array.isArray(arrayValue)) {
						observer.complete?.();
						return;
					}

					// For each element in the array, extract the specified keys
					for (const element of arrayValue) {
						if (typeof element === "object" && element !== null) {
							// Extract each key and unify with corresponding variable
							let currentSubst = s;
							let allUnified = true;

							for (const [key, outputVar] of Object.entries(mapping)) {
								const value = element[key];
								const unified = unify(outputVar, value, currentSubst);
								if (unified !== null) {
									currentSubst = unified;
								} else {
									// If any unification fails, skip this element
									allUnified = false;
									break;
								}
							}

							if (allUnified) {
								observer.next(currentSubst);
							}
						}
					}

					observer.complete?.();
				}),
		);
}
