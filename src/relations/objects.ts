import { Goal, Subst, Term } from "../core/types.ts";
import { walk, unify } from "../core/kernel.ts";
import { SimpleObservable } from "../core/observable.ts";

/**
 * A goal that extracts specific keys from an object and unifies them with logic variables.
 * This is a simpler alternative to projectJsonata for basic object key extraction.
 * 
 * Usage:
 *   extract($.input_object, {
 *     name: $.output_name,
 *     age: $.output_age,
 *     email: $.output_email
 *   })
 * 
 * This will take the object in $.input_object and unify:
 * - object.name with $.output_name
 * - object.age with $.output_age  
 * - object.email with $.output_email
 */
export function extract(
  inputVar: Term,
  mapping: Record<string, Term>
): Goal {
  return (input$: SimpleObservable<Subst>) => 
    input$.flatMap((s: Subst) => new SimpleObservable<Subst>((observer) => {
      const inputValue: Term<any> = walk(inputVar, s);
      
      // Input must be resolved to an object
      if (typeof inputValue !== 'object' || inputValue === null) {
        observer.complete?.();
        return;
      }
      
      // Extract each key and unify with corresponding variable
      let currentSubst = s;
      for (const [key, outputVar] of Object.entries(mapping)) {
        const value = inputValue[key];
        const unified = unify(outputVar, value, currentSubst);
        if (unified !== null) {
          currentSubst = unified;
        } else {
          // If any unification fails, skip this result
          observer.complete?.();
          return;
        }
      }
      
      observer.next(currentSubst);
      observer.complete?.();
    }));
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
  mapping: Record<string, Term>
): Goal {
  return (input$: SimpleObservable<Subst>) => 
    input$.flatMap((s: Subst) => new SimpleObservable<Subst>((observer) => {
      const arrayValue = walk(arrayVar, s);
      
      // Input must be resolved to an array
      if (!Array.isArray(arrayValue)) {
        observer.complete?.();
        return;
      }
      
      // For each element in the array, extract the specified keys
      for (const element of arrayValue) {
        if (typeof element === 'object' && element !== null) {
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
    }));
}