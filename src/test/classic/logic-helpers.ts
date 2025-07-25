import { inspect } from "node:util";
import { eq, and, or } from "../../core/combinators.ts";
import {
  walk,
  isVar,
  unify,
  arrayToLogicList,
  lvar
} from "../../core/kernel.ts"
import { Term, Goal, Subst } from "../../core/types.ts";
import { SimpleObservable } from "../../core/observable.ts";
import { suspendable, CHECK_LATER, makeSuspendHandler } from "../../core/suspend-helper.ts";
import { not, neqo, thruCount } from "../../relations/control.ts";
import { alldistincto, membero } from "../../relations/lists.ts";
import { addSuspendToSubst } from "../../core/subst-suspends.ts";

export function make(piles, keyVals, keyBy, numArrays) {

  const fields = Object.keys(keyVals);

  // Smarter membero: assigns each variable to a value not already taken in subst, pure (not suspendable)
  function smartMembero($, field) {
    return (input$: SimpleObservable<Subst>) => new SimpleObservable<Subst>((observer) => {
      const sub = input$.subscribe({
        complete: observer.complete,
        error: observer.error,
        next: (subst) => {
          const avail = new Set(keyVals[field]);
          const unbound = [];
          for(const primaryValue of keyVals[keyBy]) {
            const otherVar = $[`${keyBy}_${primaryValue}_${field}`];
            const otherValue = walk(otherVar, subst);
            // const otherValue = otherVar;
            if(!isVar(otherValue)) {
              if(!avail.has(otherValue)) {
                return
              }
              avail.delete(otherValue);
            } else {
              unbound.push(otherVar);
            }
          }
          if(avail.size !== unbound.length) {
            return;
          }

          for(const oneVal of avail) {
            for (const oneVar of unbound) {
              const nextSubst = unify(oneVar, oneVal, subst);
              if(nextSubst === null) continue;
              observer.next(nextSubst)
            }
          }
        }
      });

      return () => sub.unsubscribe();
    });
  }

  function smartMemberoAll($) {
    return and(
      ...fields.filter(x => x !== keyBy).map(field => and(
        ...keyVals[keyBy].map(primaryValue => and(
          membero($[`${keyBy}_${primaryValue}_${field}`], keyVals[field]),
          thruCount(`membero ${primaryValue} ${field}`),
          // collectThenGo(),
        ))
      ))
    )

    // return and(
    //   ...fields.filter(x => x !== keyBy).map(field => and(
    //     smartMembero($, field),
    //     thruCount(`smartMembero ${field}`),
    //     collectThenGo(),
    //   ))
    // )
  }

  function getVar($, primaryValue, varName) {
    return $[`${keyBy}_${primaryValue}_${varName}`];
  }


  function unsetLater($, whereObj, updateObj) {
    function evaluator(values: unknown[], subst: Subst) {
      const currentObj = Object.fromEntries(fields.map((field, i) => [field, values[i]]));
      for(const whereKey of Object.keys(whereObj)) {
        const whereVal = walk(whereObj[whereKey], subst);
        if (currentObj[whereKey] !== whereVal) {
          return CHECK_LATER
        }
      }
      
      let newSubst = subst;
      for(const updateKey of Object.keys(updateObj)) {
        function makeResumeFn(primaryValue, updateKey, updateVal) {
          return (subst) => {
            const primaryOtherValue = walk($[`${keyBy}_${primaryValue}_${updateKey}`], subst);
            if(isVar(primaryOtherValue)) {
              return CHECK_LATER;
            }
            if (primaryOtherValue === updateVal) {
              return null;
            }
            return subst;
          }
        }

        const resumeFn = makeResumeFn(currentObj[keyBy], updateKey, updateObj[updateKey]);
        const resumeSubst = resumeFn(subst);
        if(resumeSubst === CHECK_LATER) {
          const watchVar = $[`${keyBy}_${currentObj[keyBy]}_${updateKey}`];
          const nextSubst = addSuspendToSubst(subst, resumeFn, [watchVar])
          newSubst = nextSubst;
        } else if(resumeSubst === null) {
          return null;
        } else {
          newSubst = resumeSubst
        }

      }

      return newSubst;
    };
    return and(
      ...keyVals[keyBy].map(primaryValue => {
        const watchVars = fields.map(field => field === keyBy ? primaryValue : $[`${keyBy}_${primaryValue}_${field}`]);

        return suspendable(watchVars, evaluator);  
      })
    );
  } 

  function setLater($, setObj) {
    function evaluator(values: unknown[], subst: Subst) {
      const currentObj = Object.fromEntries(fields.map((field, i) => [field, values[i]]));
      let found = false;
      for(const whereKey of Object.keys(setObj)) {
        if (currentObj[whereKey] === setObj[whereKey]) {
          found = true;
          break;
        }
      }

      if(found === false) {
        return CHECK_LATER;
      }

      let newSubst = subst;
      for(const updateKey of Object.keys(setObj)) {
        const updateVar = $[`${keyBy}_${currentObj[keyBy]}_${updateKey}`];
        const nextSubst = unify(updateVar, setObj[updateKey], newSubst);
        if( nextSubst === null) {
          return null;
        }
        newSubst = nextSubst;
      }

      return newSubst;
    };
    return and(
      ...keyVals[keyBy].map(primaryValue => {
        const watchVars = fields.map(field => field === keyBy ? primaryValue : $[`${keyBy}_${primaryValue}_${field}`]);

        return suspendable(watchVars, evaluator);  
      })
    );
  } 

  function unlink ($, objA, objB) {
    const objAKeys = Object.keys(objA);
    if(objAKeys.includes(keyBy)) {
      if (!isVar(objA[keyBy])) {
        return _directUnlink($, objA[keyBy], objB);
      } else {
        throw new Error("not implemented");
      }
    }
    const objBKeys = Object.keys(objB);
    if(objBKeys.includes(keyBy)) {
      if (!isVar(objB[keyBy])) {
        return _directUnlink($, objB[keyBy], objA);
      } else {
        throw new Error("not implemented");
      }
    }
    return and(
      unsetLater($, objA, objB),
      unsetLater($, objB, objA),
    );
  }

  function _directUnlink($, primaryValue, obj) {
    const objKeys = Object.keys(obj);
    const goals = [];
    const varStart = `${keyBy}_${primaryValue}`;
    for (const key of objKeys) {
      const keyVar = $[`${varStart}_${key}`];
      goals.push(neqo(keyVar, obj[key]));
    }
    return and(...goals);
  }

  // function _messyUnlink($, objA, objB) {
  //   const goals = [];
  //   goals.push(or(
  //     ...(keyVals[keyBy].map(primaryVal => 
  //       and(
  //         _directLink($, {
  //           [keyBy]: primaryVal,
  //           ...objA
  //         }),
  //         _directUnlink($, primaryVal, objB)
  //       )
  //     ))
  //   ))
  //   return and(...goals);
  // }

  function link ($, obj) {
    const objKeys = Object.keys(obj);
    if(objKeys.includes(keyBy)) {
      if (!isVar(obj[keyBy])) {
        return _directLink($, obj);
      } else {
        throw new Error("not implemented");
      }
    }
    return setLater($, obj);
  }

  // function _messyLink($, obj) {
  //   const goals = [];
  //   goals.push(or(
  //     ...(keyVals[keyBy].map(primaryVal => 
  //       _directLink($, {
  //         [keyBy]: primaryVal,
  //         ...obj
  //       })
  //     ))
  //   ))
  //   return and(...goals);
  // }

  function _directLink($, obj) {
    const objKeys = Object.keys(obj);
    const goals = [];
    const varStart = `${keyBy}_${obj[keyBy]}`;
    for (const key of objKeys) {
      const keyVar = $[`${varStart}_${key}`];
      goals.push(eq(keyVar, obj[key]));
    }
    return and(...goals);
  }

  function constrainArrays($): Goal {
    const constraints = [];
    const pileVars = [];

    // Set up pile variables
    for (let i = 1; i <= numArrays; i++) {
      pileVars.push($[`keyby_${keyBy}_${i}`]);
    }
    // console.log("Constrain", [piles, pileVars]);

    constraints.push(eq(piles, pileVars));

    // Set up each pile
    for (let i = 1; i <= numArrays; i++) {
      const pileVar = $[`keyby_${keyBy}_${i}`];
      const arrayElements = [];
      const keyByVal = keyVals[keyBy][i-1];
      for (const field of fields) {
        if (field === keyBy) {
          arrayElements.push(keyByVal);
          continue;
        }
        arrayElements.push($[`${keyBy}_${keyByVal}_${field}`]);
      }
      constraints.push(eq(pileVar, arrayElements));
    }

    constraints.push(distinctValidateAll($));

    return and(...constraints);
  }

  function enforceConsistency($) {
    const goals = []

    const fields = Object.keys(keyVals); // e.g., ["a", "n", "d", "i", "f"]
    for (const field of fields) {
      for (const dfield of fields) {
        if (field === dfield) continue;
        const arrayElements = [];
        for (const dval of keyVals[dfield]) {
          arrayElements.push($[`${dfield}_${dval}_${field}`]);
        }
        for(const oneEl of arrayElements) {
          // console.log("MEMBERO AS OR", [oneEl, keyVals[field]])
          // goals.push(membero(oneEl,keyVals[field]))
          // goals.push(or(
          //   ...(keyVals[field].map(x => eq(oneEl, x))),
          // ))
          // goals.push(thruCount(`membero as or for ${field}`))
          // goals.push(collectThenGo())
        }
        // goals.push(alldistincto(arrayElements));
      }
      // const arrayElements = fields.map(field => $[`p${i}_${field}`]);
      // Constrain first field (a) to keyVals.a[i]
      // constraints.push(eq($[`p${i}_${fields[0]}`], keyVals[fields[0]][i - 1]));
    }

    // for (const key of Object.keys(keyVals)) {
    //   for (const value of Object.values(keyVals[key])) {
    //     goals.push(link($, { [key]: value }))
    //   }
    // }

    return and(...goals);
  }

  function manualDistinctnessValidator (allVars, possibleVals) {
    return suspendable(allVars, (values, subst) => {
      const seen = new Set();
      for (const v of values) {
        if(!isVar(v) && !possibleVals.includes(v)) {
          // console.log("Forbidden Value", { v, possibleVals })
          return null;
        }
        const vr = isVar(v) ? `__${v.id}__` : v;
        if(seen.has(vr)) {
          // console.log("MDV DUPE", {
          //   allVars, values, vr, seen 
          // })
          return null;
        }
        seen.add(vr);
      }

      const ungroundedValues = values.filter(x => isVar(x));
      if(ungroundedValues.length === 0) {
        // console.log("ALL MATCHED");
        return subst;
      }

      const leftovers = possibleVals.filter(x => !seen.has(x));
      
      // Handle case with exactly one ungrounded variable and one leftover value
      if(ungroundedValues.length === 1 && leftovers.length === 1) {
        const res = unify(ungroundedValues[0], leftovers[0], subst);
        // console.log("FORCED LAST", {
        //   var: ungroundedValues[0], val: leftovers[0], allVars, seen, res: res !== null 
        // });
        return res;
      }

      return CHECK_LATER;

    }, 0);
  }

  let rcvId = 0;
  function rowConsistencyValidator($, rowVars) {
    const handler = (values, subst) => {
      
      const id = ++rcvId;
      const fields = Object.keys(keyVals);
      
      // Find any newly grounded values in this row
      const groundedPairs = [];
      for (let i = 0; i < values.length; i++) {
        if (!isVar(values[i])) {
          groundedPairs.push({ field: fields[i], value: values[i] });
        }
      }
      
      // If no grounded values, nothing to propagate yet
      if (groundedPairs.length === 0) {
        return CHECK_LATER;
      }

      let resultSubst = subst;
      // For each grounded pair, unify with all other grounded pairs
      for (const { field: fieldA, value: valueA } of groundedPairs) {
        for (const { field: fieldB, value: valueB } of groundedPairs) {
          if (fieldA === fieldB) continue;
          
          // Unify the cross-reference variable: fieldA_valueA_fieldB should equal valueB
          const crossRefVar = $[`${fieldA}_${valueA}_${fieldB}`];
          // console.log(id, "UF", crossRefVar, valueB);
          const unifyResult = unify(crossRefVar, valueB, resultSubst);
          if (unifyResult === null) {
            console.log(id, "RCV UNIFY FAILED", {
              crossRefVar: crossRefVar.id || crossRefVar,
              fieldA,
              valueA,
              fieldB,
              valueB
            });
            return null;
          }
          resultSubst = unifyResult;
          
          // Now propagate all existing mappings from fieldB_valueB_* to fieldA_valueA_*
          for (const fieldC of fields) {
            // if (fieldC === fieldA || fieldC === fieldB) continue;
            
            const varBC = $[`${fieldB}_${valueB}_${fieldC}`];
            const valueBC = walk(varBC, resultSubst);
            
            // If fieldB_valueB_fieldC has a value, propagate it to fieldA_valueA_fieldC
            // if (!isVar(valueBC)) {
            const varAC = $[`${fieldA}_${valueA}_${fieldC}`];
            const propagateResult = unify(varAC, valueBC, resultSubst);
            if (propagateResult === null) {
              console.log(id, "RCV PROPAGATION FAILED", {
                from: `${fieldB}_${valueB}_${fieldC}`,
                to: `${fieldA}_${valueA}_${fieldC}`,
                value: valueBC
              });
              return null;
            }
            resultSubst = propagateResult;
            // console.log(id, "PROPAGATED", `${fieldB}_${valueB}_${fieldC}`, "->", `${fieldA}_${valueA}_${fieldC}`, "=", valueBC);
            // }
          }
        }
      }

      const handleSuspend = makeSuspendHandler(rowVars, handler, 0);
      const suspendSubst = addSuspendToSubst(resultSubst, handleSuspend, rowVars);
      return suspendSubst;
      // Return the updated substitution with the same suspendable constraint
      // return suspendable(rowVars, handler, 0)(resultSubst);
    }
    return suspendable(rowVars, handler, 0);
  }

  function onlyStringSubst(s: Subst) {
    const result: Record<string, string> = {};
    for (const [key, value] of s.entries()) {
      if (typeof key === "string") {
        result[key] = value;
      }
    }
    return result;
  }

  function distinctValidateAll($) {
    const constraints = [];
    for (const field of fields) {
      const row_collection = [];
      for (const primaryVal of keyVals[keyBy]) {
        const varName = `${keyBy}_${primaryVal}_${field}`;
        row_collection.push($[varName]);
      }
      constraints.push(manualDistinctnessValidator(row_collection, keyVals[field]));
    }
    // for (const sourceField of fields) {
    //   for (const sourceVal of keyVals[sourceField]) {
    //     const unique_collection = [];
    //     for (const targetField of fields) {
    //       if (sourceField === targetField) {
    //         unique_collection.push(sourceVal);
    //       } else {
    //         const varName = `${sourceField}_${sourceVal}_${targetField}`;
    //         unique_collection.push($[varName]);
    //       }
    //     }
    //     // constraints.push(rowConsistencyValidator($, unique_collection));
    //   }
    // }
    return and(...constraints);
  }

  function distincto(vars: Term[]): Goal {
    return suspendable(vars, (values, subst) => {
      const groundedValues = values.filter(v => !isVar(v));
      if (groundedValues.length > 0) {
        const uniqueValues = new Set(groundedValues.map(v => JSON.stringify(v)));
        if (uniqueValues.size !== groundedValues.length) {
          console.log("Distincto failed: duplicate values", groundedValues);
          return null;
        }
        // Only apply domain check for standard pile variables
        const fieldMatch = vars[0]?.id?.match(/q_p\d+_([^_]+)_/);
        const field = fieldMatch ? fieldMatch[1] : null;
        if (field && keyVals[field as keyof typeof keyVals] && vars[0]?.id?.startsWith("q_p")) {
          const domain = keyVals[field as keyof typeof keyVals];
          const usedValues = new Set(groundedValues);
          const remainingVars = vars.length - groundedValues.length;
          if (domain.length - usedValues.size < remainingVars) {
            console.log(`Distincto failed for ${field}: only ${domain.length - usedValues.size} values left for ${remainingVars} variables`);
            return null;
          }
        }
      }
      return groundedValues.length === vars.length ? subst : CHECK_LATER;
    }, 1);
  }

  return {
    constrainArrays,
    enforceConsistency,
    distinctValidateAll,
    rowConsistencyValidator,
    link,
    unlink,
    distincto,
    getVar,
    smartMembero,
    smartMemberoAll,
  }
}


export function collectThenGo() {
  return (input$: SimpleObservable<Subst>) => new SimpleObservable<Subst>((observer) => {
    const substs: Subst[] = [];
    const sub = input$.subscribe({
      next: (s) => substs.push(s),
      error: observer.error,
      complete: () => {
        for (const oneS of substs) {
          observer.next(oneS);
        }
        observer.complete?.();
      }
    });

    return () => sub.unsubscribe();
  });
}


