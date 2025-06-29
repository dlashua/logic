// Memory-based fact relations using modular architecture
// This file provides a compatibility layer for existing code while using the new modular system

import { 
  makeFacts as makeFactsNew,
  makeFactsObj as makeFactsObjNew,
  makeFactsSym as makeFactsSymNew,
  makeFactsObjSym as makeFactsObjSymNew,
  FactRelation,
  FactObjRelation,
  intersect,
  isIndexable
} from './facts/facts-memory.ts';

import { 
  Term, 
  Var, 
  Subst, 
  Goal, 
  or, 
  walk 
} from './core.ts';

// Re-export the new modular implementations
export const makeFacts = makeFactsNew;
export const makeFactsObj = makeFactsObjNew;
export const makeFactsSym = makeFactsSymNew;
export const makeFactsObjSym = makeFactsObjSymNew;

// Re-export types
export type { FactRelation, FactObjRelation };

// Re-export utilities
export { intersect, isIndexable };

// Legacy compatibility function (slower version using or)
export function makeFactsSymSlow(): FactRelation {
  const orig = makeFacts();

  const symGoal = (...q: Term[]) => or(orig(q[0], q[1]), orig(q[1], q[0]));
  symGoal.set = orig.set;
  symGoal.raw = orig.raw;
  symGoal.indexes = orig.indexes;

  return symGoal as FactRelation;
}

/**
 * Aggregates all possible values of a logic variable into an array and binds to sourceVar in a single solution.
 */
export function aggregateVar(sourceVar: Var, subgoal: Goal): Goal {
  return async function* (s: Subst) {
    const results: Term[] = [];
    for await (const subst of subgoal(s)) {
      results.push(await walk(sourceVar, subst));
    }
    const s2 = new Map(s);
    s2.set(sourceVar.id, results);
    yield s2;
  };
}

/**
 * For each unique combination of groupVars, aggregate all values of each aggVar in aggVars, and yield a substitution with arrays bound to each aggVar.
 */
export function aggregateVarMulti(groupVars: Var[], aggVars: Var[], subgoal: Goal): Goal {
  return async function* (s: Subst) {
    const groupMap = new Map<string, Term[][]>();
    for await (const subst of subgoal(s)) {
      const groupKey = JSON.stringify(await Promise.all(groupVars.map(v => walk(v, subst))));
      let aggArrays = groupMap.get(groupKey);
      if (!aggArrays) {
        aggArrays = aggVars.map(() => []);
        groupMap.set(groupKey, aggArrays);
      }
      aggVars.forEach((v, i) => {
        aggArrays[i].push(walk(v, subst));
      });
    }
    if (groupMap.size === 0) {
      const s2 = new Map(s);
      aggVars.forEach((v, i) => s2.set(v.id, []));
      yield s2;
      return;
    }
    for (const [groupKey,
      aggArrays] of groupMap.entries()) {
      const groupValues = JSON.parse(groupKey);
      const s2 = new Map(s);
      groupVars.forEach((v, index) => s2.set(v.id, groupValues[index]));
      aggVars.forEach((v, index) => s2.set(v.id, aggArrays[index].map(async x => await x)));
      yield s2;
    }
  };
}
