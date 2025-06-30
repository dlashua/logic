import type { Subst, Term, Goal, Var } from "../core/types.ts";
import { walk } from "../core/kernel.ts";

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
      for (let i = 0; i < aggVars.length; i++) {
        const value = await walk(aggVars[i], subst);
        aggArrays![i].push(value);
      }
    }
    if (groupMap.size === 0) {
      const s2 = new Map(s);
      aggVars.forEach((v, i) => s2.set(v.id, []));
      yield s2;
      return;
    }
    for (const [groupKey, aggArrays] of groupMap.entries()) {
      const groupValues = JSON.parse(groupKey);
      const s2 = new Map(s);
      groupVars.forEach((v, index) => s2.set(v.id, groupValues[index]));
      aggVars.forEach((v, index) => s2.set(v.id, aggArrays[index]));
      yield s2;
    }
  };
}
