// Fact helpers for MiniKanren-style logic programming
import {
  Subst,
  Term,
  Var,
  isVar,
  unify,
  walk
} from './core.ts'
import { Goal, or } from './relations.ts';

/**
 * A relation for tuple facts (array-based).
 * Has a callable interface and a .set method for adding facts.
 */
export interface FactRelation {
    (...query: Term[]): Goal;
    set: (...fact: Term[]) => void;
    raw: Term[][];
    indexes: Map<number, Map<any, Set<number>>>;
}

/**
 * A relation for object facts (object-based).
 * Has a callable interface and a .set method for adding facts.
 */
export interface FactObjRelation {
    (queryObj: Record<string, Term>): Goal;
    set: (factObj: Record<string, Term>) => void;
    raw: Record<string, Term>[];
    indexes: Map<string, Map<any, Set<number>>>;
    keys: string[];
}

/**
 * Create a tuple-based fact relation (like a table of tuples).
 */
export function makeFacts(): FactRelation {
  const facts: Term[][] = [];
  const indexes = new Map<number, Map<any, Set<number>>>();

  function goalFn(...query: Term[]): Goal {
    return async function* (s: Subst) {
      const walkedQuery = await Promise.all(query.map(term => walk(term, s)));

      // Find all indexable, grounded positions
      const indexedPositions: number[] = [];
      walkedQuery.forEach((wq, i) => {
        if (!isVar(wq) && indexes.has(i)) {
          indexedPositions.push(i);
        }
      });

      let candidateIndexes: Set<number> | null = null;
      if (indexedPositions.length > 0) {
        // Intersect all index hits
        for (const i of indexedPositions) {
          const wq = walkedQuery[i];
          const index = indexes.get(i);
          if (!index) continue;
          const factNums = index.get(wq);
          if (!factNums) {
            candidateIndexes = new Set();
            break;
          }
          if (candidateIndexes === null) {
            candidateIndexes = new Set(factNums);
          } else {
            candidateIndexes = intersect(candidateIndexes, factNums);
            if (candidateIndexes.size === 0) break;
          }
        }
      }

      if (candidateIndexes === null) {
        // No grounded values: full scan
        for (const fact of facts) {
          const s1 = await unify(query, fact, s);
          if (s1) {
            yield s1;
          }
        }
        return;
      }

      for (const factIndex of candidateIndexes) {
        const fact = facts[factIndex];
        const s1 = await unify(query, fact, s);
        if (s1) {
          yield s1;
        }
      }
    };
  }

  // const relation = (...query: Term[]): Goal => goalFn(...query);

  /**
     * Add a fact (tuple) to the relation.
     */
  goalFn.set = (...fact: Term[]) => {
    const factIndex = facts.length;
    facts.push(fact);
    fact.forEach((term, i) => {
      if (isIndexable(term)) {
        let index = indexes.get(i);
        if (!index) {
          index = new Map<any, Set<number>>();
          indexes.set(i, index);
        }
        let set = index.get(term);
        if (!set) {
          set = new Set<number>();
          index.set(term, set);
        }
        set.add(factIndex);
      }
    });
  };

  goalFn.raw = facts;
  goalFn.indexes = indexes;
  return goalFn;
}

/**
 * Create an object-based fact relation (like a table of objects).
 */
export function makeFactsObj(keys: string[]): FactObjRelation {
  const facts: Record<string, Term>[] = [];
  const indexes = new Map<string, Map<any, Set<number>>>();

  function goalFn(queryObj: Record<string, Term>): Goal {
    const keysArr = Object.keys(queryObj);
    return async function* (s: Subst) {
      const walkedQuery: Record<string, Term> = {};
      for (const k of keysArr) {
        walkedQuery[k] = await walk(queryObj[k], s);
      }

      // Find all indexable, grounded keys
      const indexedKeys: string[] = [];
      for (const k of keysArr) {
        if (!isVar(walkedQuery[k]) && indexes.has(k)) {
          indexedKeys.push(k);
        }
      }

      let candidateIndexes: Set<number> | null = null;
      if (indexedKeys.length > 0) {
        for (const k of indexedKeys) {
          const wq = walkedQuery[k];
          const index = indexes.get(k);
          if (!index) continue;
          const factNums = index.get(wq);
          if (!factNums) {
            candidateIndexes = new Set();
            break;
          }
          if (candidateIndexes === null) {
            candidateIndexes = new Set(factNums);
          } else {
            candidateIndexes = intersect(candidateIndexes, factNums);
            if (candidateIndexes.size === 0) break;
          }
        }
      }

      if (candidateIndexes === null) {
        // No grounded values: full scan
        for (const fact of facts) {
          const s1 = await unify(keysArr.map(k => queryObj[k]), keysArr.map(k => fact[k]), s);
          if (s1) yield s1;
        }
        return;
      }

      for (const factIndex of candidateIndexes) {
        const fact = facts[factIndex];
        const s1 = await unify(keysArr.map(k => queryObj[k]), keysArr.map(k => fact[k]), s);
        if (s1) yield s1;
      }
    };
  }

  function relation(queryObj: Record<string, Term>): Goal {
    return goalFn(queryObj);
  }

  /**
     * Add a fact (object) to the relation.
     */
  relation.set = (factObj: Record<string, Term>) => {
    const keys = Object.keys(factObj);
    const factIndex = facts.length;
    const fact: Record<string, Term> = {};
    for (const k of keys) {
      fact[k] = factObj[k];
    }
    facts.push(fact);
    for (const k of keys) {
      const term = fact[k];
      if (isIndexable(term)) {
        let index = indexes.get(k);
        if (!index) {
          index = new Map<any, Set<number>>();
          indexes.set(k, index);
        }
        let set = index.get(term);
        if (!set) {
          set = new Set<number>();
          index.set(term, set);
        }
        set.add(factIndex);
      }
    }
  };

  relation.raw = facts;
  relation.indexes = indexes;
  relation.keys = keys;
  return relation;
}

// --- Symmetric tuple-based fact relation (query-time symmetry) ---
export function makeFactsSym(): FactRelation {
  const orig = makeFacts();
  const symGoal = (...query: Term[]): Goal => {
    if (query.length === 2) {
      return or(orig(...query), orig(query[1], query[0]));
    } else {
      return orig(...query);
    }
  };
  symGoal.set = orig.set;
  symGoal.raw = orig.raw;
  symGoal.indexes = orig.indexes;
  // Object.assign(symGoal, orig);
  return symGoal as FactRelation;
}

// --- Symmetric object-based fact relation (query-time symmetry) ---
export function makeFactsObjSym(keys: string[]): FactObjRelation {
  const orig = makeFactsObj(keys);
  const symGoal = (queryObj: Record<string, Term>): Goal => {
    if (keys.length === 2) {
      const [k1, k2] = keys;
      const swapped: Record<string, Term> = {};
      swapped[k1] = queryObj[k2];
      swapped[k2] = queryObj[k1];
      return or(orig(queryObj), orig(swapped));
    } else {
      return orig(queryObj);
    }
  };
  Object.assign(symGoal, orig);
  return symGoal as FactObjRelation;
}

// --- Helpers ---

/**
 * Returns the intersection of two sets.
 */
export function intersect<F>(set_a: Set<F>, set_b: Set<F>): Set<F> {
  const set_n = new Set<F>();
  set_a.forEach(item => {
    if (set_b.has(item)) {
      set_n.add(item);
    }
  });
  return set_n;
}

/**
 * Returns true if a value is indexable (string, number, boolean, or null).
 */
export function isIndexable(v: any): boolean {
  return typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null;
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
      groupVars.forEach((v, i) => s2.set(v.id, groupValues[i]));
      aggVars.forEach((v, i) => s2.set(v.id, aggArrays[i].map(async x => await x)));
      yield s2;
    }
  };
}
