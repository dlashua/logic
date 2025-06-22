import { Term, Subst, walk, arrayToLogicList, unify } from './core.ts';
import { Goal, eq } from './relations.ts';

/**
 * Helper: group by key for logic goals, then apply a callback per group.
 * Calls cb(s, key, items) for each group, where key is the group key and items is the array of values.
 */
export async function* groupByGoal(
    keyVar: Term,
    valueVar: Term,
    goal: Goal,
    s: Subst,
    cb: (s: Subst, key: any, items: any[]) => AsyncGenerator<Subst>
): AsyncGenerator<Subst> {
    // Collect all (key, value) pairs
    const pairs: { key: any, value: any }[] = [];
    for await (const s1 of goal(s)) {
        const key = await walk(keyVar, s1);
        const value = await walk(valueVar, s1);
        pairs.push({ key, value });
    }
    // Group by key
    const grouped = new Map<any, { key: any, items: any }>();
    for (const { key, value } of pairs) {
        const k = JSON.stringify(key); // Use JSON.stringify for deep equality
        if (!grouped.has(k)) grouped.set(k, { key, items: [] });
        const group = grouped.get(k);
        if (group) group.items.push(value);
    }
    // For each group, yield using the callback
    for (const { key, items } of grouped.values()) {
        for await (const s2 of cb(s, key, items)) {
            yield s2;
        }
    }
}

/**
 * groupAggregateo(keyVar, valueVar, goal, outKey, outAgg, aggFn):
 * For each group, yields a substitution with outKey = group key and outAgg = aggFn(items).
 * aggFn receives the array of values for the group and returns the aggregate result.
 */
export function groupAggregateo(
    keyVar: Term,
    valueVar: Term,
    goal: Goal,
    outKey: Term,
    outAgg: Term,
    aggFn: (items: any[]) => any
): Goal {
    return async function* (s: Subst) {
        yield* groupByGoal(keyVar, valueVar, goal, s, async function* (s, key, items) {
            const agg = aggFn(items);
            for await (const s2 of eq(outKey, key)(s)) {
                for await (const s3 of eq(outAgg, agg)(s2)) {
                    yield s3;
                }
            }
        });
    };
}

/**
 * aggregateRel(aggFn): returns a group-by aggregation goal constructor.
 * The returned function has signature (keyVar, valueVar, goal, outKey, outAgg) => Goal
 * Example: const groupCollecto = aggregateRel(arrayToLogicList)
 */
export function aggregateRel(aggFn: (items: any[]) => any) {
    return function (keyVar: Term, valueVar: Term, goal: Goal, outKey: Term, outAgg: Term): Goal {
        return groupAggregateo(keyVar, valueVar, goal, outKey, outAgg, aggFn);
    };
}

export const groupCollecto = aggregateRel(arrayToLogicList);
export const groupCounto = aggregateRel((items: any[]) => items.length);
