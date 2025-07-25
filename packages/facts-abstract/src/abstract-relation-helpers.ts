import type { Term, Subst } from "logic";
import { isVar, walk, unify } from "logic";
import { queryUtils } from "logic";
import type { GoalRecord, WhereCondition, QueryParams, DataRow } from "./types.js"

/**
 * Helper functions for AbstractRelation
 * Extracted to keep the main class focused
 */

/**
 * Check if one goal could benefit from cached data of another goal
 */
export function couldBenefitFromCache(myGoal: GoalRecord, otherGoal: GoalRecord, subst: Subst): string {
  if (myGoal.relationIdentifier !== otherGoal.relationIdentifier) {
    return "different_relation";
  }

  // // Check if relation options are compatible for caching
  // // For REST APIs, different pathTemplates mean different endpoints
  // const myOptions = myGoal.relationOptions as any;
  // const otherOptions = otherGoal.relationOptions as any;
  
  // if (myOptions?.pathTemplate !== otherOptions?.pathTemplate) {
  //   return "different_path_template";
  // }
  
  // // Add other option compatibility checks as needed
  // if (JSON.stringify(myOptions) !== JSON.stringify(otherOptions)) {
  //   return "incompatible_options";
  // }

  const myColumns = Object.keys(myGoal.queryObj);
  const otherColumns = Object.keys(otherGoal.queryObj);

  let matches = 0;

  for (const column of myColumns) {
    if (otherColumns.includes(column)) {
      const myValueRaw = myGoal.queryObj[column];
      const otherValueRaw = otherGoal.queryObj[column];
      const myValue = walk(myValueRaw, subst);
      const otherValue = walk(otherValueRaw, subst);

      if (!isVar(myValue)) {
        if (!isVar(otherValue)) {
          if (myValue === otherValue) {
            matches++;
          } else {
            return "value_not_match";
          }
        } else {
          return "term_to_var";
        }
      } else {
        if (isVar(otherValue)) {
          matches++;
        } else {
          return "var_to_term";
        }
      }
    }
  }

  if (matches > 0) {
    return "match";
  }
  return "no_matches";
}

/**
 * Check if two goals can have their queries merged
 */
export function canMergeQueries(goalA: GoalRecord, goalB: GoalRecord): boolean {
  const aColumns = Object.keys(goalA.queryObj);
  const bColumns = Object.keys(goalB.queryObj);
  
  if (aColumns.length !== bColumns.length) {
    return false;
  }
  
  if (!aColumns.every(col => bColumns.includes(col))) {
    return false;
  }
  
  for (const column of aColumns) {
    const aValue = goalA.queryObj[column];
    const bValue = goalB.queryObj[column];
    
    if (isVar(aValue) && isVar(bValue)) {
      if (aValue.id !== bValue.id) {
        return false;
      }
    } else if (isVar(aValue) || isVar(bValue)) {
      return false;
    } else {
      if (aValue !== bValue) {
        return false;
      }
    }
  }
  
  return true;
}

/**
 * Collect WHERE clauses from a set of goals for merging
 */
export async function collectAllWhereClauses(goals: GoalRecord[], s: Subst): Promise<Record<string, Set<any>>> {
  const allWhereClauses: Record<string, Set<any>> = {};
  for (const goal of goals) {
    const whereCols = queryUtils.onlyGrounded(goal.queryObj);
    for (const [col, value] of Object.entries(whereCols)) {
      if (!allWhereClauses[col]) allWhereClauses[col] = new Set();
      allWhereClauses[col].add(value);
    }
  }
  return allWhereClauses;
}

/**
 * Collect WHERE clauses from substitutions for batching
 */
export async function collectWhereClausesFromSubstitutions(
  queryObj: Record<string, Term>,
  substitutions: Subst[]
): Promise<Record<string, Set<any>>> {
  const whereClauses: Record<string, Set<any>> = {};
  for (const subst of substitutions) {
    const walked = await queryUtils.walkAllKeys(queryObj, subst);
    const whereCols = queryUtils.onlyGrounded(walked);
    for (const [col, value] of Object.entries(whereCols)) {
      if (!whereClauses[col]) whereClauses[col] = new Set();
      whereClauses[col].add(value);
    }
  }
  return whereClauses;
}

/**
 * Collect all columns needed for query from goals
 */
export function collectColumnsFromGoals(
  myQueryObj: Record<string, Term>,
  cacheCompatibleGoals: GoalRecord[],
  mergeCompatibleGoals?: GoalRecord[]
): { columns: string[], additionalColumns: string[] } {
  const allGoalColumns = new Set<string>();
  
  // Add columns from current goal
  Object.keys(myQueryObj).forEach(col => allGoalColumns.add(col));
  
  // Add columns from merge-compatible goals
  if (mergeCompatibleGoals) {
    for (const goal of mergeCompatibleGoals) {
      Object.keys(goal.queryObj).forEach(col => allGoalColumns.add(col));
    }
  }
  
  // Add columns from cache-compatible goals
  for (const cacheGoal of cacheCompatibleGoals) {
    Object.keys(cacheGoal.queryObj).forEach(col => allGoalColumns.add(col));
  }
  
  const additionalColumns: string[] = []; // Could be from options
  const columns = [...new Set([...allGoalColumns, ...additionalColumns])];
  
  return {
    columns,
    additionalColumns 
  };
}

/**
 * Build WHERE conditions from clause sets
 */
export function buildWhereConditions(whereClauses: Record<string, Set<any>>): WhereCondition[] {
  const conditions: WhereCondition[] = [];
  
  for (const [column, values] of Object.entries(whereClauses)) {
    if (values.size === 1) {
      conditions.push({
        column,
        operator: 'eq',
        value: Array.from(values)[0]
      });
    } else if (values.size > 1) {
      conditions.push({
        column,
        operator: 'in',
        value: null,
        values: Array.from(values)
      });
    }
  }
  
  return conditions;
}

/**
 * Format query parameters for logging
 */
export function sssformatQueryForLog(params: QueryParams): string {
  let query = `SELECT ${params.selectColumns.join(', ')} FROM ${params.relationIdentifier}`;
  
  if (params.whereConditions.length > 0) {
    const whereClause = params.whereConditions.map(cond => {
      if (cond.operator === 'in' && cond.values) {
        return `${cond.column} IN (${cond.values.map(v => typeof v === 'string' ? `'${v}'` : v).join(', ')})`;
      } else {
        const value = typeof cond.value === 'string' ? `'${cond.value}'` : cond.value;
        return `${cond.column} = ${value}`;
      }
    }).join(' AND ');
    query += ` WHERE ${whereClause}`;
  }
  
  if (params.limit) {
    query += ` LIMIT ${params.limit}`;
  }
  
  if (params.offset) {
    query += ` OFFSET ${params.offset}`;
  }
  
  return query;
}

/**
 * Unify a data row with a query object
 */
export function unifyRowWithQuery(row: DataRow, queryObj: Record<string, Term>, s: Subst): Subst | null {
  let result = s;
  for (const [column, term] of Object.entries(queryObj)) {
    const value = row[column];
    if (value === undefined) continue;
    const unified = unify(term, value, result);
    if (unified === null) {
      return null;
    }
    result = unified;
  }
  return result;
}