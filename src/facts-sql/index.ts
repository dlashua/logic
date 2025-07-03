import knex from "knex";
import type { Knex } from "knex";
import { Logger, getDefaultLogger } from "../shared/logger.ts";
import type { BaseConfig as Configuration } from "../shared/types.ts";
import type { Term, Goal } from "../core/types.ts";
import { or } from "../core/combinators.ts";
import { RegularRelationWithMerger } from "./relation.ts";
import type { RelationOptions } from "./types.ts";

export type DBManager = Awaited<ReturnType<typeof createDBManager>>;
export async function createDBManager (
  knex_connect_options: Knex.Config,
  logger: Logger,
  options?: RelationOptions,
) {
  const db = knex(knex_connect_options);
  const queries: string[] = [];
  const goals: { goalId: number; table: string; queryObj: Record<string, Term> }[] = [];
  const pendingQueries = new Map<string, {goalId: number, queryObj: Record<string, Term>, whereCols: Record<string, Term>}[]>();
  let nextGoalId = 0;
    
  return {
    db,
    addQuery: (q: string) => queries.push(q),
    getQueries: () => queries,
    clearQueries: () => queries.splice(0, queries.length),
    getQueryCount: () => queries.length,
    getNextGoalId: () => ++nextGoalId,
    addGoal: (goalId: number, table: string, queryObj: Record<string, Term>) => goals.push({
      goalId,
      table,
      queryObj 
    }),
    getGoals: () => goals,
    clearGoals: () => goals.splice(0, goals.length),
    findGoalsWithSharedKeys: (goalId: number) => {
      const targetGoal = goals.find(goal => goal.goalId === goalId);
      if (!targetGoal) return [];
      
      return goals.filter(goal => {
        if (goal.goalId === goalId) return false; // exclude itself
        
        // Only consider joins that are actually beneficial:
        // 1. Different tables (cross-table joins)
        // 2. Same table with different logic variables that can be correlated
        
        if (goal.table !== targetGoal.table) {
          // Different tables - check for shared variables
          for (const [key, term] of Object.entries(targetGoal.queryObj)) {
            if (key in goal.queryObj && goal.queryObj[key] === term) {
              return true;
            }
          }
        } else {
          // Same table - check if they share variables (for column expansion)
          for (const [key, term] of Object.entries(targetGoal.queryObj)) {
            if (key in goal.queryObj && goal.queryObj[key] === term) {
              return true; // They share a variable, so target goal can fetch additional columns
            }
          }
          return false;
        }
        
        return false;
      });
    },
    addPendingQuery: (table: string, goalId: number, queryObj: Record<string, Term>, whereCols: Record<string, Term>) => {
      const key = table;
      if (!pendingQueries.has(key)) {
        pendingQueries.set(key, []);
      }
      pendingQueries.get(key)!.push({ goalId, queryObj, whereCols });
      
      if (goalId === 2) {
        logger.log("PENDING_ADD", `Added Goal ${goalId} to pending. Total pending: ${pendingQueries.get(key)!.length}`);
      }
    },
    findPendingQueries: (table: string, goalId: number, queryObj: Record<string, Term>, whereCols: Record<string, Term>) => {
      const key = table;
      const pending = pendingQueries.get(key) || [];
      
      // Find queries with the same pattern (same queryObj structure and same WHERE columns)
      if (goalId === 2) {
        logger.log("MERGE_DEBUG", `Goal ${goalId}: Checking ${pending.length} pending queries: ${pending.map(p => `Goal${p.goalId}(${Object.keys(p.queryObj).join(',')})`).join(', ')}`);
      }
      
      const mergeable = pending.filter(p => {
        if (p.goalId === goalId) return false; // exclude self
        
        // Check if they have the same query structure (same columns)
        const pKeys = Object.keys(p.queryObj).sort();
        const currentKeys = Object.keys(queryObj).sort();
        
        if (pKeys.length !== currentKeys.length) {
          if (goalId === 2) {
            logger.log("MERGE_DEBUG", `Goal ${goalId}: Different query key lengths - pending: [${pKeys.join(',')}], current: [${currentKeys.join(',')}]`);
          }
          return false;
        }
        
        for (let i = 0; i < pKeys.length; i++) {
          if (pKeys[i] !== currentKeys[i]) {
            if (goalId === 2) {
              logger.log("MERGE_DEBUG", `Goal ${goalId} vs Goal ${p.goalId}: Different query keys at index ${i} - pending: ${pKeys[i]}, current: ${currentKeys[i]}`);
            }
            return false;
          }
        }
        
        // Check if they have the same WHERE column structure
        const pWhereKeys = Object.keys(p.whereCols).sort();
        const currentWhereKeys = Object.keys(whereCols).sort();
        
        if (pWhereKeys.length !== currentWhereKeys.length) {
          if (goalId === 2) {
            logger.log("MERGE_DEBUG", `Goal ${goalId}: Different WHERE key lengths - pending: [${pWhereKeys.join(',')}], current: [${currentWhereKeys.join(',')}]`);
          }
          return false;
        }
        
        for (let i = 0; i < pWhereKeys.length; i++) {
          if (pWhereKeys[i] !== currentWhereKeys[i]) {
            if (goalId === 2) {
              logger.log("MERGE_DEBUG", `Goal ${goalId}: Different WHERE keys at index ${i} - pending: ${pWhereKeys[i]}, current: ${currentWhereKeys[i]}`);
            }
            return false;
          }
        }
        
        // If we reach here, it should be mergeable
        if (goalId === 2) {
          logger.log("MERGE_DEBUG", `Goal ${goalId}: MERGEABLE with goal ${p.goalId}`);
        }
        
        return true;
      });
      
      // Debug logging
      if (pending.length > 0) {
        logger.log("PENDING_QUERIES_DEBUG", {
          table,
          goalId,
          totalPending: pending.length,
          mergeableFound: mergeable.length,
          allPendingGoals: pending.map(p => p.goalId),
          mergeableGoals: mergeable.map(m => m.goalId),
          currentQueryObj: queryObj,
          currentWhereCols: whereCols,
          pendingDetails: pending.map(p => ({
            goalId: p.goalId,
            queryObj: p.queryObj,
            whereCols: p.whereCols
          }))
        });
      }
      
      return mergeable;
    },
    clearPendingQueries: (table: string) => {
      pendingQueries.delete(table);
    },
  };
}

export const makeRelDB = async (
  knex_connect_options: Knex.Config,
  options?: Record<string, string>,
  configOverrides?: Partial<Configuration>,
) => {
  options ??= {};
  
  const logger = getDefaultLogger();
  const dbManager = await createDBManager(knex_connect_options, logger, options);

  function createRelation(table: string, options?: RelationOptions) {
    const relation = new RegularRelationWithMerger(
      dbManager,
      table,
      logger,
      options,
    );

    return (queryObj: Record<string, Term>): Goal => {
      return relation.createGoal(queryObj);
    };
  }

  function logic_createSymmetricRelation(table: string, keys: [string, string], options?: RelationOptions) {
    const relation = new RegularRelationWithMerger(
      dbManager,
      table,
      logger,
      options,
    );
  
    return (queryObj: Record<string, Term>): Goal => {
      const queryObjSwapped = {
        [keys[0]]: queryObj[keys[1]],
        [keys[1]]: queryObj[keys[0]],
      };
      return or(
        relation.createGoal(queryObj),
        relation.createGoal(queryObjSwapped),
      );
    };
  }
  
  return {
    rel: createRelation,
    relSym: logic_createSymmetricRelation,
    db: dbManager.db,
    getQueries: () => dbManager.getQueries(),
    clearQueries: () => dbManager.clearQueries(),
    getQueryCount: () => dbManager.getQueryCount(),
  };
};