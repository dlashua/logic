import knex from "knex";
import type { Knex } from "knex";
import { ConfigurationManager } from "../shared/config.ts";
import { SimpleLogger, getDefaultLogger } from "../shared/simple-logger.ts";
import type { BaseConfig as Configuration } from "../shared/types.ts";
import type { Term, Goal } from "../core/types.ts";
import { or } from "../core/combinators.ts";
import { RegularRelationWithMerger } from "./relation.ts";
import type { RelationOptions } from "./types.ts";

export type DBManager = Awaited<ReturnType<typeof createDBManager>>;
export async function createDBManager (
  knex_connect_options: Knex.Config,
  logger: SimpleLogger,
  options?: RelationOptions,
) {
  const db = knex(knex_connect_options);
  const queries: string[] = [];
  const goals: { goalId: number; table: string; queryObj: Record<string, Term> }[] = [];
  const storedQueries = new Map<string, { rows: any[] }>();
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
        
        for (const [key, term] of Object.entries(targetGoal.queryObj)) {
          if (key in goal.queryObj && goal.queryObj[key] === term) {
            return true;
          }
        }
        return false;
      });
    },
    storeQueryByKey: (cacheKey: string, rows: any[]) => {
      storedQueries.set(cacheKey, {
        rows 
      });
      logger.log("STORED_QUERY", {
        cacheKey,
        rows,
      })
    },
    findStoredQueryByKey: (cacheKey: string) => {
      const data = storedQueries.get(cacheKey);
      if (data) {
        return {
          cacheKey,
          rows: data.rows
        };
      }
      return null;
    },
    getStoredQueries: () => storedQueries,
    clearStoredQueries: () => storedQueries.clear(),

  }
}

export const makeRelDB = async (
  knex_connect_options: Knex.Config,
  options?: Record<string, string>,
  configOverrides?: Partial<Configuration>,
) => {
  options ??= {};

  // Create configuration
  const config = ConfigurationManager.create(configOverrides);
  
  // Create core dependencies
  const logger = getDefaultLogger();
  
  const dbManager = await createDBManager(knex_connect_options, logger, options)


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
      )
    };
  }
  
  // function sql_createSymmetricRelation(table: string, keys: [string, string], options?: RelationOptions) {
  //   // Use the new symmetric relation implementation with merger
  //   const symmetricRelation = new SymmetricRelationWithMerger(
  //     dbManager,
  //     table,
  //     keys,
  //     logger,
  //     options,
  //   );
  
  //   return (queryObj: Record<string, Term>): Goal => {
  //     return symmetricRelation.createGoal(queryObj);
  //   };
  // }

  return {
    rel: createRelation,
    relSym: logic_createSymmetricRelation,
    db: dbManager.db,
    getQueries: () => dbManager.getQueries(),
    clearQueries: () => dbManager.clearQueries(),
    getQueryCount: () => dbManager.getQueryCount(),
  };
};
