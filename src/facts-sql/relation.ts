import { setTimeout as sleep } from "timers/promises";
import type { Term, Subst, Goal } from "../core/types.ts";
import { unify, isVar, walk } from "../core/kernel.ts";
import { Logger, getDefaultLogger } from "../shared/logger.ts";
import { queryUtils } from "../shared/utils.ts";
import { RelationOptions } from "./types.ts";
import type { DBManager, GoalRecord } from "./index.ts";

const JOINS_ENABLED = true;
const ROW_CACHE = Symbol.for("sql-row-cache");

export class RegularRelationWithMerger {
  private logger: Logger;
  private primaryKey?: string;

  constructor(
    private dbObj: DBManager,
    private table: string,
    logger?: Logger,
    private options?: RelationOptions,
  ) {
    this.logger = logger ?? getDefaultLogger();
    this.primaryKey = options?.primaryKey; // No default - honor the idea of no primary key
  }

  haveAtLeastOneMatchingVar(a: GoalRecord, b: GoalRecord) {
    const aVarIds = Object.values(queryUtils.onlyVars(a.queryObj)).map(x => x.id);
    const bVarIds = Object.values(queryUtils.onlyVars(b.queryObj)).map(x => x.id);
    const matchingIds = aVarIds.filter(av => bVarIds.includes(av));
    if(matchingIds.length === 0) {
      return null
    }
    return {
      goal: b,
      matchingIds,
    }
  }

  async processGoal(myGoal: GoalRecord, s: Subst) {
    const otherGoals = this.dbObj.getGoals().filter(x => x.goalId !== myGoal.goalId);
    return otherGoals.map(x => this.haveAtLeastOneMatchingVar(myGoal, x)).filter(x => x !== null);
  }

  getForwardPass(s: Map<string | Symbol, any>) {
    if(!s.has(ROW_CACHE)) {
      s.set(ROW_CACHE, new Map());
    }
    return s.get(ROW_CACHE) as Map<number, Record<string, any>>;
  }

  getCache(goalId: number, s: Subst) {
    const passed = this.getForwardPass(s);
    if(passed.has(goalId)) {
      const cache = passed.get(goalId) as Record<string, any>[];
      passed.delete(goalId);
      return cache;
    }
    return null;
  }

  async cacheOrQuery(goalId: number, queryObj: Record<string, Term>, s: Subst) {
    const cache = this.getCache(goalId, s);
    this.logger.log("SAW_CACHE", {
      goalId,
      cache,
    });
    if(cache) {
      return cache;
    }

    const myGoal = this.dbObj.getGoalById(goalId);
    if(!myGoal) return [];

    const commonGoals = await this.processGoal(myGoal, s);

    this.logger.log("COMMON_GOALS", {
      myGoal,
      commonGoals,
    });

    // Combine all joinable goals (including myGoal) into one large query with outer joins on matching variable columns.
    if (commonGoals && commonGoals.length > 0) {
      // Only consider goals with matchingIds
      const joinableGoals = commonGoals.filter(g => g && g.matchingIds && g.matchingIds.length > 0);
      if (joinableGoals.length > 0) {
      // Build a single query with outer joins for all joinable goals
        let query = this.dbObj.db(`${myGoal.table} as t0`);
        const baseAlias = "t0";
        const aliases = [baseAlias];
        let aliasCounter = 1;

        // Map of goalId to alias
        const goalAliasMap: Record<number, string> = {
          [myGoal.goalId]: baseAlias 
        };

        // Join each joinable goal
        for (const goalInfo of joinableGoals) {
          const otherGoal = goalInfo.goal;
          const otherTable = otherGoal.table;
          const otherAlias = `t${aliasCounter++}`;
          goalAliasMap[otherGoal.goalId] = otherAlias;
          aliases.push(otherAlias);

          // Join on all matching variable columns
          for (const varId of goalInfo.matchingIds) {
            // Find the column names in both tables for this varId
            const myCol = Object.entries(myGoal.queryObj).find(([k, v]) => v.id === varId)?.[0];
            const otherCol = Object.entries(otherGoal.queryObj).find(([k, v]) => v.id === varId)?.[0];
            if (myCol && otherCol) {
              query = query.leftJoin(
                `${otherTable} as ${otherAlias}`,
                `${baseAlias}.${myCol}`,
                `${otherAlias}.${otherCol}`
              );
            }
          }
        }

        // Collect all columns from all involved goals
        const allGoals = [myGoal, ...joinableGoals.map(g => g.goal)];
        const selectCols: string[] = [];
        for (let i = 0; i < allGoals.length; ++i) {
          const goal = allGoals[i];
          const alias = goalAliasMap[goal.goalId];
          for (const col of Object.keys(goal.queryObj)) {
            selectCols.push(`${alias}.${col} as ${alias}__${col}`);
          }
        }
        query = query.select(selectCols);

        // Add WHERE clauses for grounded terms from all goals
        for (const goal of allGoals) {
          const alias = goalAliasMap[goal.goalId];
          const walked = await queryUtils.walkAllKeys(goal.queryObj, s);
          const whereCols = queryUtils.onlyGrounded(walked);
          for (const [col, value] of Object.entries(whereCols)) {
            query = query.where(`${alias}.${col}`, value);
          }
        }

        const sqlString = query.toString();
        this.dbObj.addQuery(sqlString);
        this.logger.log("DB_QUERY", {
          table: myGoal.table,
          sql: sqlString,
          goalId,
        });
        const rows = await query;
        if (rows.length) {
          this.logger.log("DB_ROWS", {
            table: myGoal.table,
            sql: sqlString,
            goalId,
            rows,
          });
        } else {
          this.logger.log("NO_DB_ROWS", {
            table: myGoal.table,
            sql: sqlString,
            goalId,
            rows,
          });
        }

        // map each row into an object keyed by goalId filled with the keys that goal cares about with those keys equal to the Var Id in the goal
        const goalRows: Record<number, Record<string, any>> = {};
        for (const goal of allGoals) {
          const alias = goalAliasMap[goal.goalId];
          for (const row of rows) {
            if (!goalRows[goal.goalId]) goalRows[goal.goalId] = [];
            const newRow = {};
            for (const col of Object.keys(goal.queryObj)) {
              // Use the variable id as the value for this goal's column
              const varId = (goal.queryObj[col] as any)?.id;
              // The value from the SQL row
              const value = row[`${alias}__${col}`];
              // Store as: key = col, value = value from row
              newRow[col] = value;
              // Optionally, you could also store the varId mapping if needed
              // goalRows[goal.goalId][col + "_varId"] = varId;
            }
            goalRows[goal.goalId].push(newRow);
          }
        }
        // Optionally, store in the forward pass cache for later use
        if (!s.has(ROW_CACHE)) {
          s.set(ROW_CACHE, new Map());
        }
        const cache = s.get(ROW_CACHE) as Map<number, Record<string, any>[]>;
        for (const oneGoalId of Object.keys(goalRows)) {
          const id = Number(oneGoalId);
          if (id === goalId) continue;
          if (!cache.has(id)) cache.set(id, []);
          goalRows[id].forEach(x => cache.get(id)!.push(x));
        }

        this.logger.log("ALL_GOAL_ROWS", {
          goalId,
          sql: sqlString,
          s,
        })

        this.logger.log("THIS_GOAL_ROWS", {
          goalId,
          sql: sqlString,
          rows: goalRows[goalId],
        })

        return goalRows[goalId];
      }
    }

    const rows = await this.executeQuery(goalId, queryObj, s);
    return rows;

  }

  createGoal(queryObj: Record<string, Term>): Goal {
    const goalId = this.dbObj.getNextGoalId();
    this.dbObj.addGoal(goalId, this.table, queryObj);
    
    this.logger.log("GOAL_CREATED", {
      goalId,
      table: this.table,
      queryObj
    });

    return async function* factsSql(this: RegularRelationWithMerger, s: Subst) {
      this.logger.log("GOAL_STARTED", {
        goalId,
        table: this.table,
        queryObj,
        s,
      });
      const new_s = new Map(s);
      const old_pass = s.get(ROW_CACHE) || new Map();
      new_s.set(ROW_CACHE, new Map(old_pass))
      const rows = await this.cacheOrQuery(goalId, queryObj, new_s);

      let yielded = 0;
      for (const row of rows) {
        const unifiedSubst = await this.unifyRowWithQuery(row, queryObj, new_s);
        if (unifiedSubst) {
          this.logger.log("UNIFY_SUCCESS", {
            goalId,
            queryObj,
            row,
            unifiedSubst,
          });
          
          yielded++;
          yield new Map(unifiedSubst);
        } else {
          this.logger.log("UNIFY_FAILED", {
            goalId,
            queryObj,
            row,
          });
        }
      }
    }.bind(this);
  }

  private async executeQuery(goalId: number, queryObj: Record<string, Term>, s: Subst): Promise<any[]> {
    // const sharedGoals = this.dbObj.findGoalsWithSharedKeys(goalId);
    // const joinStructure = this.createJoinStructure(goalId, queryObj, sharedGoals);

    // this.logger.log("SHARED_GOALS", {
    //   goalId,
    //   sharedGoals,
    //   joinStructure,
    //   allGoals: this.dbObj.getGoals().map(g => ({
    //     goalId: g.goalId,
    //     table: g.table,
    //     queryObj: g.queryObj 
    //   }))
    // });

    // const walkedQuery: Record<string, Term> = {};
    // const selectCols: Record<string, Term> = {};
    // const whereCols: Record<string, Term> = {};
    
    // for (const [column, term] of Object.entries(queryObj)) {
    //   const walkedTerm = await walk(term, s);
    //   walkedQuery[column] = walkedTerm;
      
    //   if (isVar(walkedTerm)) {
    //     selectCols[column] = walkedTerm;
    //   } else {
    //     whereCols[column] = walkedTerm;
    //   }
    // }

    // Check if this query can be merged with pending queries
    // const mergeableQueries = await this.findMergeableQueries(goalId, queryObj, whereCols);
    // if (mergeableQueries.length > 0) {
    //   return await this.executeMergedQuery(goalId, queryObj, whereCols, mergeableQueries, joinStructure);
    // }

    const walkedQuery = await queryUtils.walkAllKeys(queryObj, s);
    const whereCols = queryUtils.onlyGrounded(walkedQuery);

    const query = this.buildQuery(queryObj, whereCols);
    const sqlString = query.toString();
    
    this.dbObj.addQuery(`goalId:${goalId} - ${sqlString}`);
    this.logger.log("DB_QUERY", {
      table: this.table,
      sql: sqlString,
      goalId,
    });
    const rows = await query;
    
    if (rows.length) {
      this.logger.log("DB_ROWS", {
        table: this.table,
        sql: query.toString(),
        goalId,
        rows,
      });
    } else {
      this.logger.log("NO_DB_ROWS", {
        table: this.table,
        sql: query.toString(),
        goalId,
        rows,
      });
    }

    return rows;
  }

  private async findMergeableQueries(goalId: number, queryObj: Record<string, Term>, whereCols: Record<string, Term>): Promise<any[]> {
    // Add this query to pending queries
    this.dbObj.addPendingQuery(this.table, goalId, queryObj, whereCols);
    
    // Wait longer for other queries to arrive (extended batching)
    await new Promise(resolve => sleep(resolve, 10));
    
    // Find other pending queries with the same pattern
    const mergeable = this.dbObj.findPendingQueries(this.table, goalId, queryObj, whereCols);
    
    // Only merge if we have at least 2 queries (current + 1 other)
    if (mergeable.length >= 1) {
      this.logger.log("MERGEABLE_CHECK", {
        goalId,
        table: this.table,
        queryObj,
        whereCols,
        mergeableCount: mergeable.length,
        mergeableGoals: mergeable.map(m => m.goalId),
        status: "MERGING"
      });
      
      return mergeable;
    } else {
      this.logger.log("MERGEABLE_CHECK", {
        goalId,
        table: this.table,
        queryObj,
        whereCols,
        mergeableCount: mergeable.length,
        mergeableGoals: [],
        status: "NO_MERGE"
      });
      
      return [];
    }
  }

  private async executeMergedQuery(
    goalId: number, 
    queryObj: Record<string, Term>, 
    whereCols: Record<string, Term>, 
    mergeableQueries: any[], 
    joinStructure: any
  ): Promise<any[]> {
    // Check if we can merge multiple WHERE values for the same column
    const allQueries = [{
      goalId,
      queryObj,
      whereCols 
    }, ...mergeableQueries];
    
    // Find common WHERE columns and group by them
    const commonColumns = this.findCommonWhereColumns(allQueries);
    
    if (commonColumns.length > 0) {
      // We can merge! Create a single query with WHERE IN clause
      const mergedQuery = this.buildMergedQuery(queryObj, allQueries, commonColumns, joinStructure);
      const sqlString = mergedQuery.toString();
      
      this.dbObj.addQuery(`goalId:${goalId} - MERGED(${allQueries.length}) - ${sqlString}`);
      this.logger.log("DB_QUERY", {
        table: this.table,
        sql: sqlString,
        goalId,
        joinStructure,
        note: `merged query for ${allQueries.length} goals`,
        mergedGoals: allQueries.map(q => q.goalId)
      });
      
      const rows = await mergedQuery;
      
      // TODO: Remove only the specific merged queries from pending, not all pending queries
      // this.dbObj.clearPendingQueries(this.table);
      
      return rows;
    } else {
      // Can't merge, execute original query
      const query = this.buildQuery(queryObj, whereCols, joinStructure);
      const sqlString = query.toString();
      
      this.dbObj.addQuery(`goalId:${goalId} - ${sqlString}`);
      this.logger.log("DB_QUERY", {
        table: this.table,
        sql: sqlString,
        goalId,
        joinStructure
      });
      const rows = await query;
      
      return rows;
    }
  }

  private buildQuery(queryObj: Record<string, Term>, whereCols: Record<string, Term>, joinStructure?: any) {
    // Build select columns - include all query columns and primary key if specified
    const selectCols = Object.keys(queryObj);
    
    // Add columns from shared goals that share variables with this goal
    const sharedGoals = joinStructure?.sharedGoals || [];
    for (const sharedGoal of sharedGoals) {
      if (sharedGoal.table === this.table) {
        // Same table - add their columns to our SELECT
        for (const column of Object.keys(sharedGoal.queryObj)) {
          if (!selectCols.includes(column)) {
            selectCols.push(column);
          }
        }
      }
    }
    
    // Ensure primary key is included for fast matching
    if (this.primaryKey && !selectCols.includes(this.primaryKey)) {
      selectCols.push(this.primaryKey);
    }
    
    // If we have joins, we need to use table aliases and qualified column names
    if (joinStructure && joinStructure.joins.length > 0) {
      const baseAlias = this.table;
      
      // Build qualified select columns
      const qualifiedSelectCols = selectCols.map(col => `${baseAlias}.${col}`);
      
      // Build qualified where clauses
      const qualifiedWhereCols: Record<string, any> = {};
      for (const [col, value] of Object.entries(whereCols)) {
        qualifiedWhereCols[`${baseAlias}.${col}`] = value;
      }
      
      let query = this.dbObj.db(`${this.table} as ${baseAlias}`)
        .distinct()
        .select(qualifiedSelectCols)
        .where(qualifiedWhereCols);
      
      query = this.applyJoins(query, baseAlias, joinStructure);
      return query;
    } else {
      // No joins, use simple query
      const query = this.dbObj.db(this.table)
        .distinct()
        .select(selectCols)
        .where(whereCols);
      
      return query;
    }
  }

  private applyJoins(query: any, baseAlias: string, joinStructure: any) {
    let aliasCounter = 1;
    
    for (const join of joinStructure.joins) {
      const alias = `${join.table}_${aliasCounter}`;
      aliasCounter++;
      
      if (join.conditions.length > 0) {
        const firstCondition = join.conditions[0];
        query = query.join(
          `${join.table} as ${alias}`,
          `${baseAlias}.${firstCondition.leftCol}`,
          `${alias}.${firstCondition.rightCol}`
        );
        
        for (let i = 1; i < join.conditions.length; i++) {
          const condition = join.conditions[i];
          query = query.andWhere(
            `${baseAlias}.${condition.leftCol}`,
            '=',
            query.client.raw(`${alias}.${condition.rightCol}`)
          );
        }
      }
    }
    
    return query;
  }

  private createJoinStructure(
    currentGoalId: number,
    currentQueryObj: Record<string, Term>,
    sharedGoals: { goalId: number; table: string; queryObj: Record<string, Term> }[]
  ) {
    if (!JOINS_ENABLED || sharedGoals.length === 0) {
      return {
        baseTable: this.table,
        joins: []
      };
    }

    const joins: {
      table: string;
      conditions: { leftCol: string; rightCol: string; termId: any }[]
    }[] = [];

    for (const sharedGoal of sharedGoals) {
      // Only create joins for different tables (cross-table joins)
      if (sharedGoal.table === this.table) {
        continue; // Skip same-table goals - they won't benefit from joins
      }
      
      const conditions: { leftCol: string; rightCol: string; termId: any }[] = [];
      
      for (const [currentKey, currentTerm] of Object.entries(currentQueryObj)) {
        for (const [sharedKey, sharedTerm] of Object.entries(sharedGoal.queryObj)) {
          if (currentKey === sharedKey && currentTerm === sharedTerm) {
            conditions.push({
              leftCol: currentKey,
              rightCol: sharedKey,
              termId: currentTerm
            });
          }
        }
      }
      
      if (conditions.length > 0) {
        joins.push({
          table: sharedGoal.table,
          conditions
        });
      }
    }

    // Also collect same-table goals that share variables for column expansion
    const sameTableSharedGoals = [];
    for (const sharedGoal of sharedGoals) {
      if (sharedGoal.table === this.table) {
        // Check if they share variables
        const hasSharedVars = Object.entries(currentQueryObj).some(([currentKey, currentTerm]) =>
          Object.entries(sharedGoal.queryObj).some(([sharedKey, sharedTerm]) =>
            currentTerm === sharedTerm // Same variable reference
          )
        );
        
        if (hasSharedVars) {
          sameTableSharedGoals.push(sharedGoal);
        }
      }
    }

    return {
      baseTable: this.table,
      joins,
      sharedGoals: sameTableSharedGoals
    };
  }

  private findCommonWhereColumns(allQueries: any[]): string[] {
    if (allQueries.length < 2) return [];
    
    // Get the WHERE columns from the first query
    const firstWhereColumns = Object.keys(allQueries[0].whereCols);
    
    // Check if all other queries have the same WHERE columns
    const commonColumns = firstWhereColumns.filter(col => {
      return allQueries.every(query => col in query.whereCols);
    });
    
    return commonColumns;
  }

  private buildMergedQuery(
    baseQueryObj: Record<string, Term>, 
    allQueries: any[], 
    commonColumns: string[], 
    joinStructure: any
  ) {
    // Build select columns - include all query columns and primary key if specified
    const selectCols = Object.keys(baseQueryObj);
    
    // Ensure primary key is included for fast matching
    if (this.primaryKey && !selectCols.includes(this.primaryKey)) {
      selectCols.push(this.primaryKey);
    }
    
    // Collect all unique values for each common column
    const columnValueMap: Record<string, Set<any>> = {};
    
    for (const col of commonColumns) {
      columnValueMap[col] = new Set();
      for (const query of allQueries) {
        if (col in query.whereCols) {
          columnValueMap[col].add(query.whereCols[col]);
        }
      }
    }
    
    // If we have joins, we need to use table aliases and qualified column names
    if (joinStructure && joinStructure.joins.length > 0) {
      const baseAlias = this.table;
      
      // Build qualified select columns
      const qualifiedSelectCols = selectCols.map(col => `${baseAlias}.${col}`);
      
      let query = this.dbObj.db(`${this.table} as ${baseAlias}`)
        .distinct()
        .select(qualifiedSelectCols);
      
      // Add WHERE IN clauses for common columns
      for (const col of commonColumns) {
        const values = Array.from(columnValueMap[col]);
        if (values.length > 1) {
          query = query.whereIn(`${baseAlias}.${col}`, values);
        } else {
          query = query.where(`${baseAlias}.${col}`, values[0]);
        }
      }
      
      query = this.applyJoins(query, baseAlias, joinStructure);
      return query;
    } else {
      // No joins, use simple query
      let query = this.dbObj.db(this.table)
        .distinct()
        .select(selectCols);
      
      // Add WHERE IN clauses for common columns
      for (const col of commonColumns) {
        const values = Array.from(columnValueMap[col]);
        if (values.length > 1) {
          query = query.whereIn(col, values);
        } else {
          query = query.where(col, values[0]);
        }
      }
      
      return query;
    }
  }
  
  private async unifyRowWithQuery(
    row: any,
    queryObj: Record<string, Term>,
    s: Subst
  ): Promise<Subst | null> {
    let resultSubst = s;
    
    for (const [col, term] of Object.entries(queryObj)) {
      const rowValue = row[col];
      if (rowValue === undefined) {
        return null;
      }

      const unified = await unify(term, rowValue, resultSubst);
      if (!unified) {
        return null;
      }
      resultSubst = unified;
    }
    
    return resultSubst;
  }
}