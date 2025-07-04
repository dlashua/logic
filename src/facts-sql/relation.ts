import { setTimeout as sleep } from "timers/promises";
import type { Term, Subst, Goal } from "../core/types.ts";
import { unify, isVar, walk } from "../core/kernel.ts";
import { Logger, getDefaultLogger } from "../shared/logger.ts";
import { queryUtils } from "../shared/utils.ts";
import { SimpleObservable } from "../core/observable.ts";
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

    // const commonGoals = await this.processGoal(myGoal, s);
    const commonGoals = [];

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
        let query = this.dbObj.db(`${myGoal.table} as t0`).distinct();
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
              ).distinct();
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
        query = query.distinct().select(selectCols);

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
        let rowcnt = 0;
        const maxAlias = aliasCounter;
        for (const row of rows) {
          rowcnt++;
          let goalcnt = 0;
          for (const goal of allGoals) {
            goalcnt++;
            const goalmod = maxAlias - goalcnt;
            this.logger.log("ROW_SKIPPER", {
              goalId: goal.goalId,
              goalcnt,
              rowcnt,
              maxAlias,
              mod: maxAlias + 1 - goalcnt,
              ans: rowcnt % (maxAlias + 1 - goalcnt),
            })
            if(rowcnt % (maxAlias + 1 - goalcnt) !== 0) continue; 

            const alias = goalAliasMap[goal.goalId];
            const newRow = {};
            if (!goalRows[goal.goalId]) goalRows[goal.goalId] = [];
            for (const col of Object.keys(goal.queryObj)) {
              // Use the variable id as the value for this goal's column
              const varId = (goal.queryObj[col] as any)?.id;
              // The value from the SQL row
              const rowValue = row[`${alias}__${col}`];
              newRow[col] = rowValue;
            }
            goalRows[goal.goalId].push(newRow);
          }
        }

        // Store results for other goals in their caches
        const passed = this.getForwardPass(s);
        for (const goal of allGoals) {
          if (goal.goalId !== goalId && goalRows[goal.goalId]) {
            passed.set(goal.goalId, goalRows[goal.goalId]);
          }
        }

        this.logger.log("GOAL_ROWS", {
          goalId,
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

    return (s: Subst) => new SimpleObservable<Subst>((observer) => {
      let cancelled = false;
      
      this.logger.log("GOAL_STARTED", {
        goalId,
        table: this.table,
        queryObj,
        s,
      });

      const processQuery = async () => {
        try {
          const new_s = new Map(s);
          const old_pass = s.get(ROW_CACHE) || new Map();
          new_s.set(ROW_CACHE, new Map(old_pass))
          const rows = await this.cacheOrQuery(goalId, queryObj, new_s) || [];

          let yielded = 0;
          for (let i = 0; i < rows.length; i++) {
            if (cancelled) break;
            
            const row = rows[i];
            const unifiedSubst = await this.unifyRowWithQuery(row, queryObj, new_s);
            if (unifiedSubst && !cancelled) {
              this.logger.log("UNIFY_SUCCESS", {
                goalId,
                queryObj,
                row,
                unifiedSubst,
              });
              
              yielded++;
              observer.next(new Map(unifiedSubst));
            } else if (!cancelled) {
              this.logger.log("UNIFY_FAILED", {
                goalId,
                queryObj,
                row,
              });
            }
            
            // Yield control periodically to allow cancellation
            if (i % 10 === 0) {
              await new Promise(resolve => queueMicrotask(resolve));
            }
          }

          if (!cancelled) {
            observer.complete?.();
          }
        } catch (error) {
          if (!cancelled) {
            observer.error?.(error);
          }
        }
      };

      processQuery();

      return () => {
        cancelled = true;
      };
    });
  }

  private async executeQuery(goalId: number, queryObj: Record<string, Term>, s: Subst): Promise<any[]> {
    const walkedQuery = await queryUtils.walkAllKeys(queryObj, s);
    const whereCols = queryUtils.onlyGrounded(walkedQuery);

    const query = this.buildQuery(queryObj, whereCols);
    const sqlString = query.toString();
    
    this.dbObj.addQuery(sqlString);
    this.logger.log("DB_QUERY", {
      table: this.table,
      sql: sqlString,
      goalId,
    });

    const rows = await query;
    
    if (rows.length) {
      this.logger.log("DB_ROWS", {
        table: this.table,
        sql: sqlString,
        goalId,
        rows,
      });
    } else {
      this.logger.log("NO_DB_ROWS", {
        table: this.table,
        sql: sqlString,
        goalId,
        rows,
      });
    }

    return rows;
  }

  private buildQuery(queryObj: Record<string, Term>, whereCols: Record<string, any>) {
    let query = this.dbObj.db(this.table);
    
    // Add WHERE clauses for grounded values
    for (const [column, value] of Object.entries(whereCols)) {
      query = query.where(column, value);
    }

    // Select all columns if no specific columns are configured
    if (this.options?.selectColumns) {
      query = query.select(this.options.selectColumns);
    } else {
      query = query.select('*');
    }

    return query;
  }

  private async unifyRowWithQuery(row: any, queryObj: Record<string, Term>, s: Subst): Promise<Subst | null> {
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
}