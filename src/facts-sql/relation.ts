import type { Term, Subst, Goal } from "../core/types.ts";
import { unify, isVar, walk } from "../core/kernel.ts";
import { Logger, getDefaultLogger } from "../shared/logger.ts";
import { RelationOptions } from "./types.ts";
import type { DBManager } from "./index.ts";

const CACHE_ENABLED = true;
const JOINS_ENABLED = false;

export class RegularRelationWithMerger {
  private logger: Logger;
  private primaryKey: string;

  constructor(
    private dbObj: DBManager,
    private table: string,
    logger?: Logger,
    private options?: RelationOptions,
  ) {
    this.logger = logger ?? getDefaultLogger();
    this.primaryKey = options?.primaryKey || 'id'; // Default to 'id' if not specified
  }

  createGoal(queryObj: Record<string, Term>): Goal {
    const queryKeys = Object.keys(queryObj);
    const goalId = this.dbObj.getNextGoalId();
    this.dbObj.addGoal(goalId, this.table, queryObj);

    return async function* factsSql(this: RegularRelationWithMerger, s: Subst) {
      const rows = await this.cacheOrQuery(goalId, queryObj, s);
      
      let yielded = 0;
      for (const row of rows) {
        const unifiedSubst = await this.unifyRowWithQuery(row, queryObj, s);
        if (unifiedSubst) {
          this.logger.log("UNIFY_SUCCESS", {
            goalId,
            queryObj,
            row,
          });
          
          yielded++;
          yield unifiedSubst;
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

  private async cacheOrQuery(goalId: number, queryObj: Record<string, Term>, s: Subst): Promise<any[]> {
    const sharedGoals = this.dbObj.findGoalsWithSharedKeys(goalId);
    const joinStructure = this.createJoinStructure(goalId, queryObj, sharedGoals);

    this.logger.log("SHARED_GOALS", {
      goalId,
      sharedGoals,
      joinStructure
    });

    const walkedQuery: Record<string, Term> = {};
    const selectCols: Record<string, Term> = {};
    const whereCols: Record<string, Term> = {};
    
    for (const [column, term] of Object.entries(queryObj)) {
      const walkedTerm = await walk(term, s);
      walkedQuery[column] = walkedTerm;
      
      if (isVar(walkedTerm)) {
        selectCols[column] = walkedTerm;
      } else {
        whereCols[column] = walkedTerm;
      }
    }

    // Check row cache for primary key queries
    if (Object.keys(whereCols).length === 1 && this.primaryKey in whereCols && !isVar(whereCols[this.primaryKey])) {
      const pkValue = whereCols[this.primaryKey];
      const tableCache = this.dbObj.getRowCache().get(this.table);
      if (tableCache && tableCache.has(pkValue)) {
        const row = tableCache.get(pkValue);
        this.logger.log("ROW_CACHE_HIT", {
          goalId,
          table: this.table,
          primaryKey: this.primaryKey,
          value: pkValue,
        });
        return [row];
      }
    }

    const query = this.buildQuery(queryObj, whereCols, joinStructure);
    const sqlString = query.toString();
    const cacheKey = sqlString;
    
    if (CACHE_ENABLED) {
      this.logger.log("CACHE_LOOKUP", {
        goalId,
        cacheKey,
        allCacheKeys: Array.from(this.dbObj.getStoredQueries().keys()).slice(0, 3)
      });
      
      const storedQuery = this.dbObj.findStoredQueryByKey(cacheKey);
      if (storedQuery) {
        this.logger.log("CACHE_HIT", {
          goalId,
          queryObj,
          cacheKey,
          rowCount: storedQuery.rows.length
        });
        
        return storedQuery.rows;
      }
    }
    
    this.dbObj.addQuery(`goalId:${goalId} - ${sqlString}`);
    this.logger.log("DB_QUERY", {
      table: this.table,
      sql: sqlString,
      goalId,
      joinStructure
    });
    const rows = await query;
    
    if (CACHE_ENABLED) {
      this.dbObj.storeQueryByKey(cacheKey, rows);
      this.logger.log("CACHE_WRITTEN", {
        table: this.table,
        cacheKey,
        goalId,
        rowCount: rows.length
      });

      // Cache rows by primary key
      const tableCache = this.dbObj.getRowCache().get(this.table) || new Map();
      for (const row of rows) {
        if (this.primaryKey in row) {
          tableCache.set(row[this.primaryKey], row);
        }
      }
      this.dbObj.getRowCache().set(this.table, tableCache);
    }
    
    if (rows.length) {
      this.logger.log("DB_ROWS", {
        table: this.table,
        sql: query.toString(),
        goalId,
        cacheKey,
        rows,
      });
    } else {
      this.logger.log("NO_DB_ROWS", {
        table: this.table,
        sql: query.toString(),
        goalId,
        cacheKey,
        rows,
      });
    }

    return rows;
  }

  private buildQuery(queryObj: Record<string, Term>, whereCols: Record<string, Term>, joinStructure: any) {
    const selectCols = this.options?.selectColumns || ['*'];

    let query = this.dbObj.db(this.table)
      .distinct()
      .select(selectCols)
      .where(whereCols);
  
    if (joinStructure && joinStructure.joins.length > 0) {
      query = this.applyJoins(query, this.table, joinStructure);
    }
  
    return query;
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

    return {
      baseTable: this.table,
      joins
    };
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