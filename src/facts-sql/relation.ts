import type { Term, Subst, Goal } from "../core/types.ts";
import { unify, isVar, walk } from "../core/kernel.ts";
import { SimpleLogger, getDefaultLogger } from "../shared/simple-logger.ts";
import { RelationOptions } from "./types.ts";
import type { DBManager } from "./index.ts";

const CACHE_ENABLED = true;
const JOINS_ENABLED = false;

export class RegularRelationWithMerger {
  private logger: SimpleLogger;

  constructor(
    private dbObj: DBManager,
    private table: string,
    logger?: SimpleLogger,
    options?: RelationOptions,
  ) {
    this.logger = logger ?? getDefaultLogger();
  }

  createGoal(queryObj: Record<string, Term>): Goal {
    const queryKeys = Object.keys(queryObj);
    const goalId = this.dbObj.getNextGoalId();
    this.dbObj.addGoal(goalId, this.table, queryObj);

    return async function* factsSql(this: RegularRelationWithMerger, s: Subst) {
      const rows = await this.cacheOrQuery(goalId, queryObj, s);
      
      let yielded = 0;
      for (const row of rows) {
        // Convert aliased row back to original queryObj keys
        const convertedRow: Record<string, any> = {};
        for (const col of queryKeys) {
          const aliasedColName = `${col}_0`;
          if (aliasedColName in row) {
            convertedRow[col] = row[aliasedColName];
          }
        }

        const unifiedSubst = await this.unifyRowWithQuery(convertedRow, queryObj, s);
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
    // Walk all terms with current Subst to create cache key
    const walkedTerms: Record<string, any> = {};
    const groundedTerms: Record<string, any> = {};
    for (const [key, term] of Object.entries(queryObj)) {
      const walkedTerm = await walk(term, s);
      walkedTerms[key] = walkedTerm;
      
      // Only include grounded (non-variable) terms in cache key
      if (!isVar(walkedTerm)) {
        groundedTerms[key] = walkedTerm;
      }
    }
    const cacheKey = `${this.table}_${JSON.stringify(groundedTerms)}`;
    
    // Check if this specific goal+subst combination has been cached
    if (CACHE_ENABLED) {
      this.logger.log("CACHE_LOOKUP", {
        goalId,
        walkedTerms,
        cacheKey,
        allCacheKeys: Array.from(this.dbObj.getStoredQueries().keys())
      });
      
      const storedQuery = this.dbObj.findStoredQueryByKey(cacheKey);
      if (storedQuery) {
        this.logger.log("CACHE_HIT", {
          goalId,
          queryObj,
          walkedTerms,
          cacheKey,
          rowCount: storedQuery.rows.length
        });
        
        return storedQuery.rows;
      }
    }

    // Execute new query
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

    const query = this.buildQuery(queryObj, whereCols, joinStructure);
    
    this.dbObj.addQuery(`goalId:${goalId} - ${query.toString()}`);
    this.logger.log("DB_QUERY", {
      table: this.table,
      sql: query.toString(),
      goalId,
      joinStructure
    });
    const rows = await query;
    
    // Store query results with walked terms cache key
    if (CACHE_ENABLED) {
      this.dbObj.storeQueryWithKey(cacheKey, [goalId], rows);
      this.logger.log("CACHE_WRITTEN", {
        table: this.table,
        cacheKey,
        goalId,
        walkedTerms,
        rowCount: rows.length
      });
    }
    
    if(rows.length) {
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
    const selectColKeys = Object.keys(queryObj)
    
    // Build qualified select columns with table prefixes and aliases
    const baseAlias = `${this.table}_0`;
    const qualifiedSelectCols = selectColKeys.map(col => `${baseAlias}.${col} as ${col}_0`);
    // Build qualified where clauses with column aliases
    const qualifiedWhereCols: Record<string, any> = {};
    for (const [col, value] of Object.entries(whereCols)) {
      qualifiedWhereCols[`${col}_0`] = value;
    }
    
    let query = this.dbObj.db(`${this.table} as ${baseAlias}`)
      .distinct()
      .select(qualifiedSelectCols)
      .where(qualifiedWhereCols);
    
    // Apply joins if joinStructure exists
    if (joinStructure && joinStructure.joins.length > 0) {
      query = this.applyJoins(query, baseAlias, joinStructure);
    }
    
    return query;
  }

  private applyJoins(query: any, baseAlias: string, joinStructure: any) {
    let aliasCounter = 1;
    
    for (const join of joinStructure.joins) {
      const alias = `${join.table}_${aliasCounter}`;
      aliasCounter++;
      
      // Create a single join with the first condition
      if (join.conditions.length > 0) {
        const firstCondition = join.conditions[0];
        query = query.join(
          `${join.table} as ${alias}`,
          `${baseAlias}.${firstCondition.leftCol}`,
          `${alias}.${firstCondition.rightCol}`
        );
        
        // Add additional conditions as AND clauses
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
      
      // Find matching key-term pairs between current goal and shared goal
      for (const [currentKey, currentTerm] of Object.entries(currentQueryObj)) {
        for (const [sharedKey, sharedTerm] of Object.entries(sharedGoal.queryObj)) {
          // Only create join condition if both key and term match
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

    // Optimized unification - directly match variable IDs with row values
    let resultSubst = s;
    
    for (const [col, term] of Object.entries(queryObj)) {
      const rowValue = row[col];
      if (rowValue === undefined) {
        return null; // Variable not found in result set
      }

      const unified = await unify(term, rowValue, resultSubst);
      if (!unified) {
        return null; // Unification failed
      }
      resultSubst = unified;
    }
    
    return resultSubst;
  }

}