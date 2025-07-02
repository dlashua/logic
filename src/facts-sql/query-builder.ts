import type { Knex } from "knex";
import { isVar } from "../core/kernel.ts";
import type { QueryPattern, MergedQuery } from "./query-merger.ts";

export class QueryBuilder {
  constructor(private db: Knex) {}

  /**
   * Creates a semantic cache key based on query structure rather than SQL text.
   * This is more reliable than string-based normalization.
   */
  public createCacheKey(mergedQuery: MergedQuery, joinVars?: any[]): string {
    const symmetricPattern = mergedQuery.patterns.find(p => p.isSymmetric);
    
    const key = {
      tables: mergedQuery.patterns.map(p => p.table).sort(),
      whereCols: mergedQuery.combinedWhereCols,
      selectCols: mergedQuery.patterns.flatMap(p => Object.keys(p.selectCols)).sort(),
      joinVars: joinVars?.map(jv => jv.varId).sort() || [],
      isSymmetric: !!symmetricPattern,
      symmetricKeys: symmetricPattern?.symmetricKeys || null,
      queryType: joinVars && joinVars.length > 0 ? 'join' : 
        symmetricPattern ? 'symmetric' : 'single'
    };
    
    return JSON.stringify(key);
  }

  public build(mergedQuery: MergedQuery, joinVars?: any[]): Knex.QueryBuilder {
    const symmetricPattern = mergedQuery.patterns.find(p => p.isSymmetric);
    if (symmetricPattern) {
      return this.buildSymmetricQuery(mergedQuery, symmetricPattern);
    }

    if (joinVars && joinVars.length > 0) {
      return this.buildJoinQuery(mergedQuery, joinVars);
    }

    return this.buildSingleTableQuery(mergedQuery);
  }

  private buildSingleTableQuery(mergedQuery: MergedQuery): Knex.QueryBuilder {
    const { table, combinedWhereCols } = mergedQuery;
    const selectCols = this.buildSelectWithVariableAliases(mergedQuery.patterns);
    return this.db.from(table).select(selectCols).where(combinedWhereCols);
  }

  private buildSymmetricQuery(mergedQuery: MergedQuery, symmetricPattern: QueryPattern): Knex.QueryBuilder {
    const { table } = mergedQuery;
    const keys = symmetricPattern.symmetricKeys!;
    const groundedValues = Object.values(mergedQuery.combinedWhereCols).filter(value => !isVar(value)) as (string | number)[];
    const selectCols = [];

    selectCols.push(this.db.raw(`${keys[0]} AS ${keys[0]}_sym`));
    selectCols.push(this.db.raw(`${keys[1]} AS ${keys[1]}_sym`));

    for (const [column, term] of Object.entries(symmetricPattern.selectCols)) {
      if (isVar(term)) {
        selectCols.push(this.db.raw(`${column} AS ${term.id}`));
      }
    }

    for (const [column, value] of Object.entries(symmetricPattern.whereCols)) {
      const originalTerm = symmetricPattern.queryObj[column];
      if (isVar(originalTerm)) {
        selectCols.push(this.db.raw(`? AS ??`, [value, originalTerm.id]));
      }
    }

    const queryBuilder = this.db.from(table).select(selectCols);

    if (groundedValues.length === 2) {
      queryBuilder.where({
        [keys[0]]: groundedValues[0],
        [keys[1]]: groundedValues[1] 
      });
    } else if (groundedValues.length === 1) {
      queryBuilder.where(keys[0], groundedValues[0]).orWhere(keys[1], groundedValues[0]);
    }

    return queryBuilder;
  }

  private buildJoinQuery(mergedQuery: MergedQuery, joinVars: any[]): Knex.QueryBuilder {
    const tableAliases = this.createTableAliases(mergedQuery.patterns);
    const selectCols = this.buildSelectWithAliases(mergedQuery.patterns, tableAliases);
    const queryBuilder = this.db.queryBuilder();
    const firstPattern = mergedQuery.patterns[0];
    const firstAlias = tableAliases.get(firstPattern.goalId)!;
    queryBuilder.from(`${firstPattern.table} as ${firstAlias}`);
    queryBuilder.select(selectCols);
    this.buildAndApplyJoinClauses(queryBuilder, mergedQuery.patterns, tableAliases, joinVars);
    this.buildAndApplyWhereClauses(queryBuilder, mergedQuery.patterns, tableAliases);
    return queryBuilder;
  }

  private createTableAliases(patterns: QueryPattern[]): Map<number, string> {
    const aliases = new Map<number, string>();
    const tableCount = new Map<string, number>();
    for (const pattern of patterns) {
      const currentCount = tableCount.get(pattern.table) || 0;
      tableCount.set(pattern.table, currentCount + 1);
      if (currentCount === 0) {
        aliases.set(pattern.goalId, pattern.table);
      } else {
        aliases.set(pattern.goalId, `${pattern.table}_${currentCount}`);
      }
    }
    return aliases;
  }

  private buildSelectWithAliases(patterns: QueryPattern[], tableAliases: Map<number, string>): (Knex.Raw)[] {
    const selectCols: (Knex.Raw)[] = [];
    for (const pattern of patterns) {
      const alias = tableAliases.get(pattern.goalId)!;
      for (const [column, term] of Object.entries(pattern.selectCols)) {
        if (isVar(term)) {
          selectCols.push(this.db.raw(`??.?? AS ??`, [alias, column, term.id]));
        }
      }
    }
    return selectCols;
  }

  private buildSelectWithVariableAliases(patterns: QueryPattern[]): (Knex.Raw)[] {
    const selectExpressions = new Map<string, Knex.Raw>(); // Use a map to prevent duplicate aliases

    for (const pattern of patterns) {
      // Part 1: Select variables from columns
      for (const [column, term] of Object.entries(pattern.selectCols)) {
        if (isVar(term) && !selectExpressions.has(term.id)) {
          selectExpressions.set(term.id, this.db.raw(`?? AS ??`, [column, term.id]));
        }
      }
      // Part 2: Select grounded values that were originally variables
      for (const [column, value] of Object.entries(pattern.whereCols)) {
        const originalTerm = pattern.queryObj[column];
        if (isVar(originalTerm) && !selectExpressions.has(originalTerm.id)) {
          selectExpressions.set(originalTerm.id, this.db.raw(`? AS ??`, [value, originalTerm.id]));
        }
      }
    }
    return Array.from(selectExpressions.values());
  }

  private buildAndApplyJoinClauses(queryBuilder: Knex.QueryBuilder, patterns: QueryPattern[], tableAliases: Map<number, string>, joinVars: any[]): void {
    for (let i = 1; i < patterns.length; i++) {
      const pattern = patterns[i];
      const alias = tableAliases.get(pattern.goalId)!;
      const joinConditions: { col1: string, col2: string }[] = [];
      for (const joinVar of joinVars) {
        // TODO fix these types
        const currentCols = joinVar.columns.filter((c: any) => c.goalId === pattern.goalId);
        const previousCols = joinVar.columns.filter((c: any) => {
          for (let j = 0; j < i; j++) {
            if (c.goalId === patterns[j].goalId) {
              return true;
            }
          }
          return false;
        });
        for (const currentCol of currentCols) {
          for (const prevCol of previousCols) {
            const prevAlias = tableAliases.get(prevCol.goalId)!;
            joinConditions.push({ 
              col1: `${prevAlias}.${prevCol.column}`, 
              col2: `${alias}.${currentCol.column}` 
            });
          }
        }
      }
      if (joinConditions.length > 0) {
        queryBuilder.innerJoin(`${pattern.table} as ${alias}`, function() {
          for (const cond of joinConditions) {
            this.on(cond.col1, '=', cond.col2);
          }
        });
      } else {
        // TODO yucky hack
        queryBuilder.crossJoin(`${pattern.table} as ${alias}` as unknown as Knex.Raw<any>);
      }
    }
  }

  private buildAndApplyWhereClauses(queryBuilder: Knex.QueryBuilder, patterns: QueryPattern[], tableAliases: Map<number, string>): void {
    for (const pattern of patterns) {
      const alias = tableAliases.get(pattern.goalId)!;
      for (const [column, value] of Object.entries(pattern.whereCols)) {
        if(isVar(value)) {
          continue;
        }
        // TODO yucky hack
        queryBuilder.where(`${alias}.${column}`, value as string);
      }
    }
  }
}
