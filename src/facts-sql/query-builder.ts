import type { Knex } from "knex";
import { WhereClause } from './types.ts';

export class QueryBuilder {
  constructor(private db: Knex) {}

  /**
   * Build a regular select query
   */
  buildSelectQuery(
    table: string, 
    selectCols: string[], 
    whereClauses: WhereClause[]
  ): Knex.QueryBuilder {
    let query = this.db(table).select(selectCols);
    
    for (const clause of whereClauses) {
      query = query.where(clause.column, clause.value as any);
    }
    
    return query;
  }

  /**
   * Build a confirmation query (returns 1 if rows exist)
   */
  buildConfirmationQuery(
    table: string, 
    whereClauses: WhereClause[]
  ): Knex.QueryBuilder {
    let query = this.db(table).select(this.db.raw('1'));
    
    for (const clause of whereClauses) {
      query = query.where(clause.column, clause.value as any);
    }
    
    return query;
  }

  /**
   * Build a symmetric relation query
   */
  buildSymmetricQuery(
    table: string,
    keys: [string, string],
    groundedValues: (string | number)[]
  ): Knex.QueryBuilder {
    if (groundedValues.length === 2) {
      return this.db(table)
        .select(keys)
        .where(keys[0], groundedValues[0])
        .andWhere(keys[1], groundedValues[1]);
    } else {
      return this.db(table)
        .select(keys)
        .where(keys[0], groundedValues[0])
        .orWhere(keys[1], groundedValues[0]);
    }
  }

  /**
   * Execute query and return results with metadata
   */
  async executeQuery(query: Knex.QueryBuilder): Promise<{ rows: any[], sql: string }> {
    const sql = query.toString();
    const rows = await query;
    return {
      rows,
      sql 
    };
  }
}