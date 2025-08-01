import type {
  DataRow,
  DataStore,
  QueryParams,
  WhereCondition,
} from "@swiftfall/facts-abstract";
import type { Knex } from "knex";

/**
 * SQL implementation of DataStore using Knex
 * This wraps the existing SQL logic into the abstract interface
 */
export class SqlDataStore implements DataStore {
  readonly type = "sql";

  constructor(private db: Knex) {}

  async executeQuery(params: QueryParams): Promise<DataRow[]> {
    let query = this.db(params.relationIdentifier);

    // Apply WHERE conditions
    for (const condition of params.whereConditions) {
      if (condition.operator === "eq") {
        query = query.where(condition.column, condition.value);
      } else if (condition.operator === "in" && condition.values) {
        query = query.whereIn(condition.column, condition.values);
      } else if (condition.operator === "gt") {
        query = query.where(condition.column, ">", condition.value);
      } else if (condition.operator === "lt") {
        query = query.where(condition.column, "<", condition.value);
      } else if (condition.operator === "gte") {
        query = query.where(condition.column, ">=", condition.value);
      } else if (condition.operator === "lte") {
        query = query.where(condition.column, "<=", condition.value);
      } else if (condition.operator === "like") {
        query = query.where(condition.column, "like", condition.value);
      }
    }

    // Select columns
    query = query.select(params.selectColumns);

    // Apply pagination if specified
    if (params.limit) {
      query = query.limit(params.limit);
    }
    if (params.offset) {
      query = query.offset(params.offset);
    }

    // Log the actual SQL query
    if (params.logQuery) {
      params.logQuery(`${query.toString()}`);
    }

    return await query;
  }

  async getColumns(relationIdentifier: string): Promise<string[]> {
    const result = await this.db(relationIdentifier).columnInfo();
    return Object.keys(result);
  }

  buildWhereConditions(clauses: Record<string, Set<any>>): WhereCondition[] {
    const conditions: WhereCondition[] = [];

    for (const [column, values] of Object.entries(clauses)) {
      if (values.size === 1) {
        conditions.push({
          column,
          operator: "eq",
          value: Array.from(values)[0],
        });
      } else if (values.size > 1) {
        conditions.push({
          column,
          operator: "in",
          value: null,
          values: Array.from(values),
        });
      }
    }

    return conditions;
  }

  async close(): Promise<void> {
    await this.db.destroy();
  }
}
