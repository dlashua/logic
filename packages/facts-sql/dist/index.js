// src/index.ts
import { createAbstractRelationSystem } from "@codespiral/facts-abstract";
import knex from "knex";
import { getDefaultLogger } from "@codespiral/logic";

// src/sql-datastore.ts
var SqlDataStore = class {
  constructor(db) {
    this.db = db;
  }
  type = "sql";
  async executeQuery(params) {
    let query = this.db(params.relationIdentifier);
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
    query = query.select(params.selectColumns);
    if (params.limit) {
      query = query.limit(params.limit);
    }
    if (params.offset) {
      query = query.offset(params.offset);
    }
    if (params.logQuery) {
      params.logQuery(`${query.toString()}`);
    }
    return await query;
  }
  async getColumns(relationIdentifier) {
    const result = await this.db(relationIdentifier).columnInfo();
    return Object.keys(result);
  }
  buildWhereConditions(clauses) {
    const conditions = [];
    for (const [column, values] of Object.entries(clauses)) {
      if (values.size === 1) {
        conditions.push({
          column,
          operator: "eq",
          value: Array.from(values)[0]
        });
      } else if (values.size > 1) {
        conditions.push({
          column,
          operator: "in",
          value: null,
          values: Array.from(values)
        });
      }
    }
    return conditions;
  }
  async close() {
    await this.db.destroy();
  }
};

// src/index.ts
var makeRelDB = async (knex_connect_options, options, configOverrides) => {
  options ??= {};
  const logger = getDefaultLogger();
  const db = knex(knex_connect_options);
  const dataStore = new SqlDataStore(db);
  const config = {
    batchSize: 100,
    debounceMs: 50,
    enableCaching: true,
    enableQueryMerging: true,
    ...configOverrides
  };
  const relationSystem = createAbstractRelationSystem(
    dataStore,
    logger,
    config
  );
  return {
    rel: relationSystem.rel,
    relSym: relationSystem.relSym,
    db,
    getQueries: relationSystem.getQueries,
    clearQueries: relationSystem.clearQueries,
    getQueryCount: relationSystem.getQueryCount,
    close: relationSystem.close
  };
};
export {
  makeRelDB
};
//# sourceMappingURL=index.js.map