import type { Knex } from "knex";
import type { Goal, Term } from "../core/types.ts";
import { Logger } from "../shared/logger.ts";
import { QueryCache } from "./cache.ts";
import { QueryBuilder } from "./query-builder.ts";
import { RegularRelation } from "./regular-relation.ts";
import { SymmetricRelation } from "./symmetric-relation.ts";
import { RelationOptions } from "./types.ts";

export interface RelationFactoryDependencies {
  db: Knex;
  logger: Logger;
  cache: QueryCache;
  queryBuilder: QueryBuilder;
  queries: string[];
  realQueries: string[];
}

export class RelationFactory {
  constructor(private deps: RelationFactoryDependencies) {}

  createRelation(table: string, options?: RelationOptions) {
    const relation = new RegularRelation(
      table,
      this.deps.logger,
      this.deps.cache,
      this.deps.queryBuilder,
      this.deps.queries,
      this.deps.realQueries,
      options,
    );

    return (queryObj: Record<string, Term>): Goal => {
      return relation.createGoal(queryObj);
    };
  }

  createSymmetricRelation(table: string, keys: [string, string], options?: RelationOptions) {
    const relation = new SymmetricRelation(
      table,
      keys,
      this.deps.logger,
      this.deps.cache,
      this.deps.queryBuilder,
      this.deps.queries,
      this.deps.realQueries,
      options,
    );

    return (queryObj: Record<string, Term<string | number>>): Goal => {
      return relation.createGoal(queryObj);
    };
  }
}