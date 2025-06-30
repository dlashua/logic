import type { Knex } from "knex";
import { Term, Subst } from "../core.ts";
import { Logger } from "../shared/logger.ts";
import { QueryCache } from "./cache.ts";
import { QueryBuilder } from "./query-builder.ts";
import { RegularRelation } from "./regular-relation.ts";
import { SymmetricRelation } from "./symmetric-relation.ts";
import { GoalFunction, RelationOptions } from "./types.ts";

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

    return (queryObj: Record<string, Term>): GoalFunction => {
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

    return (queryObj: Record<string, Term<string | number>>): GoalFunction => {
      return relation.createGoal(queryObj);
    };
  }
}