import type { Knex } from "knex";
import { Term, Subst } from "../core.ts";
import { Logger } from "./logger.ts";
import { QueryCache } from "./cache.ts";
import { PatternManager, SymmetricPatternManager } from "./pattern-manager.ts";
import { QueryBuilder } from "./query-builder.ts";
import { RegularRelation } from "./regular-relation.ts";
import { SymmetricRelation } from "./symmetric-relation.ts";
import { GoalFunction } from "./types.ts";

export interface RelationFactoryDependencies {
  db: Knex;
  logger: Logger;
  cache: QueryCache;
  queryBuilder: QueryBuilder;
  queries: string[];
  realQueries: string[];
  cacheQueries: string[];
}

export class RelationFactory {
  constructor(private deps: RelationFactoryDependencies) {}

  createRelation(table: string) {
    const patternManager = new PatternManager(this.deps.logger);
    const relation = new RegularRelation(
      table,
      patternManager,
      this.deps.logger,
      this.deps.cache,
      this.deps.queryBuilder,
      this.deps.queries,
      this.deps.realQueries,
      this.deps.cacheQueries
    );

    return (queryObj: Record<string, Term>): GoalFunction => {
      return relation.createGoal(queryObj);
    };
  }

  createSymmetricRelation(table: string, keys: [string, string]) {
    const patternManager = new SymmetricPatternManager(this.deps.logger);
    const relation = new SymmetricRelation(
      table,
      keys,
      patternManager,
      this.deps.logger,
      this.deps.cache,
      this.deps.queryBuilder,
      this.deps.queries,
      this.deps.realQueries,
      this.deps.cacheQueries
    );

    return (queryObj: Record<string, Term<string | number>>): GoalFunction => {
      return relation.createGoal(queryObj);
    };
  }
}