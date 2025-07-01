import type { Knex } from "knex";
import { Term, Subst } from "../core/types.ts";
import { Logger } from "../shared/logger.ts";
import { or } from "../core/combinators.ts";
import { QueryCache } from "./cache.ts";
import { QueryBuilder } from "./query-builder.ts";
import { QueryMerger } from "./query-merger.ts";
import { RegularRelationWithMerger } from "./regular-relation-with-merger.ts";
import { SymmetricRelation } from "./symmetric-relation.ts";
import { SymmetricRelationWithMerger } from "./symmetric-relation-with-merger.ts";
import { GoalFunction, RelationOptions } from "./types.ts";

export interface RelationFactoryWithMergerDependencies {
  db: Knex;
  logger: Logger;
  queryBuilder: QueryBuilder;
  queries: string[];
  realQueries: string[];
}

export class RelationFactoryWithMerger {
  private queryMerger: QueryMerger;
  private queryCache: QueryCache; // For symmetric relations
  private static globalGoalId = 1; // Global goal ID counter across all relations
  
  constructor(private deps: RelationFactoryWithMergerDependencies, mergeDelayMs = 100) {
    this.queryMerger = new QueryMerger(
      deps.logger,
      deps.queryBuilder,
      deps.db,
      deps.queries,
      deps.realQueries,
      mergeDelayMs
    );
    
    // Create cache for symmetric relations (with default configuration)
    this.queryCache = new QueryCache({
      enabled: false, // Disable caching for now
      maxSize: 1000,
      ttl: 60000
    }, deps.logger);
  }
  
  static getNextGoalId(): number {
    return this.globalGoalId++;
  }

  createRelation(table: string, options?: RelationOptions) {
    const relation = new RegularRelationWithMerger(
      table,
      this.deps.logger,
      this.queryMerger,
      () => RelationFactoryWithMerger.getNextGoalId(),
      options,
    );

    return (queryObj: Record<string, Term>): GoalFunction => {
      return relation.createGoal(queryObj);
    };
  }

  logic_based_createSymmetricRelation(table: string, keys: [string, string], options?: RelationOptions) {
    const relation = new RegularRelationWithMerger(
      table,
      this.deps.logger,
      this.queryMerger,
      () => RelationFactoryWithMerger.getNextGoalId(),
      options,
    );

    return (queryObj: Record<string, Term>): GoalFunction => {
      const queryObjSwapped = {
        [keys[0]]: queryObj[keys[1]],
        [keys[1]]: queryObj[keys[0]],
      };
      return or(
        relation.createGoal(queryObj),
        relation.createGoal(queryObjSwapped),
      )
    };
  }

  sql_based_createSymmetricRelation(table: string, keys: [string, string], options?: RelationOptions) {
    // Use the new symmetric relation implementation with merger
    const symmetricRelation = new SymmetricRelationWithMerger(
      table,
      keys,
      this.deps.logger,
      this.queryMerger,
      () => RelationFactoryWithMerger.getNextGoalId(),
      options
    );

    return (queryObj: Record<string, Term>): GoalFunction => {
      return symmetricRelation.createGoal(queryObj);
    };
  }

  createSymmetricRelation = this.sql_based_createSymmetricRelation;
}