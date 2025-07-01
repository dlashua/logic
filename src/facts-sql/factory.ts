import type { Term, Goal } from "../core/types.ts";
import { Logger } from "../shared/logger.ts";
import { or } from "../core/combinators.ts";
import { QueryMerger } from "./query-merger.ts";
import { RegularRelationWithMerger } from "./relation.ts";
import { SymmetricRelationWithMerger } from "./symmetric-relation.ts";
import type { RelationOptions } from "./types.ts";

export class RelationFactoryWithMerger {
  constructor(
    private logger: Logger,
    private queryMerger: QueryMerger,
  ) {}


  createRelation(table: string, options?: RelationOptions) {
    const relation = new RegularRelationWithMerger(
      table,
      this.logger,
      this.queryMerger,
      options,
    );

    return (queryObj: Record<string, Term>): Goal => {
      return relation.createGoal(queryObj);
    };
  }

  logic_createSymmetricRelation(table: string, keys: [string, string], options?: RelationOptions) {
    const relation = new RegularRelationWithMerger(
      table,
      this.logger,
      this.queryMerger,
      options,
    );

    return (queryObj: Record<string, Term>): Goal => {
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

  sql_createSymmetricRelation(table: string, keys: [string, string], options?: RelationOptions) {
    // Use the new symmetric relation implementation with merger
    const symmetricRelation = new SymmetricRelationWithMerger(
      table,
      keys,
      this.logger,
      this.queryMerger,
      options
    );

    return (queryObj: Record<string, Term>): Goal => {
      return symmetricRelation.createGoal(queryObj);
    };
  }

  createSymmetricRelation = this.sql_createSymmetricRelation;
}