import type { Knex } from "knex";
import knex from "knex";
import { ConfigurationManager } from "../shared/config.ts";
import { Logger } from "../shared/logger.ts";
import { QueryBuilder } from "./query-builder.ts";
import { RelationFactoryWithMerger } from "./relation-factory-with-merger.ts";
import { Configuration } from "./types.ts";

export const makeRelDBWithMerger = async (
  knex_connect_options: Knex.Config,
  opts?: Record<string, string>,
  configOverrides?: Partial<Configuration>,
  mergeDelayMs = 10
) => {
  opts ??= {};
  const db = knex(knex_connect_options);
  
  // Create configuration - enable all logging for debugging
  const config = ConfigurationManager.create({
    logging: {
      enabled: false,
      level: 'debug',
      sqlQueries: true,
      performance: true,
      cache: true
    },
    ...configOverrides
  });
  
  // Create core dependencies
  const logger = new Logger(config.logging);
  const queryBuilder = new QueryBuilder(db);
  
  // Query logging arrays
  const queries: string[] = [];
  const realQueries: string[] = [];
  
  // Create relation factory with merger
  const relationFactory = new RelationFactoryWithMerger({
    db,
    logger,
    queryBuilder,
    queries,
    realQueries,
  }, mergeDelayMs);

  return {
    rel: relationFactory.createRelation.bind(relationFactory),
    relSym: relationFactory.createSymmetricRelation.bind(relationFactory),
    db,
    queries,
    realQueries,
    // Safe query management methods
    getQueries: () => [...realQueries], // Return copy to prevent mutation
    clearQueries: () => {
      queries.length = 0;
      realQueries.length = 0;
    },
    getQueryCount: () => realQueries.length,
  };
};