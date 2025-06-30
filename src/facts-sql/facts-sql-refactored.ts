import type { Knex } from "knex";
import knex from "knex";
import { ConfigurationManager } from "../shared/config.ts";
import { Logger } from "../shared/logger.ts";
import { QueryCache } from "./cache.ts";
import { QueryBuilder } from "./query-builder.ts";
import { RelationFactory } from "./relation-factory.ts";
import { Configuration } from "./types.ts";

export const makeRelDB = async (
  knex_connect_options: Knex.Config,
  opts?: Record<string, string>,
  configOverrides?: Partial<Configuration>
) => {
  opts ??= {};
  const db = knex(knex_connect_options);
  
  // Create configuration
  const config = ConfigurationManager.create(configOverrides);
  
  // Create core dependencies
  const logger = new Logger(config.logging);
  const cache = new QueryCache(config.cache, logger);
  const queryBuilder = new QueryBuilder(db);
  
  // Query logging arrays
  const queries: string[] = [];
  const realQueries: string[] = [];
  
  // Create relation factory
  const relationFactory = new RelationFactory({
    db,
    logger,
    cache,
    queryBuilder,
    queries,
    realQueries,
  });

  return {
    rel: relationFactory.createRelation.bind(relationFactory),
    relSym: relationFactory.createSymmetricRelation.bind(relationFactory),
    db,
    queries,
    realQueries,
  };
};