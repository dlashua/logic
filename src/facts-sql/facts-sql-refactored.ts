import type { Knex } from "knex";
import knex from "knex";
import { ConfigurationManager } from "../shared/config.ts";
import { Logger } from "../shared/logger.ts";
import { QueryMerger } from "./query-merger.ts";
import { RelationFactoryWithMerger } from "./relation-factory-with-merger.ts";
import type { Configuration } from "./types.ts";

export const makeRelDB = async (
  knex_connect_options: Knex.Config,
  opts?: Record<string, string>,
  configOverrides?: Partial<Configuration>,
  mergeDelayMs = 100
) => {
  opts ??= {};
  const db = knex(knex_connect_options);
  
  // Create configuration
  const config = ConfigurationManager.create(configOverrides);
  
  // Create core dependencies
  const logger = new Logger(config.logging);
  
  // Create the query merger
  const queryMerger = new QueryMerger(
    logger,
    db,
    mergeDelayMs
  );

  // Create relation factory with the merger
  const mergerRelationFactory = new RelationFactoryWithMerger(logger, queryMerger);

  return {
    rel: mergerRelationFactory.createRelation.bind(mergerRelationFactory),
    relSym: mergerRelationFactory.createSymmetricRelation.bind(mergerRelationFactory),
    db,
    getQueries: () => queryMerger.getQueries(),
    clearQueries: () => queryMerger.clearQueries(),
    getQueryCount: () => queryMerger.getQueryCount(),
  };
};