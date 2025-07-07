import { getDefaultLogger } from "../shared/logger.ts";
import { createAbstractRelationSystem } from "../facts-abstract/index.ts";
import type { AbstractRelationConfig, RestDataStoreConfig } from "../facts-abstract/types.ts";
import { RestDataStore } from "./rest-datastore.ts";
import type { RestRelationOptions } from "./types.ts";

/**
 * REST API implementation using the abstract data layer
 * Example of how to create a facts system backed by a REST API
 */
export const makeRelREST = async (
  restConfig: RestDataStoreConfig,
  config?: AbstractRelationConfig,
) => {
  const logger = getDefaultLogger();
  
  // Create REST data store
  const dataStore = new RestDataStore(restConfig);
  
  // Configure the abstract relation system
  const systemConfig: AbstractRelationConfig = {
    batchSize: 50, // Smaller batches for REST APIs
    debounceMs: 100, // Longer debounce for network calls
    enableCaching: true,
    enableQueryMerging: false, // REST APIs might not benefit from query merging
    ...config
  };
  
  // Create the abstract relation system
  const relationSystem = createAbstractRelationSystem<RestRelationOptions>(dataStore, logger, systemConfig);
  
  return {
    rel: relationSystem.rel,
    relSym: relationSystem.relSym,
    getQueries: relationSystem.getQueries,
    clearQueries: relationSystem.clearQueries,
    getQueryCount: relationSystem.getQueryCount,
    close: relationSystem.close,
    getDataStore: relationSystem.getDataStore,
  };
};

// Re-export for convenience
export type { RestDataStoreConfig } from "../facts-abstract/types.ts";