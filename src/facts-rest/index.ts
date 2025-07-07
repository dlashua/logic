import { getDefaultLogger } from "../shared/logger.ts";
import { createAbstractRelationSystem } from "../facts-abstract/index.ts";
import type { AbstractRelationConfig, RestDataStoreConfig } from "../facts-abstract/types.ts";
import { RestDataStore } from "./rest-datastore.ts";
import type { RestRelationOptions } from "./types.ts";
import type { RelationCache } from "./relation-cache.ts";

/**
 * REST API implementation using the abstract data layer
 * Example of how to create a facts system backed by a REST API
 */
export const makeRelREST = async (
  restConfig: RestDataStoreConfig & { cache?: RelationCache, cacheMethods?: string[] },
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

  // Patch rel to accept cache option per relation
  const origRel = relationSystem.rel;
  function rel(pathTemplate: string, options: RestRelationOptions & { cache?: RelationCache | null } = {}) {
    // If cache is explicitly set, create a new RestDataStore for this rel with the given cache
    if (Object.prototype.hasOwnProperty.call(options, 'cache')) {
      const relCache = options.cache;
      const relDataStore = new RestDataStore({
        ...restConfig,
        cache: relCache 
      });
      const relSystem = createAbstractRelationSystem<RestRelationOptions>(relDataStore, logger, systemConfig);
      return relSystem.rel(pathTemplate, options);
    }
    // Otherwise, use the default system
    return origRel(pathTemplate, options);
  }

  return {
    rel,
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