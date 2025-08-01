import type {
  AbstractRelationConfig,
  RestDataStoreConfig,
} from "@swiftfall/facts-abstract";
import { createAbstractRelationSystem } from "@swiftfall/facts-abstract";
import { getDefaultLogger } from "@swiftfall/logic";
import type { RelationCache } from "./relation-cache.js";
import { RestDataStore } from "./rest-datastore.js";
import type { RestRelationOptions } from "./types.js";

/**
 * REST API implementation using the abstract data layer
 * Example of how to create a facts system backed by a REST API
 */
export const makeRelREST = async (
  restConfig: RestDataStoreConfig & {
    cache?: RelationCache;
    cacheMethods?: string[];
  },
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
    ...config,
  };

  // Create the abstract relation system
  const relationSystem = createAbstractRelationSystem<RestRelationOptions>(
    dataStore,
    logger,
    systemConfig,
  );

  // Patch rel to accept cache option per relation
  const origRel = relationSystem.rel;
  function rel(
    pathTemplate: string,
    options: RestRelationOptions & { cache?: RelationCache | null } = {},
  ) {
    // If cache is explicitly set, create a new RestDataStore for this rel with the given cache
    if (Object.hasOwn(options, "cache")) {
      const relCache = options.cache;
      const relDataStore = new RestDataStore({
        ...restConfig,
        cache: relCache,
      });
      const relSystem = createAbstractRelationSystem<RestRelationOptions>(
        relDataStore,
        logger,
        systemConfig,
      );
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
export type { RestDataStoreConfig } from "@swiftfall/facts-abstract";
