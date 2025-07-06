import type { Goal, Term } from "../core/types.ts";
import { or } from "../core/combinators.ts";
import { Logger, getDefaultLogger } from "../shared/logger.ts";
import type { DataStore, GoalManager, RelationOptions, AbstractRelationConfig } from "./types.ts"
import { DefaultGoalManager } from "./goal-manager.ts";
import { AbstractRelation } from "./abstract-relation.ts";

/**
 * Factory for creating abstract relation systems
 * This is the main entry point for the abstract data layer
 */
export class AbstractRelationFactory {
  private goalManager: GoalManager;
  private logger: Logger;
  private config: AbstractRelationConfig;

  constructor(
    private dataStore: DataStore,
    logger?: Logger,
    config?: AbstractRelationConfig
  ) {
    this.logger = logger ?? getDefaultLogger();
    this.goalManager = new DefaultGoalManager();
    this.config = config ?? {};
  }

  /**
   * Create a regular relation for a table
   */
  createRelation(table: string, options?: RelationOptions) {
    const relation = new AbstractRelation(
      this.dataStore,
      this.goalManager,
      table,
      this.logger,
      options,
      this.config
    );

    return (queryObj: Record<string, Term>): Goal => {
      return relation.createGoal(queryObj);
    };
  }

  /**
   * Create a symmetric relation for bidirectional queries
   */
  createSymmetricRelation(table: string, keys: [string, string], options?: RelationOptions) {
    const relation = new AbstractRelation(
      this.dataStore,
      this.goalManager,
      table,
      this.logger,
      options,
      this.config
    );

    return (queryObj: Record<string, Term>): Goal => {
      // Create both directions of the query
      const queryObjSwapped = {
        [keys[0]]: queryObj[keys[1]],
        [keys[1]]: queryObj[keys[0]],
      };

      return or(
        relation.createGoal(queryObj),
        relation.createGoal(queryObjSwapped),
      );
    };
  }

  /**
   * Get debugging information
   */
  getQueries(): string[] {
    return this.goalManager.getQueries();
  }

  clearQueries(): void {
    this.goalManager.clearQueries();
  }

  getQueryCount(): number {
    return this.goalManager.getQueryCount();
  }

  /**
   * Access the underlying data store
   */
  getDataStore(): DataStore {
    return this.dataStore;
  }

  /**
   * Close the data store connection
   */
  async close(): Promise<void> {
    if (this.dataStore.close) {
      await this.dataStore.close();
    }
  }
}

/**
 * Main factory function - creates an abstract relation system
 */
export async function createAbstractRelationSystem(
  dataStore: DataStore,
  logger?: Logger,
  config?: AbstractRelationConfig
) {
  const factory = new AbstractRelationFactory(dataStore, logger, config);
  
  return {
    rel: factory.createRelation.bind(factory),
    relSym: factory.createSymmetricRelation.bind(factory),
    getQueries: factory.getQueries.bind(factory),
    clearQueries: factory.clearQueries.bind(factory),
    getQueryCount: factory.getQueryCount.bind(factory),
    getDataStore: factory.getDataStore.bind(factory),
    close: factory.close.bind(factory),
  };
}

// Re-export types and implementations
export type { 
  DataStore, 
  GoalManager, 
  RelationOptions, 
  AbstractRelationConfig,
  QueryParams,
  WhereCondition,
  DataRow
} from "./types.ts";

export { DefaultGoalManager } from "./goal-manager.ts";
export { DefaultCacheManager } from "./cache-manager.ts";
export { AbstractRelation } from "./abstract-relation.ts";