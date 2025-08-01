import type { Goal, Term } from "@swiftfall/logic";
import { getDefaultLogger, type Logger, or } from "@swiftfall/logic";
import { AbstractRelation } from "./abstract-relation.js";
import { DefaultGoalManager } from "./goal-manager.js";
import type {
  AbstractRelationConfig,
  DataStore,
  GoalManager,
  RelationOptions,
} from "./types.js";

export type { RestDataStoreConfig } from "./types.js";
/**
 * Factory for creating abstract relation systems
 * This is the main entry point for the abstract data layer
 */
// Make AbstractRelationFactory generic over options type
export class AbstractRelationFactory<
  TOptions extends RelationOptions = RelationOptions,
> {
  private goalManager: GoalManager;
  private logger: Logger;
  private config: AbstractRelationConfig;

  constructor(
    private dataStore: DataStore,
    logger?: Logger,
    config?: AbstractRelationConfig,
  ) {
    this.logger = logger ?? getDefaultLogger();
    this.goalManager = new DefaultGoalManager();
    this.config = config ?? {};
  }

  /**
   * Get the appropriate relation identifier based on datastore type and options
   */
  private getRelationIdentifier(table: string, options?: TOptions): string {
    // For REST APIs, prefer pathTemplate as the identifier since it's more meaningful
    if (this.dataStore.type === "rest") {
      const restOptions = options as any;
      if (restOptions?.pathTemplate) {
        return restOptions.pathTemplate;
      }
    }
    // For SQL or when no pathTemplate, use table name
    return table;
  }

  /**
   * Create a regular relation for a table
   */
  createRelation(table: string, options?: TOptions) {
    // For REST APIs, use pathTemplate as identifier if available, otherwise use table name
    // For SQL APIs, always use table name
    const relationIdentifier = this.getRelationIdentifier(table, options);

    const relation = new AbstractRelation<TOptions>(
      this.dataStore,
      this.goalManager,
      relationIdentifier,
      this.logger,
      options,
      this.config,
    );

    return (queryObj: Record<string, Term>): Goal => {
      return relation.createGoal(queryObj);
    };
  }

  /**
   * Create a symmetric relation for bidirectional queries
   */
  createSymmetricRelation(
    table: string,
    keys: [string, string],
    options?: RelationOptions,
  ) {
    const relationIdentifier = this.getRelationIdentifier(
      table,
      options as TOptions,
    );

    const relation = new AbstractRelation(
      this.dataStore,
      this.goalManager,
      relationIdentifier,
      this.logger,
      options,
      this.config,
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
export function createAbstractRelationSystem<
  TOptions extends RelationOptions = RelationOptions,
>(dataStore: DataStore, logger?: Logger, config?: AbstractRelationConfig) {
  const factory = new AbstractRelationFactory<TOptions>(
    dataStore,
    logger,
    config,
  );

  return {
    rel: factory.createRelation.bind(factory),
    relSym: factory.createSymmetricRelation?.bind(factory),
    getQueries: factory.getQueries.bind(factory),
    clearQueries: factory.clearQueries.bind(factory),
    getQueryCount: factory.getQueryCount.bind(factory),
    getDataStore: factory.getDataStore.bind(factory),
    close: factory.close.bind(factory),
  };
}

export { AbstractRelation } from "./abstract-relation.js";
export { DefaultCacheManager } from "./cache-manager.js";
export { DefaultGoalManager } from "./goal-manager.js";
// Re-export types and implementations
export type {
  AbstractRelationConfig,
  DataRow,
  DataStore,
  GoalManager,
  QueryParams,
  RelationOptions,
  WhereCondition,
} from "./types.js";
