import { Subst, Term, Logger, Goal } from '@swiftfall/logic';

/**
 * Core goal record that tracks logic goals independently of data store implementation
 */
interface GoalRecord {
    goalId: number;
    relationIdentifier: string;
    queryObj: Record<string, Term>;
    batchKey?: string;
    relationOptions?: RelationOptions;
}
/**
 * WHERE clause condition for data stores
 */
interface WhereCondition {
    column: string;
    operator: "eq" | "in" | "gt" | "lt" | "gte" | "lte" | "like";
    value: any;
    values?: any[];
}
/**
 * Query parameters passed to data store implementations
 */
interface QueryParams {
    relationIdentifier: string;
    selectColumns: string[];
    whereConditions: WhereCondition[];
    limit?: number;
    offset?: number;
    relationOptions?: RelationOptions;
    goalId?: number;
    logQuery?: (queryString: string) => void;
}
/**
 * Result row from data store
 */
type DataRow = Record<string, any>;
/**
 * Options for relation creation
 */
interface RelationOptions {
    primaryKey?: string;
    selectColumns?: string[];
    fullScanKeys?: string[];
    restPrimaryKey?: string;
}
/**
 * Abstract data store interface that any backend must implement
 */
interface DataStore {
    /**
     * Execute a query and return rows
     */
    executeQuery(params: QueryParams): Promise<DataRow[]>;
    /**
     * Get available columns for a table (optional, for validation)
     */
    getColumns?(table: string): Promise<string[]> | string[];
    /**
     * Build where conditions in a data-store specific way
     * Default implementation provided, but can be overridden
     */
    buildWhereConditions?(clauses: Record<string, Set<any>>): WhereCondition[];
    /**
     * Close/cleanup the data store connection
     */
    close?(): Promise<void> | void;
    /**
     * Data store specific metadata
     */
    readonly type: string;
}
/**
 * Abstract goal manager that handles goal tracking, batching, and caching
 * This is data-store agnostic
 */
interface GoalManager {
    getNextGoalId(): number;
    addGoal(goalId: number, relationIdentifier: string, queryObj: Record<string, Term>, batchKey?: string, relationOptions?: RelationOptions): void;
    getGoalById(id: number): GoalRecord | undefined;
    getGoalsByBatchKey(batchKey: string): GoalRecord[];
    getGoals(): GoalRecord[];
    clearGoals(): void;
    addQuery(query: string): void;
    getQueries(): string[];
    clearQueries(): void;
    getQueryCount(): number;
}
/**
 * Cache manager interface
 */
interface CacheManager {
    get(goalId: number, subst: Subst): DataRow[] | null;
    set(goalId: number, subst: Subst, rows: DataRow[], meta?: Record<string, any>): void;
    clear(goalId?: number): void;
    has(goalId: number, subst: Subst): boolean;
}
/**
 * Configuration for the abstract relation engine
 */
interface AbstractRelationConfig {
    batchSize?: number;
    debounceMs?: number;
    enableCaching?: boolean;
    enableQueryMerging?: boolean;
    cacheManager?: CacheManager;
}
/**
 * Configuration for REST API data store
 */
interface RestDataStoreConfig {
    baseUrl: string;
    apiKey?: string;
    timeout?: number;
    headers?: Record<string, string>;
    pagination?: {
        limitParam?: string;
        offsetParam?: string;
        maxPageSize?: number;
    };
    features?: {
        /** Whether to include primary key in URL path instead of query params */
        primaryKeyInPath?: boolean;
        /** Whether API supports comma-separated values for IN operations */
        supportsInOperator?: boolean;
        /** Whether API supports field selection via query params */
        supportsFieldSelection?: boolean;
        /** Custom URL builder for different API patterns */
        urlBuilder?: (table: string, primaryKey?: string, primaryKeyValue?: any) => string;
        /** Custom query parameter formatter */
        queryParamFormatter?: (column: string, operator: string, value: any) => {
            key: string;
            value: string;
        };
    };
}

/**
 * Abstract relation engine that handles batching, caching, and query optimization
 * Works with any DataStore implementation
 */
declare class AbstractRelation<TOptions extends RelationOptions = RelationOptions> {
    private dataStore;
    private goalManager;
    private relationIdentifier;
    private _options?;
    private logger;
    private cacheManager;
    private config;
    constructor(dataStore: DataStore, goalManager: GoalManager, relationIdentifier: string, logger?: Logger, _options?: TOptions | undefined, config?: AbstractRelationConfig);
    /**
     * Create a goal for this relation
     */
    createGoal(queryObj: Record<string, Term>): Goal;
    /**
     * Execute query for a set of substitutions
     */
    private executeQueryForSubstitutions;
    /**
     * Build query parameters and execute via data store
     */
    private buildAndExecuteQuery;
    /**
     * Find related goals for merging and caching
     */
    private findRelatedGoals;
    /**
     * Find goals that are compatible for query merging
     */
    private findMergeCompatibleGoals;
    /**
     * Find goals that are compatible for result caching
     */
    private findCacheCompatibleGoals;
    /**
     * Process cached rows
     */
    private processCachedRows;
    /**
     * Process fresh query rows
     */
    private processFreshRows;
    private couldBenefitFromCache;
    private canMergeQueries;
    private collectWhereClausesFromSubstitutions;
    private collectAllWhereClauses;
    private collectColumnsFromGoals;
    private buildWhereConditions;
    private unifyRowWithQuery;
    /**
     * Create a batch processor utility
     */
    private createBatchProcessor;
}

/**
 * Default cache manager that stores cache entries in substitution objects
 * This matches the current SQL implementation behavior
 */
declare class DefaultCacheManager implements CacheManager {
    /**
     * Get cached rows for a goal from a substitution
     */
    get(goalId: number, subst: Subst): DataRow[] | null;
    /**
     * Set cached rows for a goal in a substitution
     */
    set(goalId: number, subst: Subst, rows: DataRow[], meta?: Record<string, any>): void;
    /**
     * Clear cache entries
     */
    clear(goalId?: number): void;
    /**
     * Check if cache entry exists
     */
    has(goalId: number, subst: Subst): boolean;
    /**
     * Remove cache entry for a specific goal from a substitution
     */
    delete(goalId: number, subst: Subst): void;
    /**
     * Get or create the cache map from a substitution
     */
    private getOrCreateRowCache;
    /**
     * Format cache for logging (matches current implementation)
     */
    formatCacheForLog(subst: Subst): Record<number, any>;
}

/**
 * Default implementation of GoalManager
 * Handles goal tracking, ID generation, and query logging
 */
declare class DefaultGoalManager implements GoalManager {
    private goals;
    private queries;
    private nextGoalId;
    getNextGoalId(): number;
    addGoal(goalId: number, relationIdentifier: string, queryObj: Record<string, Term>, batchKey?: string, relationOptions?: any): void;
    getGoalById(id: number): GoalRecord | undefined;
    getGoalsByBatchKey(batchKey: string): GoalRecord[];
    getGoals(): GoalRecord[];
    clearGoals(): void;
    addQuery(query: string): void;
    getQueries(): string[];
    clearQueries(): void;
    getQueryCount(): number;
}

/**
 * Factory for creating abstract relation systems
 * This is the main entry point for the abstract data layer
 */
declare class AbstractRelationFactory<TOptions extends RelationOptions = RelationOptions> {
    private dataStore;
    private goalManager;
    private logger;
    private config;
    constructor(dataStore: DataStore, logger?: Logger, config?: AbstractRelationConfig);
    /**
     * Get the appropriate relation identifier based on datastore type and options
     */
    private getRelationIdentifier;
    /**
     * Create a regular relation for a table
     */
    createRelation(table: string, options?: TOptions): (queryObj: Record<string, Term>) => Goal;
    /**
     * Create a symmetric relation for bidirectional queries
     */
    createSymmetricRelation(table: string, keys: [string, string], options?: RelationOptions): (queryObj: Record<string, Term>) => Goal;
    /**
     * Get debugging information
     */
    getQueries(): string[];
    clearQueries(): void;
    getQueryCount(): number;
    /**
     * Access the underlying data store
     */
    getDataStore(): DataStore;
    /**
     * Close the data store connection
     */
    close(): Promise<void>;
}
/**
 * Main factory function - creates an abstract relation system
 */
declare function createAbstractRelationSystem<TOptions extends RelationOptions = RelationOptions>(dataStore: DataStore, logger?: Logger, config?: AbstractRelationConfig): {
    rel: (table: string, options?: TOptions | undefined) => (queryObj: Record<string, Term>) => Goal;
    relSym: (table: string, keys: [string, string], options?: RelationOptions) => (queryObj: Record<string, Term>) => Goal;
    getQueries: () => string[];
    clearQueries: () => void;
    getQueryCount: () => number;
    getDataStore: () => DataStore;
    close: () => Promise<void>;
};

export { AbstractRelation, type AbstractRelationConfig, AbstractRelationFactory, type DataRow, type DataStore, DefaultCacheManager, DefaultGoalManager, type GoalManager, type QueryParams, type RelationOptions, type RestDataStoreConfig, type WhereCondition, createAbstractRelationSystem };
