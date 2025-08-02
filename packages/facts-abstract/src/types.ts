import type { Subst, Term } from "@codespiral/logic";

/**
 * Core goal record that tracks logic goals independently of data store implementation
 */
export interface GoalRecord {
  goalId: number;
  relationIdentifier: string; // Generic identifier - table name for SQL, path template for REST
  queryObj: Record<string, Term>;
  batchKey?: string;
  relationOptions?: RelationOptions;
}

/**
 * WHERE clause condition for data stores
 */
export interface WhereCondition {
  column: string;
  operator: "eq" | "in" | "gt" | "lt" | "gte" | "lte" | "like";
  value: any;
  values?: any[]; // for 'in' operator
}

/**
 * Query parameters passed to data store implementations
 */
export interface QueryParams {
  relationIdentifier: string;
  selectColumns: string[];
  whereConditions: WhereCondition[];
  limit?: number;
  offset?: number;
  relationOptions?: RelationOptions; // Will be typed as RestRelationOptions for REST
  goalId?: number;
  logQuery?: (queryString: string) => void;
}

/**
 * Result row from data store
 */
export type DataRow = Record<string, any>;

/**
 * Options for relation creation
 */
export interface RelationOptions {
  primaryKey?: string;
  selectColumns?: string[];
  fullScanKeys?: string[];
  // For REST APIs that need primary key in URL path
  restPrimaryKey?: string;
}

/**
 * Abstract data store interface that any backend must implement
 */
export interface DataStore {
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
export interface GoalManager {
  getNextGoalId(): number;

  addGoal(
    goalId: number,
    relationIdentifier: string,
    queryObj: Record<string, Term>,
    batchKey?: string,
    relationOptions?: RelationOptions,
  ): void;
  getGoalById(id: number): GoalRecord | undefined;
  getGoalsByBatchKey(batchKey: string): GoalRecord[];
  getGoals(): GoalRecord[];
  clearGoals(): void;

  // Query tracking for debugging
  addQuery(query: string): void;
  getQueries(): string[];
  clearQueries(): void;
  getQueryCount(): number;
}

/**
 * Cache entry for row caching
 */
export interface CacheEntry {
  data: DataRow[];
  timestamp: number;
  goalId: number;
  meta: Record<string, any> | undefined;
}

/**
 * Cache manager interface
 */
export interface CacheManager {
  get(goalId: number, subst: Subst): DataRow[] | null;
  set(
    goalId: number,
    subst: Subst,
    rows: DataRow[],
    meta?: Record<string, any>,
  ): void;
  clear(goalId?: number): void;
  has(goalId: number, subst: Subst): boolean;
}

/**
 * Configuration for the abstract relation engine
 */
export interface AbstractRelationConfig {
  batchSize?: number;
  debounceMs?: number;
  enableCaching?: boolean;
  enableQueryMerging?: boolean;
  cacheManager?: CacheManager;
}

/**
 * Configuration for REST API data store
 */
export interface RestDataStoreConfig {
  baseUrl: string;
  apiKey?: string;
  timeout?: number;
  headers?: Record<string, string>;
  pagination?: {
    limitParam?: string;
    offsetParam?: string;
    maxPageSize?: number;
  };
  // New generic features
  features?: {
    /** Whether to include primary key in URL path instead of query params */
    primaryKeyInPath?: boolean;

    /** Whether API supports comma-separated values for IN operations */
    supportsInOperator?: boolean;

    /** Whether API supports field selection via query params */
    supportsFieldSelection?: boolean;

    /** Custom URL builder for different API patterns */
    urlBuilder?: (
      table: string,
      primaryKey?: string,
      primaryKeyValue?: any,
    ) => string;

    /** Custom query parameter formatter */
    queryParamFormatter?: (
      column: string,
      operator: string,
      value: any,
    ) => { key: string; value: string };
  };
}
