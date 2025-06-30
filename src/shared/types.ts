import { Term, Subst } from "../core/types.ts";

export interface BaseConfig {
  readonly cache: CacheConfig;
  readonly logging: LogConfig;
}

export interface CacheConfig {
  readonly patternCacheEnabled: boolean;
}

export interface LogConfig {
  readonly enabled: boolean;
  readonly ignoredIds: Set<string>;
  readonly criticalIds: Set<string>;
}

export interface QueryResult<T = any> {
  readonly rows: readonly T[];
  readonly fromCache: boolean;
  readonly cacheType?: 'pattern' | 'row' | 'query';
  readonly source?: string;
}

export type CacheType = 'pattern' | 'row' | 'query';

export type GoalFunction = (s: Subst) => AsyncGenerator<Subst, void, unknown>;

export interface WhereClause {
  readonly column: string;
  readonly value: Term;
  readonly operator?: 'eq' | 'gt' | 'lt' | 'in';
}

export interface QueryParts {
  readonly selectCols: string[];
  readonly whereClauses: WhereClause[];
  readonly walkedQ: Record<string, Term>;
}

export interface IndexMap<T = any> {
  get(key: T): Set<number> | undefined;
  set(key: T, value: Set<number>): void;
  has(key: T): boolean;
}

export interface IndexManager<K = any> {
  get(position: K): IndexMap | undefined;
  set(position: K, index: IndexMap): void;
  has(position: K): boolean;
}