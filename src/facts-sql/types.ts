import type { Knex } from "knex";
import { Term, Subst } from "../core.ts";

export interface WhereClause {
  readonly column: string;
  readonly value: Term;
  readonly operator?: 'eq' | 'gt' | 'lt' | 'in';
}

export interface Pattern {
  readonly table: string;
  readonly goalIds: number[];
  readonly rows: any[];
  readonly ran: boolean;
  readonly selectCols: Record<string, Term>;
  readonly whereCols: Record<string, Term>;
  readonly queries: string[];
  readonly last: {
    selectCols: Record<string, Term>[];
    whereCols: Record<string, Term>[];
  };
}

export interface SymmetricPattern {
  readonly table: string;
  readonly goalIds: number[];
  readonly rows: any[];
  readonly ran: boolean;
  readonly selectCols: Term[];
  readonly whereCols: Term[];
  readonly queries: string[];
  readonly last: {
    selectCols: Term[][];
    whereCols: Term[][];
  };
}

export interface QueryResult<T = any> {
  readonly rows: readonly T[];
  readonly fromCache: boolean;
  readonly cacheType?: 'pattern' | 'row' | 'query';
  readonly sql?: string;
}

export interface CacheConfig {
  readonly patternCacheEnabled: boolean;
  readonly rowCacheEnabled: boolean;
  readonly recordCacheEnabled: boolean;
}

export interface LogConfig {
  readonly enabled: boolean;
  readonly ignoredIds: Set<string>;
  readonly criticalIds: Set<string>;
}

export interface Configuration {
  readonly cache: CacheConfig;
  readonly logging: LogConfig;
}

export interface QueryParts {
  readonly selectCols: string[];
  readonly whereClauses: WhereClause[];
  readonly walkedQ: Record<string, Term>;
}

export type CacheType = 'pattern' | 'row' | 'query';

export interface RelDBDependencies {
  db: Knex;
  config: Configuration;
}

export type GoalFunction = (s: Subst) => AsyncGenerator<Subst, void, unknown>;