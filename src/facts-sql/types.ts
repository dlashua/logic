import type { Knex } from "knex";
import { Term } from "../core/types.ts";
import type { BaseConfig } from "../shared/types.ts";

// Re-export shared types
export type { 
  BaseConfig as Configuration,
  CacheConfig, 
  LogConfig, 
  QueryResult, 
  CacheType, 
  QueryParts,
  WhereClause 
} from "../shared/types.ts";

// SQL-specific types
export interface Pattern {
  readonly table: string;
  readonly goalIds: number[];
  readonly rows: any[];
  readonly ran: boolean;
  readonly selectCols: Record<string, Term>;
  readonly whereCols: Record<string, Term>;
  readonly queries: string[];
  readonly timestamp?: number; // For TTL support
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

export interface RelDBDependencies {
  db: Knex;
  config: BaseConfig;
}

export interface RelationOptions {
  fullScanKeys?: string[];
  cacheTTL?: number; // Time-to-live in milliseconds, defaults to 3000ms
}

export interface CacheEntry {
  data: any[];
  timestamp: number;
}

export type FullScanCache = Record<string, Record<string, CacheEntry>>;