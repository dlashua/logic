import * as logic from 'logic';
import { BaseConfig } from 'logic';
import * as facts_abstract from 'facts-abstract';
import { Knex } from 'knex';

/**
 * SQL implementation using the abstract data layer
 * This is a drop-in replacement for the old facts-sql module
 */
declare const makeRelDB: (knex_connect_options: Knex.Config, options?: Record<string, string>, configOverrides?: Partial<BaseConfig>) => Promise<{
    rel: (table: string, options?: facts_abstract.RelationOptions | undefined) => (queryObj: Record<string, logic.Term>) => logic.Goal;
    relSym: (table: string, keys: [string, string], options?: facts_abstract.RelationOptions) => (queryObj: Record<string, logic.Term>) => logic.Goal;
    db: Knex<any, unknown[]>;
    getQueries: () => string[];
    clearQueries: () => void;
    getQueryCount: () => number;
    close: () => Promise<void>;
}>;

export { makeRelDB };
