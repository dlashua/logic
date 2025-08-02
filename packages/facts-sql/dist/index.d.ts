import * as _codespiral_logic from '@codespiral/logic';
import { BaseConfig } from '@codespiral/logic';
import * as _codespiral_facts_abstract from '@codespiral/facts-abstract';
import { Knex } from 'knex';

/**
 * SQL implementation using the abstract data layer
 * This is a drop-in replacement for the old facts-sql module
 */
declare const makeRelDB: (knex_connect_options: Knex.Config, options?: Record<string, string>, configOverrides?: Partial<BaseConfig>) => Promise<{
    rel: (table: string, options?: _codespiral_facts_abstract.RelationOptions | undefined) => (queryObj: Record<string, _codespiral_logic.Term>) => _codespiral_logic.Goal;
    relSym: (table: string, keys: [string, string], options?: _codespiral_facts_abstract.RelationOptions) => (queryObj: Record<string, _codespiral_logic.Term>) => _codespiral_logic.Goal;
    db: Knex<any, unknown[]>;
    getQueries: () => string[];
    clearQueries: () => void;
    getQueryCount: () => number;
    close: () => Promise<void>;
}>;

export { makeRelDB };
