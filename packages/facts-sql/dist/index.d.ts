import * as _swiftfall_logic from '@swiftfall/logic';
import { BaseConfig } from '@swiftfall/logic';
import * as _swiftfall_facts_abstract from '@swiftfall/facts-abstract';
import { Knex } from 'knex';

/**
 * SQL implementation using the abstract data layer
 * This is a drop-in replacement for the old facts-sql module
 */
declare const makeRelDB: (knex_connect_options: Knex.Config, options?: Record<string, string>, configOverrides?: Partial<BaseConfig>) => Promise<{
    rel: (table: string, options?: _swiftfall_facts_abstract.RelationOptions | undefined) => (queryObj: Record<string, _swiftfall_logic.Term>) => _swiftfall_logic.Goal;
    relSym: (table: string, keys: [string, string], options?: _swiftfall_facts_abstract.RelationOptions) => (queryObj: Record<string, _swiftfall_logic.Term>) => _swiftfall_logic.Goal;
    db: Knex<any, unknown[]>;
    getQueries: () => string[];
    clearQueries: () => void;
    getQueryCount: () => number;
    close: () => Promise<void>;
}>;

export { makeRelDB };
