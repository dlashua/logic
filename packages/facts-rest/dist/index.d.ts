import * as _codespiral_facts_abstract from '@codespiral/facts-abstract';
import { RelationOptions, RestDataStoreConfig, AbstractRelationConfig } from '@codespiral/facts-abstract';
export { RestDataStoreConfig } from '@codespiral/facts-abstract';
import * as _codespiral_logic from '@codespiral/logic';

interface RelationCache {
    get(key: string): Promise<any | undefined>;
    set(key: string, value: any): Promise<void>;
    delete?(key: string): Promise<void>;
    clear?(): Promise<void>;
}

/**
 * REST-specific relation options (extends global RelationOptions)
 */
interface RestRelationOptions extends RelationOptions {
    pathTemplate?: string;
}

/**
 * REST API implementation using the abstract data layer
 * Example of how to create a facts system backed by a REST API
 */
declare const makeRelREST: (restConfig: RestDataStoreConfig & {
    cache?: RelationCache;
    cacheMethods?: string[];
}, config?: AbstractRelationConfig) => Promise<{
    rel: (pathTemplate: string, options?: RestRelationOptions & {
        cache?: RelationCache | null;
    }) => (queryObj: Record<string, _codespiral_logic.Term>) => _codespiral_logic.Goal;
    relSym: (table: string, keys: [string, string], options?: _codespiral_facts_abstract.RelationOptions) => (queryObj: Record<string, _codespiral_logic.Term>) => _codespiral_logic.Goal;
    getQueries: () => string[];
    clearQueries: () => void;
    getQueryCount: () => number;
    close: () => Promise<void>;
    getDataStore: () => _codespiral_facts_abstract.DataStore;
}>;

export { makeRelREST };
