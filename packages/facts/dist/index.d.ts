import { Term, Goal, BaseConfig, Logger } from '@swiftfall/logic';

interface FactRelation {
    (...query: Term[]): Goal;
    set: (...fact: Term[]) => void;
    raw: Term[][];
    indexes: Map<number, Map<any, Set<number>>>;
}
interface FactObjRelation {
    (queryObj: Record<string, Term>): Goal;
    set: (factObj: Record<string, Term>) => void;
    update: (where: Partial<Record<string, Term>>, newValues: Record<string, Term>) => void;
    upsert: (where: Partial<Record<string, Term>>, newValues: Record<string, Term>) => void;
    raw: Record<string, Term>[];
    indexes: Map<string, Map<any, Set<number>>>;
    keys: string[];
}
interface FactRelationConfig {
    enableLogging?: boolean;
    enableIndexing?: boolean;
}
interface FactPattern {
    readonly query: Term[] | Record<string, Term>;
    readonly grounded: boolean[];
    readonly indexablePositions: (number | string)[];
}
interface FactManagerDependencies {
    logger: any;
    cache: any;
    config: FactRelationConfig;
}

declare const makeFacts: (config?: Partial<BaseConfig>, factConfig?: FactRelationConfig) => FactRelation;
declare const makeFactsObj: (keys: string[], config?: Partial<BaseConfig>, factConfig?: FactRelationConfig) => FactObjRelation;
declare const makeFactsSym: (config?: Partial<BaseConfig>, factConfig?: FactRelationConfig) => FactRelation;
declare const makeFactsObjSym: (keys: string[], config?: Partial<BaseConfig>, factConfig?: FactRelationConfig) => FactObjRelation;

declare class MemoryObjRelation {
    private keys;
    private logger;
    private config;
    /**
     * Update facts matching a where-clause with new values.
     * @param where - fields and values to match
     * @param newValues - fields and values to update
     */
    private updateFacts;
    /**
     * Remove a fact index from a Map<value, Set<index>>
     */
    private removeFromIndex;
    private facts;
    private indexes;
    private goalIdCounter;
    constructor(keys: string[], logger: Logger, config: FactRelationConfig);
    createRelation(): FactObjRelation;
    private generateGoalId;
    private createGoal;
    private processFacts;
    private addFact;
}

declare class MemoryRelation {
    private logger;
    private config;
    private facts;
    private indexes;
    private goalIdCounter;
    constructor(logger: Logger, config: FactRelationConfig);
    createRelation(): FactRelation;
    private generateGoalId;
    private createGoal;
    private processFacts;
    private addFact;
}

interface FactRelationFactoryDependencies {
    logger: Logger;
    config: FactRelationConfig;
}
declare class FactRelationFactory {
    private deps;
    constructor(deps: FactRelationFactoryDependencies);
    createArrayRelation(): FactRelation;
    createObjectRelation(keys: string[]): FactObjRelation;
    createSymmetricArrayRelation(): FactRelation;
    createSymmetricObjectRelation(keys: string[]): FactObjRelation;
}

declare class SymmetricMemoryRelation {
    private memoryRelation;
    constructor(logger: Logger, config: FactRelationConfig);
    createRelation(): FactRelation;
}
declare class SymmetricMemoryObjRelation {
    private keys;
    private memoryObjRelation;
    constructor(keys: string[], logger: Logger, config: FactRelationConfig);
    createRelation(): FactObjRelation;
}

export { type FactManagerDependencies, type FactObjRelation, type FactPattern, type FactRelation, type FactRelationConfig, FactRelationFactory, MemoryObjRelation, MemoryRelation, SymmetricMemoryObjRelation, SymmetricMemoryRelation, makeFacts, makeFactsObj, makeFactsObjSym, makeFactsSym };
