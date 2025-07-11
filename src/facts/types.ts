import { Term, Subst, Goal } from "../core/types.ts";

export interface FactRelation {
  (...query: Term[]): Goal;
  set: (...fact: Term[]) => void;
  raw: Term[][];
  indexes: Map<number, Map<any, Set<number>>>;
}

export interface FactObjRelation {
  (queryObj: Record<string, Term>): Goal;
  set: (factObj: Record<string, Term>) => void;
  raw: Record<string, Term>[];
  indexes: Map<string, Map<any, Set<number>>>;
  keys: string[];
}

export interface FactRelationConfig {
  enableLogging?: boolean;
  enableIndexing?: boolean;
}

export interface FactPattern {
  readonly query: Term[] | Record<string, Term>;
  readonly grounded: boolean[];
  readonly indexablePositions: (number | string)[];
}

export interface FactManagerDependencies {
  logger: any;
  cache: any;
  config: FactRelationConfig;
}