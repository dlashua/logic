// Observable-based Relations Export
// -----------------------------------------------------------------------------

// List operations
export {
  membero,
  firsto,
  resto,
  appendo,
  lengtho,
  permuteo,
  mapo,
  removeFirsto,
  alldistincto
} from './lists.ts';

// Aggregate operations
export {
  collecto,
  collect_distincto,
  counto,
  group_by_collecto,
  group_by_counto,
  groupByGoal,
  aggregateRelFactory,
  groupAggregateRelFactory
} from './aggregates.ts';

// Numeric operations
export {
  gto,
  lto,
  gteo,
  lteo,
  pluso,
  multo
} from './numeric.ts';

// Control flow operations
export {
  uniqueo,
  not,
  neqo,
  onceo,
  succeedo,
  failo,
  groundo,
  nonGroundo
} from './control.ts';