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
  aggregateRelFactory,
  groupAggregateRelFactory
} from './aggregates-subqueries.ts';

// Numeric operations
export {
  gto,
  lto,
  gteo,
  lteo,
  pluso,
  multo,
  maxo,
  mino
} from './numeric.ts';

// Object operations
export {
  extract,
  extractEach
} from './objects.ts';

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