import { set_parent_kid, set_relationship } from "../extended/familytree-rel.ts";
// import { makeRelDB } from "../facts-sql-optimized.ts";
import { makeRelDB } from "../facts-sql.ts";

import { Term } from "../core.ts"
import { maybeProfile, Rel } from "../relations.ts";



export const relDB = await makeRelDB({
  client: "better-sqlite3",
  connection: {
    filename: "./family.db",
  },
  useNullAsDefault: true,
}, { 
  enableBatching: 'true',
  batchTimeout: '5'
});
const PK = await relDB.rel("family");
const R = await relDB.relSym("relationship", ["a", "b"]);

export const parent_kid = Rel(function parent_kid (p: Term, k: Term) {
  return maybeProfile(PK({
    parent: p,
    kid: k,
  }))
});
export const relationship = Rel(function relationship (a: Term<string|number>, b: Term<string|number>) { return maybeProfile(R({
  a,
  b,
}))});
// RELATIONS
set_parent_kid(parent_kid);
set_relationship(relationship);

// await relDB.registerSqlOptimizer();
