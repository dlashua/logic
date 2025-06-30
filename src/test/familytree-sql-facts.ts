import { resolve } from "path";
import { fileURLToPath } from 'url';
import { FamilytreeRelations } from "../extended/familytree-rel.ts";
// import { makeRelDB } from "../facts-sql.ts";
import { Term } from "../core/types.ts";
import { makeRelDB } from "../facts-sql/facts-sql-refactored.ts";

const __dirname = fileURLToPath(new URL('.', import.meta.url));

export const relDB = await makeRelDB({
  client: "better-sqlite3",
  connection: {
    filename: resolve(__dirname, "../../data/family.db"),
  },
  useNullAsDefault: true,
});
const PK = await relDB.rel("family", {
  fullScanKeys: ["parent", "kid"]
});
const R = await relDB.relSym("relationship", ["a", "b"], {
  fullScanKeys: ["a", "b"]
});

const parent_kid = (p: Term, k: Term) =>
  PK({
    parent: p,
    kid: k,
  });

const relationship = (a: Term<string|number>, b: Term<string|number>) => 
  R({
    a,
    b,
  });
    
// RELATIONS
export const familytree = new FamilytreeRelations(parent_kid, relationship);

// await relDB.registerSqlOptimizer();