import { resolve } from "path";
import { fileURLToPath } from 'url';
import { FamilytreeRelations } from "../extended/familytree-rel.ts";
// import { makeRelDB } from "../facts-sql.ts";
import { Term } from "../core/types.ts";
import { makeRelDB } from "../facts-sql/index.ts";

const __dirname = fileURLToPath(new URL('.', import.meta.url));

export const relDB = await makeRelDB({
  client: "better-sqlite3",
  connection: {
    filename: resolve(__dirname, "../../data/family.db"),
  },
  useNullAsDefault: true,
});
const PK = await relDB.rel(
  "family",
  // {
  // primaryKey: "parent",
  // selectColumns: ["parent", "kid"],
  // } 
);
const R = await relDB.relSym(
  "relationship", 
  ["a", "b"], 
  // {
  // primaryKey: "a",
  // selectColumns: ["a", "b"],
  // } 
);
const I = await relDB.rel(
  "people_info"
);

export const parent_kid = (p: Term, k: Term) =>
  PK({
    parent: p,
    kid: k,
  });

export const relationship = (a: Term<string|number>, b: Term<string|number>) => 
  R({
    a,
    b,
  });

export const info_color = (person: Term<string>, color: Term<string>) =>
  I({
    person,
    color,
  });

export const info_number = (person: Term<string>, number: Term<number>) =>
  I({
    person,
    number,
  })


    
// RELATIONS
export const familytree = new FamilytreeRelations(parent_kid, relationship);

// await relDB.registerSqlOptimizer();