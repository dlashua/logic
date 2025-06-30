import { resolve } from "path";
import { fileURLToPath } from 'url';
import knex from "knex";
import { set_parent_kid, set_relationship } from "../extended/familytree-rel.ts";
import { Term } from "../core/types.ts";
import { makeFacts, makeFactsSym } from "../facts/facts-memory.ts";

// FACT STORAGE

const __dirname = fileURLToPath(new URL('.', import.meta.url));

export const parent_kid = makeFacts();
set_parent_kid(parent_kid);

export const relationship = makeFactsSym();
set_relationship(relationship);

// FACT HELPERS
export const family = (parents: Term[], kids: Term[]) => {
  for (const parent of parents) {
    for (const kid of kids) {
      parent_kid.set(parent, kid);
    }
  }
};

const db = knex({
  client: "better-sqlite3",
  connection: {
    filename: resolve(__dirname, "../../data/family.db"),
  },
  useNullAsDefault: true,
});

const familyRows = await db("family").select("parent", "kid");
for (const row of familyRows) {
  parent_kid.set(row.parent, row.kid);
}

const relationshipRows = await db("relationship").select("a", "b");
for (const row of relationshipRows) {
  relationship.set(row.a, row.b);
}
await db.destroy();
