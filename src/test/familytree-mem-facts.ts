import { makeFacts, makeFactsSym } from "@codespiral/facts";
import type { Term } from "@codespiral/logic";
import knex from "knex";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { FamilytreeRelations } from "../extended/familytree-rel.ts";

// FACT STORAGE

const __dirname = fileURLToPath(new URL(".", import.meta.url));

const parent_kid = makeFacts();
const relationship = makeFactsSym();

export const familytree = new FamilytreeRelations(parent_kid, relationship);

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
