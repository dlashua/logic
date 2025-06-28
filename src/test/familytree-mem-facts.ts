// eslint-disable-next-line import/no-named-as-default
import knex from "knex";
import { set_parent_kid, set_relationship } from "../extended/familytree-rel.ts";
import { Term } from "../core.ts";
import { makeFacts, makeFactsSym } from "../facts.ts";
// FACT STORAGE

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
    filename: "./family.db",
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
