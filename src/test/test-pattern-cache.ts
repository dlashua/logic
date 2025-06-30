#!/usr/bin/env tsx

import { resolve } from "path";
import { fileURLToPath } from 'url';
import { query } from "../query.ts";
import { makeRelDB } from "../facts-sql/facts-sql-refactored.ts";
import { Term } from "../core/types.ts";

const __dirname = fileURLToPath(new URL('.', import.meta.url));

async function testPatternCache() {
  console.log("=== Testing Pattern Cache Behavior ===");
  
  const relDB = await makeRelDB({
    client: "better-sqlite3",
    connection: {
      filename: resolve(__dirname, "../../data/family.db"),
    },
    useNullAsDefault: true,
  });

  const PK = relDB.rel("family", {
    fullScanKeys: ["parent", "kid"]
  });

  const parent_kid = (p: Term<string>, k: Term<string>) => PK({
    parent: p,
    kid: k 
  });

  console.log("1. First simple query");
  const queryCount1 = relDB.realQueries.length;
  const q1 = query().where($ => [parent_kid("daniel", $.kid)]);
  const results1 = [];
  for await (const row of q1) {
    if (row === null) break;
    results1.push(row);
  }
  console.log(`   Found ${results1.length} results, executed ${relDB.realQueries.length - queryCount1} database queries`);

  console.log("2. Identical second query - should use pattern cache");
  const queryCount2 = relDB.realQueries.length;
  const q2 = query().where($ => [parent_kid("daniel", $.kid)]);
  const results2 = [];
  for await (const row of q2) {
    if (row === null) break;
    results2.push(row);
  }
  console.log(`   Found ${results2.length} results, executed ${relDB.realQueries.length - queryCount2} database queries`);

  console.log("3. Different value - should execute new query");
  const queryCount3 = relDB.realQueries.length;
  const q3 = query().where($ => [parent_kid("jess", $.kid)]);
  const results3 = [];
  for await (const row of q3) {
    if (row === null) break;
    results3.push(row);
  }
  console.log(`   Found ${results3.length} results, executed ${relDB.realQueries.length - queryCount3} database queries`);

  console.log("4. Complex query with multiple relations");
  const queryCount4 = relDB.realQueries.length;
  const q4 = query()
    .where($ => [
      parent_kid($.parent, $.kid),
      parent_kid($.parent, "celeste")
    ])
    .limit(5);
  const results4 = [];
  for await (const row of q4) {
    if (row === null) break;
    results4.push(row);
  }
  console.log(`   Found ${results4.length} results, executed ${relDB.realQueries.length - queryCount4} database queries`);

  console.log("5. Identical complex query - should use pattern cache");
  const queryCount5 = relDB.realQueries.length;
  const q5 = query()
    .where($ => [
      parent_kid($.parent, $.kid),
      parent_kid($.parent, "celeste")
    ])
    .limit(5);
  const results5 = [];
  for await (const row of q5) {
    if (row === null) break;
    results5.push(row);
  }
  console.log(`   Found ${results5.length} results, executed ${relDB.realQueries.length - queryCount5} database queries`);

  console.log(`\nTotal database queries executed: ${relDB.realQueries.length}`);
  console.log("Recent queries:", relDB.realQueries.slice(-5));
  
  await relDB.db.destroy();
}

testPatternCache().catch(console.error);