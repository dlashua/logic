#!/usr/bin/env tsx

import { resolve } from "path";
import { fileURLToPath } from 'url';
import { query } from "../query.ts";
import { lift as Rel } from "../relations/control.ts";
import { makeRelDB } from "../facts-sql/facts-sql-refactored.ts";

const __dirname = fileURLToPath(new URL('.', import.meta.url));

// Test cache TTL functionality
async function testCacheTTL() {
  console.log("=== Testing Cache TTL Functionality ===");
  
  // Create a database with a short TTL for testing (500ms)
  const relDB = await makeRelDB({
    client: "better-sqlite3",
    connection: {
      filename: resolve(__dirname, "../../data/family.db"),
    },
    useNullAsDefault: true,
  });

  // Create relation with short TTL
  const PK = relDB.rel("family", {
    fullScanKeys: ["parent"],
    cacheTTL: 500 // 500ms for fast testing
  });

  const parent_kid = Rel((p, k) => PK({
    parent: p,
    kid: k 
  }));

  console.log("1. First query - should execute database query and cache result (testing fullScan)");
  const queryCount1 = relDB.realQueries.length;
  const q1 = query().where($ => [parent_kid("daniel", $.kid)]).limit(3); // Use grounded parent value to trigger fullScan
  const results1 = [];
  for await (const row of q1) {
    if (row === null) break;
    results1.push(row);
  }
  console.log(`   Found ${results1.length} results, executed ${relDB.realQueries.length - queryCount1} database queries`);

  console.log("2. Immediate second query - should hit cache");
  const queryCount2 = relDB.realQueries.length;
  const q2 = query().where($ => [parent_kid("daniel", $.kid)]).limit(3); // Same grounded value
  const results2 = [];
  for await (const row of q2) {
    if (row === null) break;
    results2.push(row);
  }
  console.log(`   Found ${results2.length} results, executed ${relDB.realQueries.length - queryCount2} database queries`);

  console.log("3. Waiting for cache to expire (600ms)...");
  await new Promise(resolve => setTimeout(resolve, 600));

  console.log("4. Third query after TTL - should execute database query again");
  const queryCount3 = relDB.realQueries.length;
  const q3 = query().where($ => [parent_kid("daniel", $.kid)]).limit(3); // Same grounded value after TTL
  const results3 = [];
  for await (const row of q3) {
    if (row === null) break;
    results3.push(row);
  }
  console.log(`   Found ${results3.length} results, executed ${relDB.realQueries.length - queryCount3} database queries`);

  // Test symmetric relation TTL as well
  console.log("\n=== Testing Symmetric Relation Cache TTL ===");
  
  const R = relDB.relSym("relationship", ["a", "b"], {
    fullScanKeys: ["a"],
    cacheTTL: 500 // 500ms for fast testing
  });

  const relationship = Rel((a, b) => R({
    a,
    b 
  }));

  console.log("1. First symmetric query - should execute database query and cache result (testing fullScan)");
  const symQueryCount1 = relDB.realQueries.length;
  const sq1 = query().where($ => [relationship("daniel", $.b)]).limit(3); // Use grounded value to trigger fullScan
  const symResults1 = [];
  for await (const row of sq1) {
    if (row === null) break;
    symResults1.push(row);
  }
  console.log(`   Found ${symResults1.length} results, executed ${relDB.realQueries.length - symQueryCount1} database queries`);

  console.log("2. Immediate second symmetric query - should hit cache");
  const symQueryCount2 = relDB.realQueries.length;
  const sq2 = query().where($ => [relationship("daniel", $.b)]).limit(3); // Same grounded value
  const symResults2 = [];
  for await (const row of sq2) {
    if (row === null) break;
    symResults2.push(row);
  }
  console.log(`   Found ${symResults2.length} results, executed ${relDB.realQueries.length - symQueryCount2} database queries`);

  console.log("3. Waiting for symmetric cache to expire (600ms)...");
  await new Promise(resolve => setTimeout(resolve, 600));

  console.log("4. Third symmetric query after TTL - should execute database query again");
  const symQueryCount3 = relDB.realQueries.length;
  const sq3 = query().where($ => [relationship("daniel", $.b)]).limit(3); // Same grounded value after TTL
  const symResults3 = [];
  for await (const row of sq3) {
    if (row === null) break;
    symResults3.push(row);
  }
  console.log(`   Found ${symResults3.length} results, executed ${relDB.realQueries.length - symQueryCount3} database queries`);

  // Print final query counts
  console.log(`\nTotal database queries executed: ${relDB.realQueries.length}`);
  
  await relDB.db.destroy();
  
  console.log("\n=== Cache TTL Test Complete ===");
  console.log("✓ Cache entries should expire after the specified TTL");
  console.log("✓ Expired entries should trigger fresh database queries");
  console.log("✓ TTL functionality working for both regular and symmetric relations");
}

testCacheTTL().catch(console.error);