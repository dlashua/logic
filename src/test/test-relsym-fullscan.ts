import { makeRelDB } from "../facts-sql/facts-sql-refactored.ts";

// Test the relSym fullScanKeys functionality
async function testRelSymFullScanKeys() {
  console.log("=== Testing relSym fullScanKeys functionality ===");
  
  // Create test database with symmetric relationship data
  const relDB = await makeRelDB({
    client: 'better-sqlite3',
    connection: {
      filename: ':memory:'
    }
  });
  
  // Create test table for symmetric relationships (like friendships, connections, etc.)
  await relDB.db.schema.createTable('connections', table => {
    table.string('person_a');
    table.string('person_b');
    table.string('type'); // friend, colleague, etc.
  });
  
  // Insert test data - symmetric relationships
  await relDB.db('connections').insert([
    { person_a: 'alice', person_b: 'bob', type: 'friend' },
    { person_a: 'alice', person_b: 'charlie', type: 'friend' },
    { person_a: 'bob', person_b: 'diana', type: 'colleague' },
    { person_a: 'charlie', person_b: 'eve', type: 'friend' },
    { person_a: 'diana', person_b: 'eve', type: 'colleague' }
  ]);
  
  console.log("\n1. Testing regular relSym (no fullScanKeys):");
  const regularConnections = relDB.relSym('connections', ['person_a', 'person_b']);
  
  // First query - should execute normal symmetric query
  console.log("First symmetric query for alice's connections:");
  let count = 0;
  for await (const result of regularConnections({ person_a: 'alice', person_b: 'bob' })(new Map())) {
    count++;
  }
  console.log(`Found ${count} results`);
  console.log(`Regular symmetric queries executed: ${relDB.realQueries.length}`);
  
  console.log("\n2. Testing relSym with fullScanKeys:");
  // Create symmetric relation with fullScanKeys for 'person_a'
  const fullScanConnections = relDB.relSym('connections', ['person_a', 'person_b'], { fullScanKeys: ['person_a'] });
  
  // Reset query counter
  const initialQueryCount = relDB.realQueries.length;
  
  // First query - should execute full scan for person_a='alice' (both directions)
  console.log("First fullScan symmetric query for alice:");
  count = 0;
  for await (const result of fullScanConnections({ person_a: 'alice', person_b: 'bob' })(new Map())) {
    count++;
  }
  console.log(`Found ${count} results`);
  const afterFirstQuery = relDB.realQueries.length;
  console.log(`Queries after first fullScan: ${afterFirstQuery - initialQueryCount}`);
  
  // Second query with same person_a - should hit cache
  console.log("\nSecond fullScan symmetric query for alice (different person_b):");
  count = 0;
  for await (const result of fullScanConnections({ person_a: 'alice', person_b: 'charlie' })(new Map())) {
    count++;
  }
  console.log(`Found ${count} results`);
  const afterSecondQuery = relDB.realQueries.length;
  console.log(`Queries after second fullScan: ${afterSecondQuery - afterFirstQuery} (should be 0 - cache hit)`);
  
  // Third query with different person_a - should execute new full scan
  console.log("\nThird fullScan symmetric query for bob:");
  count = 0;
  for await (const result of fullScanConnections({ person_a: 'bob', person_b: 'diana' })(new Map())) {
    count++;
  }
  console.log(`Found ${count} results`);
  const afterThirdQuery = relDB.realQueries.length;
  console.log(`Queries after third fullScan: ${afterThirdQuery - afterSecondQuery} (should be 2 - new symmetric full scan)`);
  
  // Fourth query using person_b with cached person_a - should still hit cache
  console.log("\nFourth query with cached person (reversed direction):");
  count = 0;
  for await (const result of fullScanConnections({ person_a: 'charlie', person_b: 'alice' })(new Map())) {
    count++;
  }
  console.log(`Found ${count} results`);
  const afterFourthQuery = relDB.realQueries.length;
  console.log(`Queries after fourth query: ${afterFourthQuery - afterThirdQuery} (should be 0 - cache hit from symmetric scan)`);
  
  console.log("\n=== All fullScan queries executed ===");
  relDB.realQueries.slice(initialQueryCount).forEach((query, index) => {
    console.log(`${index + 1}: ${query}`);
  });
  
  await relDB.db.destroy();
  console.log("\n=== RelSym fullScan test completed ===");
}

testRelSymFullScanKeys().catch(console.error);