import { makeRelDB } from "../facts-sql/facts-sql-refactored.ts";

// Test the fullScanKeys functionality
async function testFullScanKeys() {
  console.log("=== Testing fullScanKeys functionality ===");
  
  // Create test database with some sample data
  const relDB = await makeRelDB({
    client: 'better-sqlite3',
    connection: {
      filename: ':memory:'
    }
  });
  
  // Create test table
  await relDB.db.schema.createTable('test_people', table => {
    table.string('name');
    table.string('city');
    table.integer('age');
  });
  
  // Insert test data
  await relDB.db('test_people').insert([
    { name: 'alice', city: 'boston', age: 25 },
    { name: 'bob', city: 'boston', age: 30 },
    { name: 'charlie', city: 'boston', age: 35 },
    { name: 'diana', city: 'new_york', age: 28 },
    { name: 'eve', city: 'new_york', age: 32 }
  ]);
  
  console.log("\n1. Testing regular relation (no fullScanKeys):");
  const regularPeople = relDB.rel('test_people');
  
  // First query - should execute normal query
  console.log("First query for boston people:");
  let count = 0;
  for await (const result of regularPeople({ city: 'boston', name: 'alice' })(new Map())) {
    count++;
  }
  console.log(`Found ${count} results`);
  console.log(`Regular queries executed: ${relDB.realQueries.length}`);
  
  console.log("\n2. Testing fullScanKeys relation:");
  // Create relation with fullScanKeys for 'city'
  const fullScanPeople = relDB.rel('test_people', { fullScanKeys: ['city'] });
  
  // Reset query counter
  const initialQueryCount = relDB.realQueries.length;
  
  // First query - should execute full scan for city='boston'
  console.log("First fullScan query for boston people:");
  count = 0;
  for await (const result of fullScanPeople({ city: 'boston', name: 'alice' })(new Map())) {
    count++;
  }
  console.log(`Found ${count} results`);
  const afterFirstQuery = relDB.realQueries.length;
  console.log(`Queries after first fullScan: ${afterFirstQuery - initialQueryCount}`);
  
  // Second query with same city - should hit cache
  console.log("\nSecond fullScan query for boston people (different name):");
  count = 0;
  for await (const result of fullScanPeople({ city: 'boston', name: 'bob' })(new Map())) {
    count++;
  }
  console.log(`Found ${count} results`);
  const afterSecondQuery = relDB.realQueries.length;
  console.log(`Queries after second fullScan: ${afterSecondQuery - afterFirstQuery} (should be 0 - cache hit)`);
  
  // Third query with different city - should execute new full scan
  console.log("\nThird fullScan query for new_york people:");
  count = 0;
  for await (const result of fullScanPeople({ city: 'new_york', name: 'diana' })(new Map())) {
    count++;
  }
  console.log(`Found ${count} results`);
  const afterThirdQuery = relDB.realQueries.length;
  console.log(`Queries after third fullScan: ${afterThirdQuery - afterSecondQuery} (should be 1 - new full scan)`);
  
  console.log("\n=== All queries executed ===");
  relDB.realQueries.slice(initialQueryCount).forEach((query, index) => {
    console.log(`${index + 1}: ${query}`);
  });
  
  await relDB.db.destroy();
  console.log("\n=== Test completed ===");
}

testFullScanKeys().catch(console.error);