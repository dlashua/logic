import { resolve } from "path";
import { fileURLToPath } from 'url';
import { and, query, Subst } from "../core.ts"
import { makeRelDB } from "../facts-sql/facts-sql-refactored.ts";
import { relDB } from "./familytree-sql-facts.ts";

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

  function sleepGoal(ms: number) {
    return async function* sleepGoalGen(s: Subst) {
      await new Promise((resolve) => setTimeout(resolve, ms));
      yield s;
    }
  }



  const PK = relDB.rel("family", {
    cacheTTL: 10000, // Very short TTL for testing - 100ms
  });
  const runQuery = async (pause: number) => {
    console.log("=== Starting new query execution ===");
    console.log(`TTL: 500, Pause: ${pause}\n`);
    const r = await query().select("*").where($ => 
      and(
        PK({
          parent: "daniel",
          kid: $.kid 
        }),
        sleepGoal(pause), // Sleep longer than TTL to force expiration
        PK({
          parent: "daniel",
          kid: $.kid2
        })
      )
    ).toArray();
    console.log("Query result:", r);
    console.log("Database queries executed:", relDB.realQueries.length);
    // relDB.realQueries = [];
  }

  console.log("\nPause is longer than TTL. Should see 2 queries.");
  await runQuery(600);
  console.log("\nPause is shorter than TTL. Should see 3 queries: 1 new query (and 2 from before).");
  await runQuery(400);
  console.log("\nPause is shorter than TTL. Should see 4 queries: 1 new query (and 3 from before).");
  await runQuery(300);
  console.log("\nPause is shorter than TTL. Should see 5 queries: 1 new query (and 4 from before).");
  await runQuery(200);
  console.log("\nMight see 0 or 1 new query.");
  await runQuery(100);
}

await testCacheTTL();
await relDB.db.destroy();
process.exit();