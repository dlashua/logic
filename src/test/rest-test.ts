import { makeRelREST } from "../facts-rest/index.ts";
import { query } from "../query.ts";
import { lvar } from "../core/kernel.ts";
import { membero } from "../relations/lists.ts";

// Example 1: Pokemon API with primary key in path
console.log("=== Pokemon API Test (Primary Key in Path) ===");
const pokemonApi = await makeRelREST({
  baseUrl: "https://pokeapi.co/api/v2",
  features: {
    primaryKeyInPath: true, // Use /pokemon/{id} URLs
    supportsFieldSelection: false, // Pokemon API doesn't support field selection
    supportsInOperator: false // Pokemon API doesn't support comma-separated values
  }
});

const pokemonDB = pokemonApi.rel("pokemon", {
  restPrimaryKey: "name" // Primary key will be included in URL path
});

try {
  const results = await query()
    .where($ => [
      // membero($.name, ["charmeleon", "charizard", "metapod"]),
      membero($.name, ["charmeleon"]),

      pokemonDB({
        name: $.name, // This will become /pokemon/charmeleon
        weight: $.weight,
        height: $.height,
        species: $.species,
      })
    ])
    .toArray();
  
  console.log("✅ Pokemon API Success!");
  console.log("Results:", results);
  console.log("Queries", pokemonApi.getQueries());
} catch (error) {
  console.log("❌ Pokemon API error:", error.message);
}
