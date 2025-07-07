import { inspect } from "node:util";
import { makeRelREST } from "../facts-rest/index.ts";
import { query } from "../query.ts";
import { lvar } from "../core/kernel.ts";
import { membero } from "../relations/lists.ts";
import { eq, lift, project, projectJsonata } from "../core/combinators.ts";

// Example 1: Pokemon API with primary key in path
console.log("=== Pokemon API Test (Primary Key in Path) ===");
const pokemonApi = await makeRelREST({
  baseUrl: "https://pokeapi.co/api/v2",
  features: {
    primaryKeyInPath: true, // Use /pokemon/{id} URLs
    supportsFieldSelection: false, // Pokemon API doesn't support field selection
    supportsInOperator: false // Pokemon API doesn't support comma-separated values
  },
  pagination: {
    limitParam: 'limit',
    offsetParam: 'offset',
    maxPageSize: 20,
  },
});

const pokemon = pokemonApi.rel("pokemon", {
  restPrimaryKey: "name" 
});

const pokemon_species = pokemonApi.rel("pokemon-species", {
  restPrimaryKey: "name" 
});


const results = await query()
  .where($ => [
    membero($.name, ["charizard"]),
    // membero($.name, ["charmeleon"]),

    pokemon({
      name: $.name,
      species: $._species,
    }),

    pokemon({
      name: $.name,
      weight: $.weight,
      height: $.height,
    }),

    projectJsonata(
      $._species,
      "$.name",
      $.species_name
    ),

    pokemon_species({
      name: $.species_name,
      genera: $._species_genera,
      varieties: $._species_varieties,
    }),

    projectJsonata(
      $._species_varieties,
      "$[*].pokemon.name",
      $.varieties_list,
    ),

    projectJsonata(
      $._species_genera,
      "$[language.name='en'].genus",
      $.species_genus_en,
    ),

    
    membero($.more_name, $.varieties_list),
    pokemon({
      name: $.more_name,
      weight: $.more_weight,
      height: $.more_height,
    }),

  ])
  .toArray();
  
console.log("✅ Pokemon API Success!");
console.log("Results:", inspect(results, {
  depth: null,
  colors: true 
}));
console.log("Queries", pokemonApi.getQueries());

