import { inspect } from "node:util";
import { makeRelREST } from "../facts-rest/index.ts";
import { query } from "../query.ts";
import { lvar } from "../core/kernel.ts";
import { gteo, maxo, extractEach } from "../relations/index.ts";
import { Term } from "../core/types.ts";
import { and, eq, fresh, or } from "../core/combinators.ts";

// const tmdbSearchApi = await makeRelREST({
//   baseUrl: "https://api.themoviedb.org/3/search/",
//   features: {
//     supportsFieldSelection: false,
//     supportsInOperator: false,
//     primaryKeyInPath: false,
//   },
//   apiKey: process.env.TMDB_API_ACCESS_TOKEN,
//   pagination: {
//     limitParam: 'limit',
//     offsetParam: 'offset',
//     maxPageSize: 20,
//   },
// });

const tmdbApi = await makeRelREST({
  baseUrl: "https://api.themoviedb.org/3/",
  features: {
    supportsFieldSelection: false,
    supportsInOperator: false,
    // primaryKeyInPath: true,
  },
  apiKey: process.env.TMDB_API_ACCESS_TOKEN,
  pagination: {
    limitParam: 'limit',
    offsetParam: 'offset',
    maxPageSize: 20,
  },
});

const search = tmdbApi.rel("/search/:type");

const movie = tmdbApi.rel("/movie/:id");

const movie_credits = tmdbApi.rel("/movie/:id/credits")

const most_popular_movie = (query: Term<string>, data: Record<string, Term>) => 
  fresh((popularity, id) => and(
    search({
      type: "movie",
      query,
      id,
      popularity,
    }),
    maxo(popularity),
    "id" in data ? eq(data.id, id) : (so) => so,
    movie({
      id,
      ...data,
    })
  ));
  

const results = await query()
  .where($ => [

    most_popular_movie(
      "matrix",
      {
        id: $.id,
        title: $.title,
        release_date: $.release_date,
      }
    ),

    movie_credits({
      id: $.id,
      cast: $._cast,
    }),

    extractEach($._cast, {
      name: $.actor_name,
      popularity: $.actor_popularity,
      character: $.actor_character,
    }),

    gteo($.actor_popularity, 0.8),
    or(
      eq($.actor_name, "Robert Taylor"),
      eq($.actor_name, "Keanu Reeves"),
      // eq($.actor_character, "Neo"),
    )

  ])
  .toArray();
  
console.log("Results:", inspect(results, {
  depth: null,
  colors: true 
}));
console.log("Queries", tmdbApi.getQueries());
