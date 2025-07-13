import "@dotenvx/dotenvx/config";
import { inspect } from "node:util";
import { resolve } from "path";
import { fileURLToPath } from 'url';
import { X } from "vitest/dist/chunks/reporters.d.BFLkQcL6.js";
import { makeRelREST } from "../facts-rest/index.ts";
import { query } from "../query.ts";
import { lvar } from "../core/kernel.ts";
import {
  gteo,
  maxo,
  extractEach,
  membero,
  lengtho,
  lteo
} from "../relations/index.ts"
import { Term } from "../core/types.ts";
import {
  and,
  eq,
  fresh,
  or,
  Subquery
} from "../core/combinators.ts";
import { group_by_collect_streamo, group_by_count_streamo, sort_by_streamo, take_streamo } from "../relations/aggregates.ts"
import { group_by_collecto , collect_distincto } from "../relations/aggregates-subqueries.ts";
import { SqlRelationCache } from "../facts-rest/relation-cache.ts";
import { substLog } from "../relations/control.ts";

const __dirname = fileURLToPath(new URL('.', import.meta.url));

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

const restCache = new SqlRelationCache({
  knexConfig: {
    client: "better-sqlite3",
    connection: {
      filename: resolve(__dirname, "../../data/rest_cache.db"),
    },
    useNullAsDefault: true,
  },
  tableName: "cache",
  ttlSeconds: 60 * 60 * 6,
})

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
  cache: restCache,
});

const search = tmdbApi.rel("/search/:type");
const movie = tmdbApi.rel("/movie/:id");
const movie_credits = tmdbApi.rel("/movie/:id/credits")
const discover_movie = tmdbApi.rel("/discover/movie")

const SCIFI_GENRE = 878;

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
    discover_movie({
      with_genres: SCIFI_GENRE,
      sort_by: "popularity.desc",
      region: "United States",
      id: $.id,
      title: $.title,
      // primary_release_year: $.release_year,
      "primary_release_date.gte": "1990-01-01",
      "primary_release_date.lte": "1999-12-31",
      popularity: $.movie_popularity,
      // release_date: $.release_date,
      limit: 1000,
    }),
    sort_by_streamo($.movie_popularity, "desc"),
    take_streamo(30),

    movie_credits({
      id: $.id,
      cast: $._cast,
    }),
    extractEach($._cast, {
      name: $.actor_name,
      popularity: $.actor_popularity,
      order: $.actor_movie_order,
    }),
    lteo($.actor_movie_order, 9),
    gteo($.actor_popularity, 3),

    group_by_collect_streamo($.actor_name, $.title, $.titles, true),
    lengtho($.titles, $.titles_count),

    sort_by_streamo($.titles_count, "desc"),

  ])
  // .select(
  //   $ => ({
  //     title_actor: $.actor_name,
  //     count: $.titles_count,
  //     // titles: $.titles,
  //   })
  // )
  .toArray();
  
console.log("Results:", inspect(results, {
  depth: null,
  colors: true 
}));
// console.log("Queries", tmdbApi.getQueries());
process.exit();

