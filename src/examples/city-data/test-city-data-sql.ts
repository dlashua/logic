import util from "node:util";
import { query } from "../../query.ts";
import { and, eq } from "../../core/combinators.ts";
import { gto } from "../../relations/numeric.ts";
import { lengtho } from "../../relations/lists.ts";
import { group_by_collecto } from "../../relations/aggregates-subqueries.ts";
import { city, relDB } from "./city-data-sql.ts"

const timeit = async (name: string, fn: () => any) => {
  const start = Date.now();
  const res = await fn();
  const elapsed = Date.now() - start;
  console.log({
    name,
    elapsed,
  })
}

await timeit("sql", async () => {
  console.log("sql way...");

  const results = await query()
    // .select($ => ({
    //   state: $.state,
    //   city_cnt: $.city_cnt,
    // }))
    .where(
      ($) => [
        // eq($.id, 12157107),
        city({
          id: $.id,
          country_code: "PE" 
        }),
        city({
          id: $.id,
          city: $.in_city
        }),

        // city({
        //   id: $.id2,
        //   country_code: "AU" 
        // }),
        // city({
        //   id: $.id2,
        //   state: $.in_state2
        // }),


        // group_by_collecto(
        //   $.in_state,
        //   $.city,
        //   and(
        //     city({
        //       id: $.id,
        //       country_code: "US" 
        //     }),
        //     city({
        //       id: $.id,
        //       state: $.in_state 
        //     }),
        //     city({
        //       id: $.id,
        //       city: $.city 
        //     }),
        //     city({
        //       id: $.id,
        //       population: $.pop 
        //     }),
        //     gto($.pop, 10000),
        //   ),
        //   $.state,
        //   $.cities,
        // ),
        // lengtho($.cities, $.city_cnt),
      ])
    .limit(10)
    .toArray()
    ;


  console.log("all sql queries", relDB.getQueries());
  console.log({
    results: results,
    results_count: results.length,
    queries_count: relDB.getQueryCount() 
  });
  return results;
});

process.exit();