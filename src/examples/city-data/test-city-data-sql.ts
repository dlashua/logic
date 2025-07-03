import { query } from "../../query.ts";
import { and } from "../../core/combinators.ts";
import { gto } from "../../relations/numeric.ts";
import { lengtho } from "../../relations/lists.ts";
import { group_by_collecto } from "../../relations/aggregates.ts";
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

  const results = query()
    // .select($ => ({
    //   state: $.state,
    //   city_cnt: $.city_cnt,
    // }))
    .where(
      ($) => [
        city({
          id: $.id,
          country_code: "US" 
        }),
        city({
          id: $.id,
          state: $.in_state 
        }),


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
    .limit(10);

  const res = [];
  for await (const item of results) {
    res.push(item);
  }
  console.log(relDB.getQueries());
  console.log({
    results_count: res.length,
    queries_count: relDB.getQueryCount() 
  });
  return res;
});

process.exit();