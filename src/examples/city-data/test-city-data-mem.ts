import { query } from "../../query.ts";
import { and } from "../../core/combinators.ts";
import { gto } from "../../relations/numeric.ts";
import { lengtho } from "../../relations/lists.ts";
import { group_by_collecto } from "../../relations/aggregates.ts";
import {
  loadData,
  acquireData,
  city,
  state,
  countrycode,
  population
} from "./city-data-mem.ts"


await acquireData();
await loadData();

const timeit = async (name: string, fn: () => any) => {
  const start = Date.now();
  const res = await fn();
  const elapsed = Date.now() - start;
  console.log({
    name,
    elapsed,
  })
}

await timeit("logic", async () => {
  console.log("logic way...");

  const results = query()
    .select($ => ({
      state: $.state,
      city_cnt: $.city_cnt,
    }))
    .where(
      ($) => [
        group_by_collecto(
          $.in_state,
          $.city,
          and(
            countrycode($.id, "US"),
            state($.id, $.in_state),
            city($.id, $.city),
            population($.id, $.pop),
            gto($.pop, 10000),
          ),
          $.state,
          $.cities,
        ),
        lengtho($.cities, $.city_cnt),
      ])
    // 10,

  const res = [];
  for await (const item of results) {
    res.push(item);
  }
  console.log(res.length);
  return res;
});