import { query, and, gtc } from "../../core.ts";
import { arrayLength } from "../../relations-list.ts";
import { aggregateVarMulti } from "../../relations-agg.ts";
import {
  loadData,
  acquireData,
  city,
  state,
  countrycode,
  population
} from "./city-data.ts"


await acquireData();
await loadData();

const out = (d: unknown) => console.dir(d, {
  depth: 100 
})
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

        aggregateVarMulti(
          [$.state],
          [$.city],
          and(
            countrycode($.id, "US"),
            state($.id, $.state),
            city($.id, $.city),
            population($.id, $.pop),
            gtc($.pop, 10000),
          ),
        ),
        arrayLength($.city, $.city_cnt),
      
      ])
    // 10,

  const res = [];
  for await (const item of results) {
    res.push(item);
  }
  console.log(res.length);
  return res;
});


