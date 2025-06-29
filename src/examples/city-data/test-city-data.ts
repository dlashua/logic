import { aggregateVarMulti } from "../../facts.ts";
import { run } from "../../old_run.ts";
import {
  loadData,
  acquireData,
  city,
  state,
  countrycode,
  population
} from "./city-data.ts"
import { and, gtc } from "./relations.ts";


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

  const results = run(
    (v_state, v_city, v_id, v_pop, v_city_cnt) => [
      {
        v_state,
        v_city_cnt,
      },
      and(
        aggregateVarMulti(
          [v_state],
          [v_city],
          and(
            countrycode(v_id, "US"),
            state(v_id, v_state),
            city(v_id, v_city),
            population(v_id, v_pop),
            gtc(v_pop, 10000),
          ),
        ),
        arrayLength(v_city, v_city_cnt),
      )
    ],
    // 10,
    Infinity,
  )

  const res = [];
  for await (const item of results) {
    res.push(item);
  }
  console.log(res.length);
  return res;
});


