import { query } from "../query-builder.ts";
import { cousinsAgg } from "../extended/familytree-rel.ts"
import { relDB } from "./familytree-sql-facts.ts";
import { getCousinsOf, QUERIES } from "./direct-sql.ts";


async function sequentialPromises(promiseFunctions: (() => Promise<any>)[]) {
  const res = [];
  for (const promiseFunction of promiseFunctions) {
    res.push(await promiseFunction());
  }
  return res;
}

async function timeit(name: string, fn: () => Promise<any>, statsFn: () => (Promise<void> | void)) {
  if (global.gc) {
    global.gc();
  } else {
    console.log("Run with node --expose-gc for more accurate memory readings");
  }

  const startMem = process.memoryUsage().heapUsed;
  const start = performance.now();
  const res = await fn();
  const elapsed = performance.now() - start;
  const endMem = process.memoryUsage().heapUsed;
  const memUsed = (endMem - startMem) / 1024 / 1024;
  console.log(
    `${name} - elapsed: ${elapsed.toFixed(3)}ms, memory: ${memUsed.toFixed(3)}MB`,
    "\n" + res.map((one: any) => JSON.stringify(one)).join("\n") + "\n"
  );
  // await statsFn();
  // console.log("\n");
}

async function logic_test(people: string[], degree = 1, removal = 0) {
  return Promise.all(
    people.map(person => 
      query()
        .select("*")
        .where($ => cousinsAgg(person, $.o, degree, removal))
        .toArray()
        .then(cousinsO => cousinsO[0].o)
        .then(cousins => ({
          person,
          cousins: cousins.sort(), 
        }))
    )
  )
}

async function sql_test(people: string[], degree = 1, removal = 0) {
  return Promise.all(
    people.map(person => 
      getCousinsOf(person, degree, removal)
        .then(cousins => ({
          person,
          cousins: cousins.sort(), 
        }))
    )
  );
}

const people = ["daniel", "celeste", "alexh", "tucker"];
const tests = [
  {
    name: "cousins 1:-1",
    degree: 1,
    removal: -1,
  },
  {
    name: "cousins 1:0",
    degree: 1,
    removal: 0,
  },
  {
    name: "cousins 1:1",
    degree: 1,
    removal: 1,
  },
  {
    name: "cousins 2:-2",
    degree: 2,
    removal: -2,
  },
  {
    name: "cousins 2:0",
    degree: 2,
    removal: 0,
  },
  {
    name: "cousins 2:2",
    degree: 2,
    removal: 2,
  },
];
const testFns = [
  {
    name: "sql",
    fn: sql_test,
    statsFn:   () => {
      console.log({
      // queries: QUERIES,
        queryCount: QUERIES.length 
      })
    },
  },
  {
    name: "logic",
    fn: logic_test,
    statsFn:   () => {
      console.log({
        queryCount: relDB.realQueries.length 
      })
    },
  }
];

await sequentialPromises(tests.map(test => 
  () => sequentialPromises([
    ...testFns.map(testFn => 
      () => timeit(
        `${testFn.name} ${test.name}`,
        () => testFn.fn(people, test.degree, test.removal),
        testFn.statsFn,
      )
    ),
    async () => console.log("---------------------------------------------"),
  ])

))


