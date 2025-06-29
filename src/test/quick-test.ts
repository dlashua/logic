// import R from 'rubico/index.js';
// import { pipe } from 'rubico/dist/rubico.mjs';
import assert, { deepEqual, deepStrictEqual } from "node:assert";
import { query } from "../old_query-builder.ts";
import {
  cousinOf,
  cousinsAgg,
  fullSiblingsAgg,
  grandparent_kid,
  grandparentAgg,
  greatgrandparentAgg,
  greatuncleAgg,
  halfSiblingOf,
  halfSiblingsAgg,
  kidsAgg,
  parentAgg,
  parentOf,
  person,
  secondcousinsAgg,
  siblingsAgg,
  stepParentAgg,
  stepParentOf,
  stepSiblingOf,
  stepSiblingsAgg,
  uncleAgg,
  set_parent_kid,
  set_relationship,
  kidOf,
  anyKidOf
} from "../extended/familytree-rel.ts"
import { membero } from "../relations-list.ts";
import { and, eq, fresh } from "../old_relations.ts";
import { lvar } from "../old_core.ts";
import { QUERIES } from "./direct-sql.ts";

console.log("START quick-test");

process.on('unhandledRejection', (reason) => {
  if (reason === null) {
    console.warn('End-of-stream marker (`null`) received.');
  } else {
    console.error('Unhandled Rejection:', reason);
  }
});


const BACKEND = "sql";
// const BACKEND = "mem";

/**********************************************************/
const closeFns: (() => void)[] = [];
async function loadBackend(backend: string) {
  if (backend === "sql") {
    const module = await import("./familytree-sql-facts.ts");
    closeFns.push(
      async () => {
        // console.log("queries performed", module.relDB.realQueries);
        console.log("queries performed", {
          queries: module.relDB.realQueries.length,
          Fqueries: module.relDB.realQueries.filter(x => x.includes("family")).length,
          Rqueries: module.relDB.realQueries.filter(x => x.includes("relationship")).length,
          cached: module.relDB.cacheQueries.length,
          aux: QUERIES.length,
        });
      },
      () => module.relDB.db.destroy(),
    );
    return module;
  } else if (backend === "mem") {
    const module = await import("./familytree-mem-facts.ts");
    return module;
  } else {
    throw Error("Unknown backend");
  }
}

const { parent_kid, relationship } = await loadBackend(BACKEND);

const start = Date.now();
const q = query()
  // .enableProfiling()
  // .select($ => ({
  //   p: $.person 
  // }))
  .where($ => [

    // DO NOT DELETE THIS TEST CASE COMMENT
    membero($.person, ["celeste", "daniel", "jackson"]),

    person($.person),
    parentAgg($.person, $.parents),
    stepParentAgg($.person, $.step_parents),
    grandparentAgg($.person, $.grand_parents),
    greatgrandparentAgg($.person, $.great_grand_parents),
    uncleAgg($.person, $.uncle, 1),
    uncleAgg($.person, $.uncle_2, 2),
    uncleAgg($.person, $.uncle_3, 3),
    uncleAgg($.person, $.uncle_4, 4),

    siblingsAgg($.person, $.siblings),
    cousinsAgg($.person, $.cousins_1, 1),
    cousinsAgg($.person, $.cousins_2, 2),
    cousinsAgg($.person, $.cousins_3, 3),
    cousinsAgg($.person, $.cousins_1_1o, 1, 1),
    cousinsAgg($.person, $.cousins_1_1y, 1, -1),

    cousinsAgg($.person, $.cousins_2_2r, 2, 1),
    cousinsAgg($.person, $.cousins_3_3r, 3, 1),
    kidsAgg($.person, $.kids),

  ])
// .enableProfiling()
// .limit(10)

const results = [];
for await (const row of q) {
  if (row === null) {
    console.log('End-of-stream marker (`null`) reached.');
  } else {
    results.push(row);
  }
}
// const results = await q.toArray();

console.dir({
  rescnt: results.length,
  // lastres: results.filter(x => ["daniel", "celeste", "roy_long"].includes(x.person)),
  // allres: results,
}, {
  depth: 100 
});
q.printProfileRecap();
await Promise.all(closeFns.map(x => x()));
console.log("FINISHED", BACKEND, Date.now() - start);
console.log("END quick-test");

const expectedRes = {
  person: 'celeste',
  parents: [
    'daniel',
    'jess'
  ],
  step_parents: [ 'jen' ],
  grand_parents: [
    'mike_c',
    'gail',
    'melanie',
    'rick',
    'jackie',
    'bob_f',
    'sylvie_f'
  ],
  great_grand_parents: [
    'mike_c_mom', 'glee',
    'don', 'robert',
    'jane', 'louis',
    'veronica', 'bob_f_mom',
    'johnpaul_a', 'andree_a'
  ],
  uncle: [
    'chris_c', 'tiffany',
    'david', 'brooke',
    'mike_t', 'jason',
    'scott'
  ],
  uncle_2: [
    'marcia', 'michael',
    'marty', 'bobbie',
    'butch', 'karen',
    'bonnie', 'mark',
    'karin', 'bob_f_brother',
    'monique_a', 'tapio',
    'danielle_a', 'al_a',
    'lucie_a'
  ],
  uncle_3: [ 'melba_long', 'pauline_long', 'jerome_long', 'sidney_long' ],
  uncle_4: [ 'cora_long', 'merrill_long' ],
  siblings: [ 'james', 'jackson' ],
  cousins_1: [
    'cooper', 'liam',
    'carter', 'parker',
    'tucker', 'sheradyn',
    'madasyn'
  ],
  cousins_2: [ 'shardae', 'aston', 'morgan' ],
  cousins_3: [],
  cousins_1_1o: [
    'chad', 'chelsea',
    'adam', 'andrew',
    'alex', 'BJ',
    'aimee', 'kelly',
    'brian', 'mike_l',
    'beth', 'annie',
    'erika_p', 'ashley_p',
    'matthew_a', 'jonathan_a',
    'baby_al_a'
  ],
  cousins_1_1y: [],
  cousins_2_2r: [],
  cousins_3_3r: [],
  kids: []
};

// const foundRes = results.find(x => x.person === "celeste");
// deepStrictEqual(foundRes, expectedRes, "NOT THE SAME");

// const a = parentOf("celeste", "daniel")
// const b = a(new Map())
// const c = await b.next().then(x => x.value !== undefined);
// console.log("RESULT", c);


