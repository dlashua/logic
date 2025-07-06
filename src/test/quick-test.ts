import {
  and,
  conde,
  eq,
  ifte,
  or
} from "../core/combinators.ts"
import { query } from "../query.ts";
import { not } from "../relations/control.ts";
import { membero } from "../relations/lists.ts";

console.log("START quick-test");

process.on('unhandledRejection', (reason) => {
  if (reason === null) {
    console.warn('End-of-stream marker (`null`) received.');
  } else {
    console.error('Unhandled Rejection:', reason);
  }
});


// const BACKEND = "sql";
const BACKEND = "mem";

/**********************************************************/
const closeFns: (() => void)[] = [];
async function loadBackend(backend: string) {
  if (backend === "sql") {
    const module = await import("./familytree-sql-facts.ts");
    closeFns.push(
      async () => {
        const allQueries = module.relDB.getQueries();
        console.log("sql queries", allQueries);
        console.log("queries performed", {
          queries: module.relDB.getQueryCount(),
        });
      },
      () => module.relDB.db.destroy(),
    );
    return module.familytree;
  } else if (backend === "mem") {
    const module = await import("./familytree-mem-facts.ts");
    return module.familytree;
  } else {
    throw Error("Unknown backend");
  }
}

const familytree = await loadBackend(BACKEND);

const start = Date.now();
function makeQuery() {
  return query()
    .where($ => [

      membero($.person, ["louis", "celeste", "daniel", "jason", "brooke", "emerson"]),
      // familytree.parent_kid($.parent, $.person),
      // not(eq($.parent, "daniel")),
      // ifte(
      //   familytree.relationship($.parent, $.stepparent),
      //   eq(1,1),
      //   eq($.stepparent, "none"),
      // )

      familytree.person($.person),
      
      // familytree.parentAgg($.person, $.parents),
      // familytree.stepParentAgg($.person, $.step_parents),
      // familytree.grandparentAgg($.person, $.grand_parents),
      // familytree.greatgrandparentAgg($.person, $.great_grand_parents),
      // familytree.uncleAgg($.person, $.uncle, 1),
  
      // familytree.siblingsAgg($.person, $.siblings),
      // familytree.cousinsAgg($.person, $.cousins_1, 1),
      // familytree.cousinsAgg($.person, $.cousins_2, 2),
      // familytree.cousinsAgg($.person, $.cousins_3, 3),

      // familytree.cousinsAgg($.person, $.cousins_1_1o, 1, 1),
      // familytree.cousinsAgg($.person, $.cousins_1_1y, 1, -1),
  
      // familytree.kidsAgg($.person, $.kids),
  
    ])
}

const q = makeQuery();
const obs$ = q.getSubstObservale();
obs$.subscribe({
  next: (v) => console.dir(v, {
    depth: null 
  })
})

const results = await q.toArray();

console.dir({
  allres: results,
  oneres: results.filter(x => x.person === "celeste"),
  rescnt: results.length,
}, {
  depth: 100 
});
await Promise.all(closeFns.map(x => x()));
console.log("FINISHED", BACKEND, Date.now() - start);
console.log("END quick-test");


process.exit();