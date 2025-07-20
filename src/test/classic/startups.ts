
import {
  and,
  or,
  eq,
  fresh,
  conde,
  ifte,
  lift
} from "../../core/combinators.ts";
import { query } from "../../query.ts";
import {
  fail,
  failo,
  neqo,
  not,
  substLog,
  thruCount
} from "../../relations/control.ts";
import { lengtho, membero } from "../../relations/lists.ts";
import { Term } from "../../core/types.ts";
import { queryUtils } from "../../shared/utils.ts";
import { isVar, lvar, unify } from "../../core/kernel.ts";
import {
  gteo,
  gto,
  lto,
  minuso,
  pluso
} from "../../relations/numeric.ts";
import { group_by_collecto, group_by_counto } from "../../relations/aggregates-subqueries.ts";
import { collectThenGo, make } from "./logic-helpers.ts";

const startups = lvar("startups");

const keyVals = {
  name: [
    "atriana_com",
    "gofro_com", 
    "protecha_com",
    "zetafish_com"
  ],
  investment: [1000000, 2000000, 3000000, 4000000],
  founders: [
    "addie_abrams",
    "betty_becker", 
    "fred_frost",
    "pat_padilla"
  ]
};

const { constrainArrays, enforceConsistency, link, unlink } = make(startups, keyVals, 4);

function exists($: any, key: string, items: any[]) {
  return and(
    ...items.map((item: any) => link($, { [key]: item }))
  );
}

const start = Date.now();

const results = await query()
  .select($ => ({ all: startups }))
  .where($ => [
    constrainArrays($),
    thruCount("constrainArrays"),

    collectThenGo(),

    // 3. Protecha.com received $1,000,000.
    link($, { name: "protecha_com", investment: 1000000 }),
    thruCount("constraint 3"),

    collectThenGo(),

    // 1. The startup that received the $2,000,000 investment was started by Fred Frost.
    link($, { investment: 2000000, founders: "fred_frost" }),
    thruCount("constraint 1"),

    collectThenGo(),

    // 4. Atriana.com received 1 million dollars more than the company started by Betty Becker.
    pluso($.founders_betty_becker_investment, 1000000, $.name_atriana_com_investment),
    link($, { founders: "betty_becker", investment: $.founders_betty_becker_investment }),
    link($, { name: "atriana_com", investment: $.name_atriana_com_investment }),
    // Atriana.com was not started by Betty Becker (they are separate entities)
    unlink($, { name: "atriana_com" }, { founders: "betty_becker" }),
    // Since Atriana.com received MORE than Betty's company, Atriana.com can't have the lowest investment
    unlink($, { name: "atriana_com" }, { investment: 1000000 }),
    // Since Betty's company received LESS than Atriana.com, Betty's company can't have the highest investment
    unlink($, { founders: "betty_becker" }, { investment: 4000000 }),
    thruCount("constraint 4"),

    collectThenGo(),

    // 2. The startup started by Addie Abrams received 2 million dollars less than Gofro.com.
    pluso($.founders_addie_abrams_investment, 2000000, $.name_gofro_com_investment),
    link($, { founders: "addie_abrams", investment: $.founders_addie_abrams_investment }),
    link($, { name: "gofro_com", investment: $.name_gofro_com_investment }),
    // Gofro.com was not started by Addie Abrams (they are separate entities)
    unlink($, { name: "gofro_com" }, { founders: "addie_abrams" }),
    // Since Gofro.com received MORE than Addie's company, Gofro.com can't have the lowest investment
    unlink($, { name: "gofro_com" }, { investment: 1000000 }),
    // Since Addie's company received LESS than Gofro.com, Addie's company can't have the highest investment
    unlink($, { founders: "addie_abrams" }, { investment: 4000000 }),
    thruCount("constraint 2"),

    collectThenGo(),

    // pluso($.founders_betty_becker_investment, 1000000, $.name_atriana_com_investment),
    // pluso($.founders_addie_abrams_investment, 2000000, $.name_gofro_com_investment),


    enforceConsistency($),
    thruCount("OUT"),
  ]).toArray();

console.dir(results, { depth: null });
console.log("count", results.length, "elapsed", Date.now() - start);

// 1. The startup that received the $2,000,000 investment was started by Fred Frost.
// 2. The startup started by Addie Abrams received 2 million dollars less than Gofro.com.
// 3. Protecha.com received $1,000,000.
// 4. Atriana.com received 1 million dollars more than the company started by Betty Becker.