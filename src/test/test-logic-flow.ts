import { Subst, Term } from "../core/types.ts";
import { walk } from "../core/kernel.ts";
import { eq, and, or } from "../core/combinators.ts";
import { createLogicVarProxy, query } from "../query.ts";
import { membero } from "../relations/lists.ts";
import { not } from "../relations/control.ts";
import { familytree, info_color, info_number, relDB } from "./familytree-sql-facts.ts";

const {
  parent_kid
} = familytree;

const log = console.log;

await query()
  .where(($) => [

    // parent_kid($.top, $.kid1),
    // parent_kid($.kid1, $.kid2),
    // parent_kid($.kid2, $.kid3),
    // parent_kid($.kid3, $.kid4),
    // and(
    //   membero($.oneperson, [$.top, $.kid1, $.kid2, $.kid3, $.kid4]),
    //   info_number($.oneperson, 4),
    //   info_color($.oneperson, "blue"),
    // )

    info_number($.person, 4),
    // not(info_color($.person, "blue")),
    parent_kid($.parent, $.person),
    parent_kid($.gp, $.parent),

  ]).toArray().then(x => x.forEach(x => log(x)));

console.log("queries", relDB.getQueries());
process.exit(0);