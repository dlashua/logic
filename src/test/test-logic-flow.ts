import { Subst, Term } from "../core/types.ts";
import { walk } from "../core/kernel.ts";
import { eq, and, or } from "../core/combinators.ts";
import { createLogicVarProxy, query } from "../query.ts";
import { membero } from "../relations/lists.ts";
import { not, substLog } from "../relations/control.ts";
import { familytree, info_color, info_number, relDB } from "./familytree-sql-facts.ts";

const {
  parent_kid
} = familytree;

const log = console.log;

const start = Date.now();
const results = await query()
  .where(($) => [

    // info_number($.oneperson, 4),
    // info_color($.oneperson, "blue"),



    // substLog("top"),
    // substLog("first and"),
    // and (

    // membero($.top, [
    //   "robert", "louis"
    // ]),
    substLog("top"),
    parent_kid($.parent, $.person),
    info_number($.person, 4),
    // substLog("after info_number"),

    // substLog("after parent_kid"),
    or(
      info_color($.person, "blue"),
      info_color($.person, "black"),
      info_color($.person, "pink"),
    ),
    // substLog("after or"),


    // ),
    // not(info_number($.kid3, 4)),
    // parent_kid($.kid3, $.kid4),
    // and(
    //   membero($.oneperson, [$.top, $.kid1, $.kid2, $.kid3]),
    //   info_number($.oneperson, 4),
    //   info_color($.oneperson, "blue"),
    // )

    // info_number($.person, 4),
    // // not(info_color($.person, "blue")),
    // parent_kid($.parent, $.person),
    // parent_kid($.gp, $.parent),

  ]).toArray();

const elapsed = Date.now() - start;
console.log("queries", relDB.getQueries());
log("results", results);
log("elapsed time", elapsed);

process.exit(0);