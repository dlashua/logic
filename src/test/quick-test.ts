import { X } from "vitest/dist/chunks/reporters.d.BFLkQcL6.js";
import {
  and,
  conde,
  eq,
  ifte,
  lift,
  or
} from "../core/combinators.ts"
import { query } from "../query.ts";
import { fail, not, substLog } from "../relations/control.ts";
import { membero } from "../relations/lists.ts";
import { Subst, Term } from "../core/types.ts";
import { SimpleObservable } from "../core/observable.ts";
import { queryUtils } from "../shared/utils.ts";
import { isVar, unify } from "../core/kernel.ts";
import { addConstraint } from "../core/constraint-helpers.ts";

const results = await query()
  .select($ => ({
    x: $.x,
    y: $.y,
    z: $.z 
  }))
  .where($ => [
    addConstraint($.y, 100, $.z),
    addConstraint($.x, 10, $.y),
    membero($.x, [1,2]),
    // membero($.y, [10,20]),
  ]).toArray();

console.log(results);