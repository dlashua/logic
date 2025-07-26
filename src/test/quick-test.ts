import { X } from "vitest/dist/chunks/reporters.d.BFLkQcL6.js";
import { and, conde, eq, fresh, ifte, lift, or } from "../core/combinators.ts";
import { isVar, unify } from "../core/kernel.ts";
import { SimpleObservable } from "../core/observable.ts";
import { Subst, Term } from "../core/types.ts";
import { query } from "../query.ts";
import {
	group_by_collecto,
	group_by_counto,
} from "../relations/aggregates-subqueries.ts";
import { fail, not, substLog } from "../relations/control.ts";
import { lengtho, membero } from "../relations/lists.ts";
import { pluso } from "../relations/numeric.ts";
import { queryUtils } from "../shared/utils.ts";

const results = await query()
	.where(($) => [
		substLog("top"),
		membero($.x, [100, 200, 300]),
		group_by_collecto(
			$.z,
			$._y,
			or(
				and(
					pluso($.x, 50, $.z),
					membero($._yt, [1, 2, 3]),
					pluso($.x, $._yt, $._y),
				),
				and(
					pluso($.x, 50, $.z),
					membero($._yt, [101, 102, 103]),
					pluso($.x, $._yt, $._y),
				),
			),
			$.yvals,
		),
		lengtho($.yvals, $.ycount),
	])
	.toArray();

console.log(results);
