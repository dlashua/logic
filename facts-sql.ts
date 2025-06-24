// eslint-disable-next-line import/no-named-as-default
import knex from "knex";
import type { Knex as KnexType } from "knex";
import { isVar, unify, walk } from "./core.ts";
import type { Subst, Term } from "./core.ts";
import { toGoal, registerAndOptimizerHook } from "./relations.ts";
import type { Goal } from "./relations.ts";

/**
 * Creates a database-backed relation factory.
 * @param knex_connect_options Knex connection options.
 * @returns An object with a `rel` function to create goals and the knex `db` instance.
 */
export const makeRelDB = (knex_connect_options: KnexType.Config) => {
  const db = knex(knex_connect_options);
  const realQueries: string[] = [];

  /**
   * Creates a goal that represents a query against a database table.
   * @param table The name of the database table.
   * @param paramMapping An object mapping column names to logic terms (variables or values).
   * @returns A logic goal.
   */
  const rel =
    (table: string, primaryKey?: string) =>
      (paramMapping: Record<string, Term>): Goal => {
        const goalFunc = async function* (s: Subst): AsyncGenerator<Subst> {
          const walkedParams: Record<string, Term> = {};
          const whereClauses: { col: string; val: any }[] = [];
          const selectCols: string[] = [];

          // Walk all params to ground them with the current substitution
          // Always select all columns for ungrounded or variable terms
          for (const col in paramMapping) {
            const walkedVal = await walk(paramMapping[col], s);
            walkedParams[col] = walkedVal;
            if (isVar(walkedVal) || typeof walkedVal === "undefined") {
              selectCols.push(col);
            } else {
              whereClauses.push({
                col,
                val: walkedVal
              });
            }
          }

          // Ensure all columns in paramMapping are selected if not grounded
          const allSelectCols = Array.from(new Set([...selectCols, ...Object.keys(paramMapping)]));

          let query;
          if (allSelectCols.length === 0) {
            const anyCol = Object.keys(paramMapping)[0];
            query = db(table).select(anyCol);
          } else {
            query = db(table).select(allSelectCols);
          }
          for (const { col, val } of whereClauses) {
            query = query.where(col, val);
          }

          realQueries.push(query.toString());
          const results = await query;

          for (const row of results) {
            const s_prime = new Map(s);
            // Unify original paramMapping terms (not walked) with row values
            const goalTerms = Object.values(paramMapping);
            const dbValues = Object.keys(paramMapping).map((col) => row[col]);
            const rowSubst = await unify(goalTerms, dbValues, s_prime);
            if (rowSubst) {
              yield rowSubst;
            }
          }
        };
        return toGoal(goalFunc, {
          name: "sql",
          args: [paramMapping],
          table, // Attach db instance for join pushdown
          primaryKey,
          realQueries // Attach realQueries array for join pushdown
        });
      };

  async function registerSqlOptimizer() {
    await registerAndOptimizerHook(async (goals) => {
      console.log(goals.filter(x => x._metadata?.name === "sql"));
      return undefined;
    });
  }

  return {
    db,
    rel,
    realQueries,
    registerSqlOptimizer,
  };
};
