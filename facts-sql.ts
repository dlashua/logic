import knex from "knex";
import type { Knex as KnexType } from "knex";
import { isVar, unify, walk } from "./core.ts";
import type { Subst, Term } from "./core.ts";
import { toGoal } from "./relations.ts";
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
        (table: string) =>
          (paramMapping: Record<string, Term>): Goal => {
            const goalFunc = async function* (s: Subst): AsyncGenerator<Subst> {
              const walkedParams: Record<string, Term> = {};
              const whereClauses: { col: string; val: any }[] = [];
              const selectCols: string[] = [];

              // Walk all params to ground them with the current substitution
              for (const col in paramMapping) {
                const walkedVal = await walk(paramMapping[col], s);
                walkedParams[col] = walkedVal;
                if (isVar(walkedVal)) {
                  // If the parameter is a variable, select its column
                  selectCols.push(col);
                } else {
                  // If the parameter is a ground value, use it in a WHERE clause
                  whereClauses.push({
                    col,
                    val: walkedVal 
                  });
                }
              }

              // If there are no variables to select, it means we are just checking for existence.
              // We must select at least one column for the query to be valid.
              // The unification at the end will ensure the result matches.
              if (selectCols.length === 0 && Object.keys(paramMapping).length > 0) {
                selectCols.push(Object.keys(paramMapping)[0]);
              }

              // Build and execute the Knex query
              let query = db(table).select(selectCols);
              for (const { col, val } of whereClauses) {
                query = query.where(col, val);
              }

              realQueries.push(query.toString());
              const results = await query;

              // For each resulting row, try to unify it with the goal parameters.
              // This binds the variables in the goal to the values from the database.
              for (const row of results) {
                // IMPORTANT: Clone the substitution for each potential result branch
                const s_prime = new Map(s);

                const goalTerms = selectCols.map((col) => paramMapping[col]);
                const dbValues = selectCols.map((col) => row[col]);

                const rowSubst = await unify(goalTerms, dbValues, s_prime);

                if (rowSubst) {
                  yield rowSubst;
                }
              }
            };

            return toGoal(goalFunc, {
              name: "sql",
              args: [table, paramMapping],
            });
          };

  return {
    db,
    rel,
    realQueries 
  };
};
