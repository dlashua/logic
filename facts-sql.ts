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
    (table: string) =>
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
          args: [table, paramMapping],
          db, // Attach db instance for join pushdown
          realQueries // Attach realQueries array for join pushdown
        });
      };

  // --- SQL Join Optimizer Registration ---
  function canSqlJoin(goals: Goal[]): boolean {
    // Only allow join optimization for runs of atomic SQL goals (not composite/conjunction)
    return goals.length >= 1 &&
      goals.every((g: any) =>
        g._metadata &&
        g._metadata.name === "sql"
      );
  }

  function sql_join_goal(goals: Goal[]): Goal {
    if (goals.length === 1) {
      // For a single SQL goal, just return it directly (no join needed)
      return goals[0];
    }
    return toGoal(
      async function* (s0: Subst) {
        // Instead of running the join once, run it for every input substitution
        // This is the logic engine's standard AND behavior
        const subs: AsyncGenerator<Subst> = (async function* () { yield s0; })();
        for await (const s of subs) {
          const tables = goals.map(g => g._metadata!.args[0]);
          const paramMaps = goals.map(g => g._metadata!.args[1]);
          const aliases = tables.map((t, i) => `t${i}`);
          const varToCol: Record<string, { alias: string, col: string }[]> = {};
          // Map variable ID to the alias/col it should be selected from (first occurrence in each goal)
          const selectCols: { alias: string, col: string, varId: string }[] = [];
          const whereClauses: { alias: string, col: string, val: any }[] = [];
          for (let i = 0; i < paramMaps.length; ++i) {
            const alias = aliases[i];
            const paramMap = paramMaps[i];
            for (const col in paramMap) {
              const term = paramMap[col];
              const walked = await walk(term, s);
              if (isVar(walked)) {
                const id = (walked as any).id;
                // Only select/join if not already grounded in s
                if (!s.has(id)) {
                  selectCols.push({
                    alias,
                    col,
                    varId: id 
                  });
                  if (!varToCol[id]) varToCol[id] = [];
                  varToCol[id].push({
                    alias,
                    col 
                  });
                } else {
                  // If grounded, add WHERE clause for this value
                  whereClauses.push({
                    alias,
                    col,
                    val: s.get(id) 
                  });
                }
              } else {
                whereClauses.push({
                  alias,
                  col,
                  val: walked 
                });
              }
            }
          }
          const db = goals[0]._metadata && (goals[0]._metadata as any).db;
          const realQueries = goals[0]._metadata && (goals[0]._metadata as any).realQueries;
          let query = db({
            [aliases[0]]: tables[0] 
          });
          for (let i = 1; i < tables.length; ++i) {
            query = query.join({
              [aliases[i]]: tables[i] 
            }, function(this: KnexType.QueryBuilder) { // Explicitly typing 'this'
              for (let j = 0; j < i; ++j) {
                for (const id in varToCol) {
                  // Only join if this variable ID appears in both tables
                  const aj = varToCol[id].find(c => c.alias === aliases[j]);
                  const ai = varToCol[id].find(c => c.alias === aliases[i]);
                  if (aj && ai) {
                    // Correcting the 'on' method to use a function as the second argument
                    this.on(
                      `${aliases[j]}.${aj.col}`,
                      function(this: KnexType.QueryBuilder) {
                        this.where(`${aliases[j]}.${aj.col}`, '=', `${aliases[i]}.${ai.col}`);
                      }
                    );
                  }
                }
              }
            });
          }
          for (const w of whereClauses) {
            query = query.where(`${w.alias}.${w.col}`, w.val);
          }
          query = query.select(selectCols.map(sc => `${sc.alias}.${sc.col}`));
          if (realQueries) realQueries.push(query.toString());
          const results = await query;
          for (const row of results) {
            const s_prime = new Map(s);
            // Set each variable ID from the correct column in the result row
            for (const sc of selectCols) {
              const value = row[sc.col] ?? row[`${sc.alias}.${sc.col}`];
              s_prime.set(sc.varId, value);
            }
            yield s_prime;
          }
        }
      },
      {
        name: "sql_join",
        args: goals.map(g => g._metadata)
      }
    );
  }

  function registerSqlJoinOptimizer() {
    registerAndOptimizerHook((goals) => {
      if (canSqlJoin(goals)) {
        // console.log("[SQL JOIN OPTIMIZER] Triggered for goals:", goals.map(g => g._metadata));
        return sql_join_goal(goals);
      }
      return undefined;
    });
  }

  return {
    db,
    rel,
    realQueries,
    registerSqlJoinOptimizer,
  };
};
