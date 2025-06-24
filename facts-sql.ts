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
          for (const col in paramMapping) {
            const walkedVal = await walk(paramMapping[col], s);
            walkedParams[col] = walkedVal;
            if (isVar(walkedVal)) {
              selectCols.push(col); // ungrounded: select
            } else {
              whereClauses.push({
                col,
                val: walkedVal 
              }); // grounded: where
            }
          }

          // If all terms are grounded, just select one column (arbitrary) to check for existence
          let query;
          if (selectCols.length === 0) {
            const anyCol = Object.keys(paramMapping)[0];
            query = db(table).select(anyCol);
          } else {
            query = db(table).select(selectCols);
          }
          for (const { col, val } of whereClauses) {
            query = query.where(col, val);
          }

          realQueries.push(query.toString());
          const results = await query;

          for (const row of results) {
            const s_prime = new Map(s);
            // Only unify ungrounded terms (selectCols)
            const goalTerms = selectCols.map((col) => paramMapping[col]);
            const dbValues = selectCols.map((col) => row[col]);
            let rowSubst;
            if (selectCols.length === 0) {
            // All grounded: just check for existence
              rowSubst = s_prime;
            } else {
              rowSubst = await unify(goalTerms, dbValues, s_prime);
            }
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
    if (goals.length < 2) return false;
    return goals.every(g => g._metadata && g._metadata.name === "sql");
  }

  function sql_join_goal(goals: Goal[]): Goal {
    return toGoal(
      async function* (s: Subst) {
        const tables = goals.map(g => g._metadata!.args[0]);
        const paramMaps = goals.map(g => g._metadata!.args[1]);
        const aliases = tables.map((t, i) => `t${i}`);
        const varToCol: Record<string, { alias: string, col: string }[]> = {};
        const selectCols: { alias: string, col: string }[] = [];
        const whereClauses: { alias: string, col: string, val: any }[] = [];
        for (let i = 0; i < paramMaps.length; ++i) {
          const alias = aliases[i];
          const paramMap = paramMaps[i];
          for (const col in paramMap) {
            const term = paramMap[col];
            const walked = await walk(term, s);
            if (isVar(walked)) {
              selectCols.push({
                alias,
                col 
              });
              const id = (walked as any).id;
              if (!varToCol[id]) varToCol[id] = [];
              varToCol[id].push({
                alias,
                col 
              });
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
          let joinOn = null;
          for (const id in varToCol) {
            const cols = varToCol[id];
            if (cols.length > 1) {
              const t0 = cols.find(c => c.alias === aliases[0]);
              const ti = cols.find(c => c.alias === aliases[i]);
              if (t0 && ti) {
                joinOn = {
                  t0,
                  ti 
                };
                break;
              }
            }
          }
          if (joinOn) {
            query = query.join(
              {
                [aliases[i]]: tables[i] 
              },
              `${aliases[0]}.${joinOn.t0.col}`,
              '=',
              `${aliases[i]}.${joinOn.ti.col}`
            );
          } else {
            query = query.join({
              [aliases[i]]: tables[i] 
            });
          }
        }
        for (const w of whereClauses) {
          query = query.where(`${w.alias}.${w.col}`, w.val);
        }
        query = query.select(selectCols.map(sc => `${sc.alias}.${sc.col}`));
        if (realQueries) realQueries.push(query.toString());
        const results = await query;
        for (const row of results) {
          const s_prime = new Map(s);
          for (const id in varToCol) {
            const cols = varToCol[id];
            for (const { alias, col } of cols) {
              const value = row[`${alias}.${col}`] ?? row[col];
              if (isVar(paramMaps[aliases.indexOf(alias)][col])) {
                s_prime.set(id, value);
              }
            }
          }
          yield s_prime;
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
