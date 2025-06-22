import { Goal, Subst, isVar, unify, walk, Term } from "./logic_lib.ts";
import knex, { Knex } from "knex";

export const makeRelDB = async (knex_connect_options: Knex.Config) => {
  const db = knex(knex_connect_options);
    interface Pattern {
        table: string;
        params: Record<string, any>;
    }
    const patterns: Pattern[] = [];
    const run = async function* (s: any) {
      async function* runPatterns(idx: number, subst: Subst): AsyncGenerator<Subst> {
        if (idx >= patterns.length) {
          yield subst;
          return;
        }
        const q = patterns[idx];
        const selectCols: string[] = [];
        const whereClauses: { col: string; val: any }[] = [];
        for (const col in q.params) {
          const v = await walk(q.params[col], subst);
          if (isVar(v)) {
            selectCols.push(col);
          } else {
            whereClauses.push({
              col,
              val: v, 
            });
          }
        }
        if (!selectCols.length) {
          // console.log("Q", "NONE", q);
          yield* runPatterns(idx + 1, subst);
          return;
        }
        let k = db(q.table).select(selectCols);
        for (const { col, val } of whereClauses) {
          k = k.where(col, '=', val);
        }
        console.log("Q", k.toString());
        const rows = await k;
        for (const row of rows) {
          console.log("R", JSON.stringify(row));
          let s2: Subst = new Map(subst as Subst);
          let ok = false;
          for (const col of selectCols) {
            const origVal = q.params[col];
            const walked = await walk(origVal, s2);
            const unified = await unify(walked, row[col], s2);
            if (unified) {
              s2 = unified;
              ok = true;
            }
          }
          if (ok) {
            yield* runPatterns(idx + 1, s2);
          }
        }
        // patterns.splice(idx, 1);
      }
      yield* runPatterns(0, s);
      // patterns.splice(0, patterns.length);
    };
    const makeRel = async (table: string, primaryKey = "id") => {
      return function goal(queryObj: Record<string, Term<any>>): Goal {
        return async function* (s: Subst) {
          // If all values are grounded, do nothing
          let allGrounded = true;
          for (const col in queryObj) {
            if (isVar(queryObj[col])) {
              allGrounded = false;
              break;
            }
          }
          if (allGrounded) return;

          // Build where clause from grounded columns
          const where: Record<string, any> = {};
          const outputVars: string[] = [];
          for (const col in queryObj) {
            const v = await walk(queryObj[col], s);
            if (isVar(v)) {
              outputVars.push(col);
            } else {
              where[col] = v;
            }
          }
          // If there are no output variables, do nothing
          if (outputVars.length === 0) return;
          // Generalized: support multiple output variables
          const rows = await db(table).select(outputVars).where(where);
          for (const row of rows) {
            const s2 = new Map(s);
            for (const col of outputVars) {
              s2.set(queryObj[col].id, row[col]);
            }
            yield s2;
          }
        };
      };
    };

    return {
      makeRel,
      run,
      db, 
    };
}