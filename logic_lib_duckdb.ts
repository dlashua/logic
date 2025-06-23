import { DuckDBInstance } from "@duckdb/node-api";
import {
  type Goal,
  isVar,
  type Subst,
  type Term,
  unify,
  walk,
} from "./logic_lib.ts";

export function makeFactsDuckDB(
  table: string,
  columns: string[],
  dbPath: string,
) {
  return (...query: Term[]) => {
    return async function* (s: Subst) {
      const where: string[] = [];
      const params: Record<string, any> = {};
      query.forEach((q, i) => {
        const v = walk(q, s);
        if (!isVar(v)) {
          if (v === null) {
            where.push(`${columns[i]} IS NULL`);
          } else {
            where.push(`${columns[i]} = $${columns[i]}`);
            params[columns[i]] = v;
          }
        }
      });
      const db = await DuckDBInstance.create(dbPath);
      const conn = await db.connect();

      const sql =
        `SELECT ${columns.join(", ")} FROM ${table}` +
        (where.length ? ` WHERE ${where.join(" AND ")}` : "");
      // console.log(sql, params);

      const reader = await conn.runAndReadAll(sql, params);
      const rows = reader.getRowObjectsJS();

      for (const row of rows) {
        const fact = columns.map((col) =>
          row[col] === undefined ? null : row[col],
        );
        const s1 = await unify(query, fact, s);
        if (s1) yield s1;
      }
    };
  };
}

export function makeFactsObjDuckDB(
  table: string,
  keys: string[],
  dbPath: string,
) {
  function goalFn(queryObj: Record<string, Term>): Goal {
    return async function* (s: Subst) {
      const where: string[] = [];
      const params: Record<string, any> = {};
      const qKeys = Object.keys(queryObj);
      for (const k of keys) {
        const v = walk(queryObj[k], s);
        if (v !== undefined && !isVar(v)) {
          if (v === null) {
            where.push(`${k} IS NULL`);
          } else {
            where.push(`${k} = $${k}`);
            params[k] = v;
          }
        }
      }
      const db = await DuckDBInstance.create(dbPath);
      const conn = await db.connect();
      const sql =
        `SELECT ${qKeys.join(", ")} FROM ${table}` +
        (where.length ? ` WHERE ${where.join(" AND ")}` : "");
      // console.log(sql, params);
      const reader = await conn.runAndReadAll(sql, params);
      const rows = reader.getRowObjectsJS();
      for (const row of rows) {
        // console.log(row);
        const fact: Record<string, Term> = {};
        for (const k of qKeys) {
          fact[k] = row[k] === undefined ? null : row[k];
        }
        const s1 = await unify(
          keys.map((k) => queryObj[k]),
          keys.map((k) => fact[k]),
          s,
        );
        if (s1) yield s1;
      }
    };
  }
  function wrapper(queryObj: Record<string, Term>): Goal {
    return goalFn(queryObj);
  }
  wrapper.keys = keys;
  wrapper.table = table;
  wrapper.dbPath = dbPath;
  return wrapper;
}
