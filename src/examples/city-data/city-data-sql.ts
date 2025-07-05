import { resolve } from "path";
import { fileURLToPath } from 'url';
import { makeRelDB } from "../../facts-sql/index.ts";

const __dirname = fileURLToPath(new URL('.', import.meta.url));

export const relDB = await makeRelDB({
  client: "better-sqlite3",
  connection: {
    filename: resolve(__dirname, "../../../data/city.db"),
  },
  useNullAsDefault: true,
});

export const city = await relDB.rel(
  "city", 
  {
    // primaryKey: "id",
    // selectColumns: ["id", "city", "state", "country_code"],
  }
);

