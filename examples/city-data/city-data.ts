import fs from "fs/promises";
import { makeFacts } from "../../src/facts.ts";

const MORE_URI =
  "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/geonames-all-cities-with-a-population-1000/exports/json?lang=en&timezone=America%2FChicago";

const LOCAL_DATA = "./data.json";

export const country_state_city = makeFacts();

export const city = makeFacts(); // ascii_name
export const state = makeFacts(); // admin1_code
export const featclass = makeFacts(); // feature_class
export const featcode = makeFacts(); // feature_code
export const countrycode = makeFacts(); // country_code
export const population = makeFacts(); // population
export const elevation = makeFacts(); // elevation
export const dem = makeFacts(); // dem
export const timezone = makeFacts(); //timezone
export const coords = makeFacts(); // coordinates/lon,lat

export const countrycode_countryname = makeFacts(); // cou_name_en

export async function acquireData() {
  if (
    await fs
      .access(LOCAL_DATA)
      .then(() => true)
      .catch(() => false)
  )
    return;
  console.log("downloading data...");
  const start = Date.now();
  await fetch(MORE_URI)
    .then((res) => res.text())
    .then((text) => fs.writeFile(LOCAL_DATA, text));
  console.log("downloaded!", Date.now() - start);
}

export async function loadData() {
  console.log("loading data...");
  const start = Date.now();
  const rawData = await fs
    .readFile(LOCAL_DATA)
    .then((buffer) => buffer.toString());
  const data = JSON.parse(rawData);
  const countryCodeMap = {} as Record<string, string>;
  for (const row of data) {
    const id = row.geoname_id;

    country_state_city.set(
      id,
      row.country_code,
      row.admin1_code,
      row.ascii_name,
    );

    city.set(id, row.ascii_name);
    state.set(id, row.admin1_code);
    featclass.set(id, row.feature_class);
    featcode.set(id, row.feature_code);
    countrycode.set(id, row.country_code);
    countryCodeMap[row.country_code] = row.cou_name_en;
    population.set(id, row.population);
    elevation.set(id, row.elevation);
    dem.set(id, row.dem);
    timezone.set(id, row.timezone);
    coords.set(id, row.coordinates.lon, row.coordinates.lat);
  }

  for (const [c_code, c_name] of Object.entries(countryCodeMap)) {
    countrycode_countryname.set(c_code, c_name);
  }
  console.log("loaded!", Date.now() - start);
}
