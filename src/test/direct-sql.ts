import knex, { Knex } from "knex";
import { resolve } from "path";
import { fileURLToPath } from "url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

export const QUERIES: string[] = [];

let dbInstance: knex.Knex | null = null;

function distinct<T>(arr: T[]): T[] {
	return [...new Set(arr)];
}

// A simple async pipe function to chain promises from left to right.
const pipeAsync =
	(...fns: ((val: any) => Promise<any> | any)[]) =>
	(initialValue: any): Promise<any> =>
		fns.reduce((acc, fn) => acc.then(fn), Promise.resolve(initialValue));

const pluck = (key: string) => (rows: Record<string, any>[]) =>
	rows.map((row) => row[key]);

const flatMapObjectValues = <T>(rows: Record<string, T>[]): T[] =>
	rows.flatMap((row) => Object.values(row));

const without =
	<T>(itemsToRemove: T[]) =>
	(source: T[]): T[] =>
		source.filter((item) => !itemsToRemove.includes(item));

const uncurry = (fn: Function) =>
	function (this: any, ...args: any[]): any {
		const result = args.reduce((f, arg) => f.call(this, arg), fn);
		return result;
	};

const flip =
	<A, B, C>(fn: (a: A) => (b: B) => C) =>
	(b: B) =>
	(a: A) =>
		fn(a)(b);

const spread =
	<T, R>(fn: (...args: T[]) => R) =>
	(args: T[]): R =>
		fn(...args);

const tap = (fn: (x: any) => Promise<any> | any) => async (x: any) => {
	await Promise.resolve(fn(x));
	return x;
};

const tapLog = (message: string) => tap((x) => console.log(message, x));

const fanout =
	(...fns: ((val: any) => any)[]) =>
	<T>(initialValue: T): Promise<T[]> =>
		Promise.all(fns.map((fn) => Promise.resolve(fn(initialValue))));

const identity = (x: any) => Promise.resolve(x);

// Curried function for building and executing a query.
const query =
	(table: string) =>
	(selectColumn: string | Record<string, string>) =>
	(whereColumn: string) =>
	(values: string[]) =>
		getSharedDB()(table).select(selectColumn).whereIn(whereColumn, values);

const queryFamily = query("family");
const queryRelationship = query("relationship");

function getSharedDB() {
	if (!dbInstance) {
		const db = knex({
			client: "better-sqlite3",
			connection: {
				filename: resolve(__dirname, "../../data/family.db"),
			},
			useNullAsDefault: true,
		});

		db.on("query", (queryData) => {
			// Knex doesn't have a built-in method to interpolate bindings on the event
			// So we'll do a simple replacement for logging.
			// This is for debugging and might not be perfectly safe for all edge cases.
			let populatedSql = queryData.sql;
			if (queryData.bindings) {
				queryData.bindings.forEach((binding: any) => {
					populatedSql = populatedSql.replace("?", `'${String(binding)}'`);
				});
			}
			QUERIES.push(populatedSql);
		});

		dbInstance = db;
	}
	return dbInstance;
}

const relationshipABQuery = (p: string[]) =>
	getSharedDB()("relationship")
		.select("a", "b")
		.whereIn("a", p)
		.orWhereIn("b", p);

// Remove all items in the first argument from the result of the async transformation
const applyAndRemove =
	<T>(transform: (x: T[]) => Promise<T[]>) =>
	async (items: T[]) =>
		without(items)(await transform(items));

const getPartners = applyAndRemove(
	pipeAsync(relationshipABQuery, flatMapObjectValues, distinct),
);

const withPartners = pipeAsync(
	fanout(identity, getPartners),
	flatMapObjectValues,
	distinct,
);

const getBioParents = pipeAsync(queryFamily("parent")("kid"), pluck("parent"));

const getBioChildren = pipeAsync(queryFamily("kid")("parent"), pluck("kid"));

const getParentsOf = pipeAsync(getBioParents, withPartners);

const getChildrenOf = pipeAsync(withPartners, getBioChildren, distinct);

const repeat = (count: number) => (fn: (val: any) => Promise<any> | any) =>
	pipeAsync(
		...Array.from(
			{
				length: Math.abs(count),
			},
			() => fn,
		),
	);

const getAncestorsAtGeneration = (generations: number) =>
	repeat(generations)(getParentsOf);
const getDescendantsAtGeneration = (generations: number) =>
	repeat(generations)(getChildrenOf);

const getAncestorsOrDescendantsAtGeneration = (generations: number) =>
	generations === 0
		? identity
		: generations > 0
			? getAncestorsAtGeneration(generations)
			: getDescendantsAtGeneration(Math.abs(generations));

const getAllDescendantsAndSelf = async (
	people: string[],
): Promise<string[]> => {
	if (people.length === 0) {
		return [];
	}
	const children = await getChildrenOf(people);
	if (children.length === 0) {
		return people;
	}
	const allDescendantsOfChildren = await getAllDescendantsAndSelf(children);
	return distinct([...people, ...allDescendantsOfChildren]);
};

const getSiblingsOf = applyAndRemove(
	pipeAsync(
		getAncestorsAtGeneration(1), // Get parents
		getDescendantsAtGeneration(1), // Get children of parents (self + siblings)
	),
);

const findCousinPool = (degree: number, removal: number) =>
	pipeAsync(
		fanout(
			// Get the removal candidates
			pipeAsync(
				getAncestorsAtGeneration(degree + (removal > 0 ? removal : 0)),
				getDescendantsAtGeneration(degree - (removal < 0 ? removal : 0)),
			),
			// Find the cousin candidates
			pipeAsync(
				getAncestorsAtGeneration(degree + (removal > 0 ? removal : 0) + 1),
				getDescendantsAtGeneration(degree - (removal < 0 ? removal : 0) + 1),
			),
		),
		spread(uncurry(without)),
	);

export const getCousinsOf = (
	personName: string,
	degree = 1,
	removal = 0,
): Promise<string[]> => findCousinPool(degree, removal)([personName]);
