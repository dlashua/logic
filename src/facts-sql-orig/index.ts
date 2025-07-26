import type { Knex } from "knex";
import knex from "knex";
import { or } from "../core/combinators.ts";
import { isVar } from "../core/kernel.ts";
import type { Goal, Term } from "../core/types.ts";
import { getDefaultLogger, type Logger } from "../shared/logger.ts";
import type { BaseConfig as Configuration } from "../shared/types.ts";
import { RegularRelationWithMerger } from "./relation.ts";
import type { RelationOptions } from "./types.ts";

export interface GoalRecord {
	goalId: number;
	table: string;
	queryObj: Record<string, Term>;
	batchKey?: string;
}

export type DBManager = Awaited<ReturnType<typeof createDBManager>>;
export async function createDBManager(
	knex_connect_options: Knex.Config,
	logger: Logger,
	options?: RelationOptions,
) {
	const db = knex(knex_connect_options);
	const queries: string[] = [];
	const goals: GoalRecord[] = [];
	const processedGoals: {
		goalId: number;
		table: string;
		goalIds: number[];
		selectCol: Record<string, any>;
		whereCol: Record<string, any>;
		rows: Record<string, any>[];
	}[] = [];

	let nextGoalId = 0;

	return {
		db,
		getNextGoalId: () => ++nextGoalId,

		addQuery: (q: string) => queries.push(q),
		getQueries: () => queries,
		clearQueries: () => queries.splice(0, queries.length),
		getQueryCount: () => queries.length,

		addGoal: (
			goalId: number,
			table: string,
			queryObj: Record<string, Term>,
			batchKey?: string,
		) =>
			goals.push({
				goalId,
				table,
				queryObj,
				batchKey,
			}),
		getGoalById: (id: number) => goals.find((x) => x.goalId === id),
		getGoalsByBatchKey: (batchKey: string) =>
			goals.filter((x) => x.batchKey === batchKey),
		getGoals: () => goals,
		clearGoals: () => goals.splice(0, goals.length),

		addProcessedGoal: (q: any) => processedGoals.push(q),
		findProcessedGoalsByOwner: (id: number) =>
			processedGoals.filter((x) => x.goalId === id),
		findProcessedGoalsByMember: (id: number) =>
			processedGoals.filter((x) => x.goalIds.includes(id)),
	};
}

export const makeRelDB = async (
	knex_connect_options: Knex.Config,
	options?: Record<string, string>,
	configOverrides?: Partial<Configuration>,
) => {
	options ??= {};

	const logger = getDefaultLogger();
	const dbManager = await createDBManager(
		knex_connect_options,
		logger,
		options,
	);

	function createRelation(table: string, options?: RelationOptions) {
		const relation = new RegularRelationWithMerger(
			dbManager,
			table,
			logger,
			options,
		);

		return (queryObj: Record<string, Term>): Goal => {
			return relation.createGoal(queryObj);
		};
	}

	function logic_createSymmetricRelation(
		table: string,
		keys: [string, string],
		options?: RelationOptions,
	) {
		const relation = new RegularRelationWithMerger(
			dbManager,
			table,
			logger,
			options,
		);

		return (queryObj: Record<string, Term>): Goal => {
			const queryObjSwapped = {
				[keys[0]]: queryObj[keys[1]],
				[keys[1]]: queryObj[keys[0]],
			};
			return or(
				relation.createGoal(queryObj),
				relation.createGoal(queryObjSwapped),
			);
		};
	}

	return {
		rel: createRelation,
		relSym: logic_createSymmetricRelation,
		db: dbManager.db,
		getQueries: () => dbManager.getQueries(),
		clearQueries: () => dbManager.clearQueries(),
		getQueryCount: () => dbManager.getQueryCount(),
	};
};
