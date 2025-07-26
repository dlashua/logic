import { log } from "console";
import { nextTick } from "process";
import {
	GOAL_GROUP_ALL_GOALS,
	GOAL_GROUP_CONJ_GOALS,
	GOAL_GROUP_ID,
	GOAL_GROUP_PATH,
	isVar,
	unify,
	walk,
} from "../core/kernel.ts";
import { SimpleObservable } from "../core/observable.ts";
import type { Goal, Observable, Subst, Term } from "../core/types.ts";
import { getDefaultLogger, type Logger } from "../shared/logger.ts";
import { queryUtils } from "../shared/utils.ts";
import type { DBManager, GoalRecord } from "./index.ts";
import type { RelationOptions } from "./types.ts";

/** DEBUGGING GOALS */
const DEBUG_GOALS = [1];

const ROW_CACHE = Symbol.for("sql-row-cache");

// WeakMap to link observables to their goal IDs
const observableToGoalId = new WeakMap<Observable<any>, number>();

// Global registry to track goals by group ID
const goalsByGroupId = new Map<number, Set<number>>();

// Removed global query cache - using improved grouping mechanism instead

// Adjustable batch size for IN queries
const BATCH_SIZE = 100;
// Adjustable debounce window for batching (ms)
const BATCH_DEBOUNCE_MS = 50;

// Removed global cache utilities - using improved grouping mechanism instead

// --- Observable/Goal Registration Utilities ---

/**
 * Register a goalId in a group.
 */
function registerGoalInGroup(
	goalsByGroupId: Map<number, Set<number>>,
	groupId: number,
	goalId: number,
): void {
	if (!goalsByGroupId.has(groupId)) {
		goalsByGroupId.set(groupId, new Set());
	}
	goalsByGroupId.get(groupId)!.add(goalId);
}

// --- Batching & Debounce Utilities ---

/**
 * Create a batch processor for streaming input, with batch size and debounce window.
 * Calls flushFn(batch) when batch is full or debounce window elapses.
 * Returns a handler for input and a cancel function.
 */
function createBatchProcessor<T>(options: {
	batchSize: number;
	debounceMs: number;
	onFlush: (batch: T[]) => Promise<void> | void;
}): {
	addItem: (item: T) => void;
	complete: () => Promise<void>;
	cancel: () => void;
} {
	let batch: T[] = [];
	let debounceTimer: NodeJS.Timeout | null = null;
	let flushingPromise: Promise<void> | null = null;
	let cancelled = false;

	const clearDebounce = (): void => {
		if (debounceTimer) {
			clearTimeout(debounceTimer);
			debounceTimer = null;
		}
	};

	const flushBatch = async (): Promise<void> => {
		clearDebounce();
		if (flushingPromise) return flushingPromise;
		if (batch.length === 0 || cancelled) return Promise.resolve();
		const toFlush = batch;
		batch = [];
		flushingPromise = Promise.resolve(options.onFlush(toFlush)).finally(() => {
			flushingPromise = null;
		});
		return flushingPromise;
	};

	const addItem = (item: T): void => {
		if (cancelled) return;
		batch.push(item);
		if (shouldFlushBatch(batch, options.batchSize)) {
			flushBatch();
		} else {
			clearDebounce();
			debounceTimer = setTimeout(() => flushBatch(), options.debounceMs);
		}
	};

	const complete = async (): Promise<void> => {
		await flushBatch();
	};

	const cancel = (): void => {
		cancelled = true;
		clearDebounce();
		batch = [];
	};

	return {
		addItem,
		complete,
		cancel,
	};
}

// --- Helper for batch flush condition (task #7) ---
function shouldFlushBatch<T>(batch: T[], batchSize: number): boolean {
	return batch.length >= batchSize;
}

// --- Cache Management Utilities ---

/**
 * Get or create the ROW_CACHE map from a substitution.
 */
function getOrCreateRowCache(
	s: Map<string | symbol, any>,
): Map<number, Record<string, any>[]> {
	if (!s.has(ROW_CACHE)) {
		s.set(ROW_CACHE, new Map());
	}
	return s.get(ROW_CACHE) as Map<number, Record<string, any>[]>;
}

/**
 * Get and remove the cache for a goalId from the substitution's ROW_CACHE.
 */
function getCacheForGoalId(
	goalId: number,
	s: Subst,
): Record<string, any>[] | null {
	const cache = getOrCreateRowCache(s);
	if (cache.has(goalId)) {
		const rows = cache.get(goalId) as Record<string, any>[];
		return rows;
	}
	return null;
}

/**
 * Create a new substitution with cache for other compatible goals in the same batch.
 */
function createUpdatedSubstWithCacheForOtherGoals(
	dbObj: DBManager,
	s: Subst,
	myGoalId: number,
	currentRow: any,
	logger: Logger,
): Subst {
	const newSubst = new Map(s);
	const originalCache = getOrCreateRowCache(s);
	const newCache = new Map(originalCache);
	newCache.delete(myGoalId);
	const myGoal = dbObj.getGoalById(myGoalId);
	if (myGoal) {
		const compatibleGoals = dbObj
			.getGoals()
			.filter(
				(g) =>
					g.goalId !== myGoalId &&
					g.batchKey === myGoal.batchKey &&
					g.batchKey !== undefined &&
					g.table === myGoal.table,
			);
		for (const otherGoal of compatibleGoals) {
			newCache.set(otherGoal.goalId, [currentRow]);
			logger.log("ADDED_CACHE_FOR_OTHER_GOAL", {
				myGoalId,
				otherGoalId: otherGoal.goalId,
				rowCount: 1,
				cachedRow: currentRow,
			});
		}
	}
	newSubst.set(ROW_CACHE, newCache);
	return newSubst;
}

// --- Goal Compatibility & Merging Utilities ---

/**
 * Check if one goal could benefit from cached data of another goal.
 * For caching, we can be more permissive than merging - goals can benefit
 * from each other's cached results as long as they share some variable mappings.
 */
function couldBenefitFromCache(
	myGoal: GoalRecord,
	otherGoal: GoalRecord,
	subst: Subst,
): string {
	if (myGoal.table !== otherGoal.table) {
		return "wrong_table";
	}

	const myColumns = Object.keys(myGoal.queryObj);
	const otherColumns = Object.keys(otherGoal.queryObj);

	let matches = 0;

	for (const column of myColumns) {
		if (otherColumns.includes(column)) {
			// Walk the value in the current substitution to see if it's grounded
			const myValueRaw = myGoal.queryObj[column];
			const otherValueRaw = otherGoal.queryObj[column];
			const myValue = walk(myValueRaw, subst);
			const otherValue = walk(otherValueRaw, subst);

			if (!isVar(myValue)) {
				// myGoal has a ground value, otherGoal must match exactly
				if (!isVar(otherValue)) {
					if (myValue === otherValue) {
						matches++;
					} else {
						return "value_not_match";
					}
				} else {
					return "term_to_var";
				}
			} else {
				// myGoal has a variable, otherGoal just needs a variable in this column
				if (isVar(otherValue)) {
					matches++;
				} else {
					return "var_to_term";
				}
			}
		}
	}

	if (matches > 0) {
		return "match";
	}
	return "no_matches";
}

/**
 * Check if two goals can have their queries merged.
 * Requires identical variable-to-column mappings for safe merging.
 */
function canMergeQueries(goalA: GoalRecord, goalB: GoalRecord): boolean {
	const aColumns = Object.keys(goalA.queryObj);
	const bColumns = Object.keys(goalB.queryObj);

	// Check if they have the same query structure (same columns)
	if (aColumns.length !== bColumns.length) {
		return false;
	}

	if (!aColumns.every((col) => bColumns.includes(col))) {
		return false;
	}

	// Check that the same variables are mapped to the same columns
	for (const column of aColumns) {
		const aValue = goalA.queryObj[column];
		const bValue = goalB.queryObj[column];

		// Both must be variables with the same ID, or both must be the same literal value
		if (isVar(aValue) && isVar(bValue)) {
			if (aValue.id !== bValue.id) {
				return false;
			}
		} else if (isVar(aValue) || isVar(bValue)) {
			// One is a variable, the other is not - they can't be merged
			return false;
		} else {
			// Both are literal values - they must be equal
			if (aValue !== bValue) {
				return false;
			}
		}
	}

	return true;
}

/**
 * Collect all WHERE clause values from a set of goals for merging.
 * Only collect grounded values that are constants in the original goal definitions,
 * not variables that happen to be bound in the current substitution.
 */
async function collectAllWhereClauses(
	goals: GoalRecord[],
	s: Subst,
): Promise<Record<string, Set<any>>> {
	const allWhereClauses: Record<string, Set<any>> = {};
	for (const goal of goals) {
		// Only collect WHERE clauses from values that are already grounded constants
		// in the original goal definition, not from bound variables
		const whereCols = queryUtils.onlyGrounded(goal.queryObj);
		for (const [col, value] of Object.entries(whereCols)) {
			if (!allWhereClauses[col]) allWhereClauses[col] = new Set();
			allWhereClauses[col].add(value);
		}
	}
	return allWhereClauses;
}

// --- Query Building Utilities ---

/**
 * Build a select query for a table with given where clauses and columns.
 * whereClauses: { col: Set<any> } (values in set will be used with whereIn if >1, else where)
 * selectColumns: array of column names to select (never '*')
 */
function buildSelectQuery(
	dbObj: DBManager,
	table: string,
	whereClauses: Record<string, Set<any>>,
	selectColumns: string[],
) {
	let query = dbObj.db(table);
	for (const [col, values] of Object.entries(whereClauses)) {
		if (values.size === 1) {
			query = query.where(col, Array.from(values)[0]);
		} else {
			query = query.whereIn(col, Array.from(values));
		}
	}
	query = query.select(selectColumns);
	return query;
}

export class RegularRelationWithMerger {
	private logger: Logger;
	private primaryKey?: string;

	constructor(
		private dbObj: DBManager,
		private table: string,
		logger?: Logger,
		private options?: RelationOptions,
	) {
		this.logger = logger ?? getDefaultLogger();
		this.primaryKey = options?.primaryKey;
	}

	/**
	 * Find goals that are compatible for query merging.
	 * These goals must have identical variable-to-column mappings.
	 */
	private findMergeCompatibleGoals(
		myGoal: GoalRecord,
		relatedGoals: { goal: GoalRecord; matchingIds: string[] }[],
	): GoalRecord[] {
		// Filter to same table goals that can be safely merged
		const compatibleGoals: GoalRecord[] = [];
		for (const { goal } of relatedGoals) {
			if (goal.table === myGoal.table && canMergeQueries(myGoal, goal)) {
				compatibleGoals.push(goal);
			}
		}

		this.logger.log("MERGE_COMPATIBILITY_CHECK", {
			myGoalId: myGoal.goalId,
			candidateGoals: relatedGoals.map((g) => ({
				goalId: g.goal.goalId,
				queryObj: g.goal.queryObj,
			})),
			mergeCompatibleGoalIds: compatibleGoals.map((g) => g.goalId),
			table: this.table,
		});

		return compatibleGoals;
	}

	/**
	 * Find goals that are compatible for result caching.
	 * These goals can benefit from our query results even if not merged.
	 */
	private findCacheCompatibleGoals(
		myGoal: GoalRecord,
		relatedGoals: { goal: GoalRecord; matchingIds: string[] }[],
		subst: Subst,
	): GoalRecord[] {
		const cacheBeneficiaryGoals: GoalRecord[] = [];
		const candidateGoalsWithCompatibility = [];

		for (const { goal } of relatedGoals) {
			const isCompatible = couldBenefitFromCache(myGoal, goal, subst);

			candidateGoalsWithCompatibility.push({
				goalId: goal.goalId,
				queryObj: goal.queryObj,
				cacheCompatible: isCompatible,
			});

			if (isCompatible === "match") {
				cacheBeneficiaryGoals.push(goal);
			}
		}

		this.logger.log("CACHE_COMPATIBILITY_CHECK", {
			myGoalId: myGoal.goalId,
			myGoalQueryObj: myGoal.queryObj,
			candidateGoals: candidateGoalsWithCompatibility,
			cacheCompatibleGoalIds: cacheBeneficiaryGoals.map((g) => g.goalId),
			table: this.table,
		});

		return cacheBeneficiaryGoals;
	}

	/**
	 * Collect WHERE clauses from substitutions for batching.
	 */
	private async collectWhereClausesFromSubstitutions(
		queryObj: Record<string, Term>,
		substitutions: Subst[],
	): Promise<Record<string, Set<any>>> {
		const whereClauses: Record<string, Set<any>> = {};
		for (const subst of substitutions) {
			const walked = await queryUtils.walkAllKeys(queryObj, subst);
			const whereCols = queryUtils.onlyGrounded(walked);
			for (const [col, value] of Object.entries(whereCols)) {
				if (!whereClauses[col]) whereClauses[col] = new Set();
				whereClauses[col].add(value);
			}
		}
		return whereClauses;
	}

	/**
	 * Collect all columns needed for query from goals.
	 */
	private collectColumnsFromGoals(
		myQueryObj: Record<string, Term>,
		cacheCompatibleGoals: GoalRecord[],
		mergeCompatibleGoals?: GoalRecord[],
	): { columns: string[]; additionalColumns: string[] } {
		const allGoalColumns = new Set<string>();

		// Add columns from current goal
		Object.keys(myQueryObj).forEach((col) => allGoalColumns.add(col));

		// Add columns from merge-compatible goals (if any)
		if (mergeCompatibleGoals) {
			for (const goal of mergeCompatibleGoals) {
				Object.keys(goal.queryObj).forEach((col) => allGoalColumns.add(col));
			}
		}

		// Add columns from cache-compatible goals
		for (const cacheGoal of cacheCompatibleGoals) {
			Object.keys(cacheGoal.queryObj).forEach((col) => allGoalColumns.add(col));
		}

		const additionalColumns = this.options?.selectColumns || [];
		const columns = [...new Set([...allGoalColumns, ...additionalColumns])];
		return {
			columns,
			additionalColumns,
		};
	}

	/**
	 * Execute a query and handle logging.
	 */
	private async executeQueryWithLogging(
		whereClauses: Record<string, Set<any>>,
		columns: string[],
		goalId: number,
		substitutions: Subst[],
		mergeCompatibleGoals?: GoalRecord[],
		cacheCompatibleGoals?: GoalRecord[],
	): Promise<Record<string, any>[]> {
		const query = buildSelectQuery(
			this.dbObj,
			this.table,
			whereClauses,
			columns,
		);
		const sqlString = query.toString();

		// Create G/M/C prefix: G:current M:merge C:cache
		const mergeIds = mergeCompatibleGoals?.map((g) => g.goalId).join(",") || "";
		const cacheIds = cacheCompatibleGoals?.map((g) => g.goalId).join(",") || "";
		const prefix = `G:${goalId}${mergeIds ? ` M:${mergeIds}` : ""}${cacheIds ? ` C:${cacheIds}` : ""}`;

		this.dbObj.addQuery(`${prefix} - ${sqlString}`);

		// Choose appropriate log message based on whether this is a merge or single query
		const logType = mergeCompatibleGoals?.length
			? "DB_QUERY_MERGED"
			: "DB_QUERY_BATCH";
		const logData: any = {
			table: this.table,
			sql: sqlString,
			goalId,
			substitutionCount: substitutions.length,
		};

		if (mergeCompatibleGoals?.length) {
			logData.mergedGoalIds = [
				goalId,
				...mergeCompatibleGoals.map((g) => g.goalId),
			];
		}

		if (cacheCompatibleGoals?.length) {
			logData.cacheCompatibleGoalIds = cacheCompatibleGoals.map(
				(g) => g.goalId,
			);
		}

		this.logger.log(logType, logData);

		const rows = await query;
		if (rows.length) {
			const rowsLogType = mergeCompatibleGoals?.length
				? "DB_ROWS_MERGED"
				: "DB_ROWS";
			this.logger.log(rowsLogType, {
				table: this.table,
				sql: sqlString,
				goalId,
				rows,
			});
		}

		return rows;
	}

	/**
	 * Build and execute a query for the given goals and substitutions.
	 * Handles both merged queries (multiple goals) and single goal queries.
	 */
	private async buildAndExecuteQuery(
		goalId: number,
		queryObj: Record<string, Term>,
		substitutions: Subst[],
		mergeCompatibleGoals: GoalRecord[],
		cacheCompatibleGoals: GoalRecord[],
	): Promise<Record<string, any>[]> {
		const myGoal = this.dbObj.getGoalById(goalId);
		if (!myGoal) return [];

		if (mergeCompatibleGoals.length > 0) {
			// Execute merged query with compatible goals
			return this.executeMergedQuery(
				myGoal,
				queryObj,
				substitutions,
				mergeCompatibleGoals,
				cacheCompatibleGoals,
			);
		} else {
			// Execute single goal query
			return this.executeSingleGoalQuery(
				goalId,
				queryObj,
				substitutions,
				cacheCompatibleGoals,
			);
		}
	}

	/**
	 * Execute a merged query that combines multiple compatible goals.
	 */
	private async executeMergedQuery(
		myGoal: GoalRecord,
		queryObj: Record<string, Term>,
		substitutions: Subst[],
		compatibleGoals: GoalRecord[],
		cacheCompatibleGoals: GoalRecord[],
	): Promise<Record<string, any>[]> {
		const allGoalsToMerge = [myGoal, ...compatibleGoals];

		// Collect WHERE clauses from compatible goals
		const representativeSubst = substitutions[0];
		const goalWhereClauses = await collectAllWhereClauses(
			allGoalsToMerge,
			representativeSubst,
		);

		// Also collect WHERE clauses from all substitutions (for batching)
		const substWhereClauses = await this.collectWhereClausesFromSubstitutions(
			queryObj,
			substitutions,
		);

		// Merge both sets of WHERE clauses
		const mergedWhereClauses: Record<string, Set<any>> = {
			...goalWhereClauses,
		};
		for (const [col, values] of Object.entries(substWhereClauses)) {
			if (mergedWhereClauses[col]) {
				for (const value of values) {
					mergedWhereClauses[col].add(value);
				}
			} else {
				mergedWhereClauses[col] = new Set(values);
			}
		}

		// Collect all columns needed by all goals (merge + cache compatible)
		const { columns: allColumns, additionalColumns } =
			this.collectColumnsFromGoals(
				queryObj,
				cacheCompatibleGoals,
				compatibleGoals,
			);

		this.logger.log("MERGED_QUERY_COLUMN_SELECTION", {
			goalId: myGoal.goalId,
			mergeGoalIds: allGoalsToMerge.map((g) => g.goalId),
			mergeGoalColumns: allGoalsToMerge.flatMap((g) => Object.keys(g.queryObj)),
			cacheCompatibleGoalIds: cacheCompatibleGoals.map((g) => g.goalId),
			cacheCompatibleColumns: cacheCompatibleGoals.flatMap((g) =>
				Object.keys(g.queryObj),
			),
			additionalColumns,
			finalSelectedColumns: allColumns,
			table: this.table,
		});

		// Execute merged query
		return await this.executeQueryWithLogging(
			mergedWhereClauses,
			allColumns,
			myGoal.goalId,
			substitutions,
			compatibleGoals,
			cacheCompatibleGoals,
		);
	}

	/**
	 * Execute a query for a single goal (no merging).
	 */
	private async executeSingleGoalQuery(
		goalId: number,
		queryObj: Record<string, Term>,
		substitutions: Subst[],
		cacheCompatibleGoals: GoalRecord[],
	): Promise<Record<string, any>[]> {
		// Collect WHERE clauses from substitutions
		const allWhereClauses = await this.collectWhereClausesFromSubstitutions(
			queryObj,
			substitutions,
		);

		// Collect columns from current goal and cache-compatible goals
		const { columns: allColumns, additionalColumns } =
			this.collectColumnsFromGoals(queryObj, cacheCompatibleGoals);

		this.logger.log("SINGLE_QUERY_COLUMN_SELECTION", {
			goalId,
			myGoalColumns: Object.keys(queryObj),
			cacheCompatibleGoalIds: cacheCompatibleGoals.map((g) => g.goalId),
			cacheCompatibleColumns: cacheCompatibleGoals.flatMap((g) =>
				Object.keys(g.queryObj),
			),
			additionalColumns,
			finalSelectedColumns: allColumns,
			table: this.table,
		});

		return await this.executeQueryWithLogging(
			allWhereClauses,
			allColumns,
			goalId,
			substitutions,
			undefined, // no merge goals
			cacheCompatibleGoals,
		);
	}

	/**
	 * Helper to format the sql-row-cache for logging
	 */
	private formatRowCacheForLog(rowCache: unknown): Record<number, any> {
		const result: Record<number, any> = {};
		if (!(rowCache instanceof Map)) return result;
		for (const [goalId, rows] of rowCache.entries()) {
			if (Array.isArray(rows)) {
				if (rows.length <= 5) {
					result[goalId] = rows;
				} else {
					result[goalId] = {
						count: rows.length,
					};
				}
			}
		}
		return result;
	}

	/**
	 * Process cached rows by filtering them to match grounded terms before unification.
	 */
	private async processCachedRows(
		goalId: number,
		queryObj: Record<string, Term>,
		cachedRows: Record<string, any>[],
		subst: Subst,
		observer: any,
	): Promise<void> {
		// Filter cached rows to only those that match grounded terms in the current substitution
		const filteredRows = cachedRows.filter((row) => {
			for (const [col, term] of Object.entries(queryObj)) {
				const grounded = walk(term, subst);
				if (!isVar(grounded) && row[col] !== grounded) {
					return false;
				}
			}
			return true;
		});

		this.logger.log("DB_NO_ROWS", () => {
			const logSubst = new Map(subst);
			if (logSubst.has(ROW_CACHE)) {
				logSubst.set(
					ROW_CACHE,
					this.formatRowCacheForLog(logSubst.get(ROW_CACHE)),
				);
			}
			return {
				goalId,
				queryObj,
				wasFromCache: true,
				updatedSubst: logSubst,
			};
		});

		for (const row of filteredRows) {
			const unifiedSubst = this.unifyRowWithQuery(
				row,
				queryObj,
				new Map(subst),
			);
			if (unifiedSubst) {
				// Preserve cache entries for other goals when processing cache hits
				const originalCache = getOrCreateRowCache(subst);
				const preservedCache = new Map(originalCache);
				preservedCache.delete(goalId); // Only remove our own cache
				unifiedSubst.set(ROW_CACHE, preservedCache);

				// Format the substitution for logging
				this.logger.log("UNIFY_SUCCESS", () => {
					const logSubst = new Map(unifiedSubst);
					if (logSubst.has(ROW_CACHE)) {
						logSubst.set(
							ROW_CACHE,
							this.formatRowCacheForLog(logSubst.get(ROW_CACHE)),
						);
					}
					return {
						goalId,
						queryObj,
						row,
						wasFromCache: true,
						unifiedSubst: logSubst,
					};
				});
				observer.next(new Map(unifiedSubst));
				await new Promise((resolve) => nextTick(resolve));
			} else {
				this.logger.log("UNIFY_FAILURE", () => {
					const logSubst = new Map(unifiedSubst);
					if (logSubst.has(ROW_CACHE)) {
						logSubst.set(
							ROW_CACHE,
							this.formatRowCacheForLog(logSubst.get(ROW_CACHE)),
						);
					}
					return {
						goalId,
						queryObj,
						row,
						wasFromCache: true,
						unifiedSubst: logSubst,
					};
				});
			}
		}
	}

	/**
	 * Process fresh query rows by unifying them with the query and emitting results.
	 */
	private async processFreshRows(
		goalId: number,
		queryObj: Record<string, Term>,
		rows: Record<string, any>[],
		substitutions: Subst[],
		observer: any,
		cacheCompatibleGoals: GoalRecord[],
	): Promise<void> {
		for (const subst of substitutions) {
			// Remove any existing cache entry for the current goalId before populating
			const cache = getOrCreateRowCache(subst);
			cache.delete(goalId);
			if (rows.length === 0) {
				this.logger.log("DB_NO_ROWS", () => {
					const logSubst = new Map(subst);
					if (logSubst.has(ROW_CACHE)) {
						logSubst.set(
							ROW_CACHE,
							this.formatRowCacheForLog(logSubst.get(ROW_CACHE)),
						);
					}
					return {
						goalId,
						queryObj,
						wasFromCache: false,
						updatedSubst: logSubst,
					};
				});
			}
			for (const row of rows) {
				const unifiedSubst = this.unifyRowWithQuery(
					row,
					queryObj,
					new Map(subst),
				);
				if (unifiedSubst) {
					const passed = getOrCreateRowCache(unifiedSubst);
					// For each cache-compatible goal, cache ALL rows (no filtering)
					for (const otherGoal of cacheCompatibleGoals) {
						if (otherGoal.goalId === goalId) continue; // Don't cache for our own goal
						passed.set(otherGoal.goalId, rows);
						this.logger.log("CACHED_FOR_OTHER_GOAL", {
							myGoalId: goalId,
							otherGoalId: otherGoal.goalId,
							rowCount: rows.length,
							reason: "cache-beneficiary",
							otherGoalQueryObj: otherGoal.queryObj,
							availableColumns: rows.length > 0 ? Object.keys(rows[0]) : [],
						});
					}

					this.logger.log("UNIFY_SUCCESS", () => {
						const logSubst = new Map(unifiedSubst);
						if (logSubst.has(ROW_CACHE)) {
							logSubst.set(
								ROW_CACHE,
								this.formatRowCacheForLog(logSubst.get(ROW_CACHE)),
							);
						}
						return {
							goalId,
							queryObj,
							row,
							wasFromCache: false,
							updatedSubst: logSubst,
						};
					});
					observer.next(new Map(unifiedSubst));
					await new Promise((resolve) => nextTick(resolve));
				} else {
					this.logger.log("UNIFY_FAILURE", () => {
						const logSubst = new Map(unifiedSubst);
						if (logSubst.has(ROW_CACHE)) {
							logSubst.set(
								ROW_CACHE,
								this.formatRowCacheForLog(logSubst.get(ROW_CACHE)),
							);
						}
						return {
							goalId,
							queryObj,
							row,
							wasFromCache: false,
							updatedSubst: logSubst,
						};
					});
				}
			}
		}
	}

	haveAtLeastOneMatchingVar(a: GoalRecord, b: GoalRecord) {
		const aVarIds = Object.values(queryUtils.onlyVars(a.queryObj)).map(
			(x) => x.id,
		);
		const bVarIds = Object.values(queryUtils.onlyVars(b.queryObj)).map(
			(x) => x.id,
		);
		const matchingIds = aVarIds.filter((av) => bVarIds.includes(av));

		if (matchingIds.length === 0) {
			return null;
		}
		return {
			goal: b,
			matchingIds,
		};
	}

	async findRelatedGoals(myGoal: GoalRecord, s: Subst) {
		// Get both inner and outer group goals from the substitution
		const innerGroupGoals = (s.get(GOAL_GROUP_CONJ_GOALS) as Goal[]) || [];
		const outerGroupGoals = (s.get(GOAL_GROUP_ALL_GOALS) as Goal[]) || [];

		// For query merging, use inner group goals (same logical group)
		// For caching, use outer group goals (cross-branch sharing)
		const goalsForCaching =
			outerGroupGoals.length > 0 ? outerGroupGoals : innerGroupGoals;

		if (goalsForCaching.length === 0) {
			return [];
		}

		// Look up goal IDs for each goal function using the WeakMap
		const otherGoalIds = goalsForCaching
			.map((goalFn) =>
				observableToGoalId.get(goalFn as unknown as Observable<any>),
			)
			.filter(
				(goalId) => goalId !== undefined && goalId !== myGoal.goalId,
			) as number[];

		// Get the goal records
		const otherGoals = otherGoalIds
			.map((goalId) => this.dbObj.getGoalById(goalId))
			.filter((goal) => goal !== undefined) as GoalRecord[];

		this.logger.log("FOUND_RELATED_GOALS", {
			myGoalId: myGoal.goalId,
			myGoalQueryObj: myGoal.queryObj,
			innerGroupGoalsCount: innerGroupGoals.length,
			outerGroupGoalsCount: outerGroupGoals.length,
			usingOuterGroupForCaching: outerGroupGoals.length > 0,
			foundOtherGoalIds: otherGoalIds,
			relatedGoals: otherGoals.map((g) => ({
				goalId: g.goalId,
				table: g.table,
				queryObj: g.queryObj,
			})),
		});

		// Return all goals with empty matchingIds - let compatibility checking handle filtering
		return otherGoals.map((goal) => ({
			goal,
			matchingIds: [], // Empty since compatibility filtering happens in calling functions
		}));
	}

	createGoal(queryObj: Record<string, Term>): Goal {
		const goalId = this.dbObj.getNextGoalId();
		this.logger.log("GOAL_CREATED", {
			goalId,
			table: this.table,
			queryObj,
		});
		// Register goal immediately when the goal function is called, before any processing
		this.dbObj.addGoal(goalId, this.table, queryObj, undefined);
		this.logger.log("GOAL_REGISTERED_EARLY", {
			goalId,
			table: this.table,
			queryObj,
		});
		// Streaming protocol: always accept Observable<Subst> as input
		const mySubstHandler = (input$: any) => {
			const resultObservable = new SimpleObservable<Subst>((observer) => {
				let cancelled = false;
				let batchIndex = 0;
				let input_complete = false;
				let batchKeyUpdated = false;
				this.logger.log("GOAL_STARTED", {
					goalId,
					table: this.table,
					queryObj,
				});
				// Use the batch processor utility
				const batchProcessor = createBatchProcessor<Subst>({
					batchSize: BATCH_SIZE,
					debounceMs: BATCH_DEBOUNCE_MS,
					onFlush: async (batch) => {
						if (cancelled) return;
						this.logger.log("FLUSH_BATCH", {
							goalId,
							batchIndex,
							batchSize: batch.length,
						});

						// All substitutions in this batch are cache misses
						// (cache hits are processed immediately above)

						this.logger.log("PROCESSING_CACHE_MISSES", {
							goalId,
							cacheMissCount: batch.length,
						});

						// Use extracted function to process fresh query results
						const rows = await this.executeQueryForSubstitutions(
							goalId,
							queryObj,
							batch,
						);
						// Find cache-compatible goals for this batch
						const representativeSubst = batch[0];
						const myGoal = this.dbObj.getGoalById(goalId);
						let cacheCompatibleGoals: GoalRecord[] = [];
						if (myGoal && representativeSubst) {
							const relatedGoals = await this.findRelatedGoals(
								myGoal,
								representativeSubst,
							);
							cacheCompatibleGoals = this.findCacheCompatibleGoals(
								myGoal,
								relatedGoals,
								representativeSubst,
							);
						}
						await this.processFreshRows(
							goalId,
							queryObj,
							rows,
							batch,
							observer,
							cacheCompatibleGoals,
						);

						batchIndex++;
						this.logger.log("FLUSH_BATCH_COMPLETE", {
							goalId,
							batchIndex,
							batchSize: 0,
						});
					},
				});
				const subscription = input$.subscribe({
					next: async (subst: Subst) => {
						this.logger.log("GOAL_NEXT", {
							goalId,
							batchIndex,
							input_complete,
						});
						if (cancelled) return;
						if (!batchKeyUpdated) {
							const groupId = subst.get(GOAL_GROUP_ID) as number | undefined;
							batchKeyUpdated = true;
							if (groupId !== undefined) {
								registerGoalInGroup(goalsByGroupId, groupId, goalId);
								this.logger.log("GOAL_GROUP_INFO", {
									goalId,
									table: this.table,
									groupId,
									registeredInGroup: groupId !== undefined,
									queryObj,
								});
							}
						}

						// Check cache first - if hit, process immediately without batching
						const cache = getCacheForGoalId(goalId, subst);
						if (cache) {
							this.logger.log("CACHE_HIT_IMMEDIATE", {
								goalId,
								rowCount: cache.length,
								table: this.table,
							});

							// Use extracted function to process cached results immediately
							await this.processCachedRows(
								goalId,
								queryObj,
								cache,
								subst,
								observer,
							);
						} else {
							// Cache miss - add to batch for SQL processing
							batchProcessor.addItem(subst);
							this.logger.log("CACHE_MISS_TO_BATCH", {
								goalId,
								input_complete,
								subst,
							});
						}
					},
					error: (err: any) => {
						if (!cancelled) observer.error?.(err);
					},
					complete: () => {
						this.logger.log("UPSTREAM_GOAL_COMPLETE", {
							goalId,
							batchIndex,
							input_complete,
							cancelled,
						});
						input_complete = true;
						batchProcessor.complete().then(() => {
							this.logger.log("GOAL_COMPLETE", {
								goalId,
								batchIndex,
								input_complete,
								cancelled,
							});
							observer.complete?.();
						});
					},
				});
				return () => {
					this.logger.log("GOAL_CANCELLED", {
						goalId,
						batchIndex,
						input_complete,
						cancelled,
					});
					cancelled = true;
					batchProcessor.cancel();
					subscription.unsubscribe?.();
				};
			});
			return resultObservable;
		};
		// Inline registerGoalHandler here since it's only used once
		const betterFnName = `SQL_${this.table}_${goalId}`;
		mySubstHandler.displayName = betterFnName;
		// Register the observable with its goalId
		observableToGoalId.set(
			mySubstHandler as unknown as Observable<any>,
			goalId,
		);
		return mySubstHandler;
	}

	private async executeQueryForSubstitutions(
		goalId: number,
		queryObj: Record<string, Term>,
		substitutions: Subst[],
	): Promise<any[]> {
		if (substitutions.length === 0) return [];

		this.logger.log("EXECUTING_UNIFIED_QUERY", {
			goalId,
			substitutionCount: substitutions.length,
			table: this.table,
		});

		const myGoal = this.dbObj.getGoalById(goalId);
		if (!myGoal) return [];

		const representativeSubst = substitutions[0];

		// Step 1: Find all related goals (do this once)
		const relatedGoals = await this.findRelatedGoals(
			myGoal,
			representativeSubst,
		);

		// Step 2: Find merge-compatible goals
		const mergeCompatibleGoals = this.findMergeCompatibleGoals(
			myGoal,
			relatedGoals,
		);

		// Step 3: Find cache-compatible goals
		const cacheCompatibleGoals = this.findCacheCompatibleGoals(
			myGoal,
			relatedGoals,
			representativeSubst,
		);

		// Step 4: Build and execute query (merged or single)
		const rows = await this.buildAndExecuteQuery(
			goalId,
			queryObj,
			substitutions,
			mergeCompatibleGoals,
			cacheCompatibleGoals,
		);

		return rows;
	}

	// Make unifyRowWithQuery public so it can be used by batch processor
	public unifyRowWithQuery(
		row: any,
		queryObj: Record<string, Term>,
		s: Subst,
	): Subst | null {
		let result = s;
		for (const [column, term] of Object.entries(queryObj)) {
			const value = row[column];
			if (value === undefined) continue;
			const unified = unify(term, value, result);
			if (unified === null) {
				return null;
			}
			result = unified;
		}
		return result;
	}
}
