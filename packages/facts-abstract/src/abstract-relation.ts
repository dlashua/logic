import type { Goal, Observable, Subst, Term } from "logic";
import {
	GOAL_GROUP_ALL_GOALS,
	GOAL_GROUP_CONJ_GOALS,
	getDefaultLogger,
	isVar,
	type Logger,
	SimpleObservable,
	walk,
} from "logic";
import {
	buildWhereConditions,
	canMergeQueries,
	collectAllWhereClauses,
	collectColumnsFromGoals,
	collectWhereClausesFromSubstitutions,
	couldBenefitFromCache,
	unifyRowWithQuery,
} from "./abstract-relation-helpers.js";
import { DefaultCacheManager } from "./cache-manager.js";
import type {
	AbstractRelationConfig,
	CacheManager,
	DataRow,
	DataStore,
	GoalManager,
	GoalRecord,
	QueryParams,
	RelationOptions,
} from "./types.js";

// WeakMap to link observables to their goal IDs
const observableToGoalId = new WeakMap<Observable<any>, number>();

// Default batch configuration
const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_DEBOUNCE_MS = 50;

/**
 * Abstract relation engine that handles batching, caching, and query optimization
 * Works with any DataStore implementation
 */
// Make AbstractRelation generic over options type
export class AbstractRelation<
	TOptions extends RelationOptions = RelationOptions,
> {
	private logger: Logger;
	private cacheManager: CacheManager;
	private config: Required<AbstractRelationConfig>;

	constructor(
		private dataStore: DataStore,
		private goalManager: GoalManager,
		private relationIdentifier: string,
		logger?: Logger,
		private _options?: TOptions,
		config?: AbstractRelationConfig,
	) {
		this.logger = logger ?? getDefaultLogger();
		this.cacheManager = config?.cacheManager ?? new DefaultCacheManager();

		// Set default config
		this.config = {
			batchSize: config?.batchSize ?? DEFAULT_BATCH_SIZE,
			debounceMs: config?.debounceMs ?? DEFAULT_DEBOUNCE_MS,
			enableCaching: config?.enableCaching ?? true,
			enableQueryMerging: config?.enableQueryMerging ?? true,
			cacheManager: this.cacheManager,
		};
	}

	/**
	 * Create a goal for this relation
	 */
	createGoal(queryObj: Record<string, Term>): Goal {
		const goalId = this.goalManager.getNextGoalId();

		this.logger.log("GOAL_CREATED", {
			goalId,
			relationIdentifier: this.relationIdentifier,
			queryObj,
			dataStore: this.dataStore.type,
		});

		// Register goal
		this.goalManager.addGoal(
			goalId,
			this.relationIdentifier,
			queryObj,
			undefined,
			this._options,
		);

		// Create the goal function
		const goalFunction = (input$: Observable<Subst>) => {
			return new SimpleObservable<Subst>((observer) => {
				let cancelled = false;
				let batchIndex = 0;
				let inputComplete = false;

				this.logger.log("GOAL_STARTED", {
					goalId,
					relationIdentifier: this.relationIdentifier,
					queryObj,
					dataStore: this.dataStore.type,
				});

				// Create batch processor
				const batchProcessor = this.createBatchProcessor({
					batchSize: this.config.batchSize,
					debounceMs: this.config.debounceMs,
					onFlush: async (batch) => {
						if (cancelled) return;

						this.logger.log("FLUSH_BATCH", {
							goalId,
							batchIndex,
							batchSize: batch.length,
							dataStore: this.dataStore.type,
						});

						// Process batch
						const rows = await this.executeQueryForSubstitutions(
							goalId,
							queryObj,
							batch as Subst[],
						);

						// Find cache-compatible goals
						const representativeSubst = batch[0];
						const myGoal = this.goalManager.getGoalById(goalId);
						let cacheCompatibleGoals: GoalRecord[] = [];

						if (myGoal && representativeSubst) {
							const relatedGoals = await this.findRelatedGoals(
								myGoal,
								representativeSubst as Subst,
							);
							cacheCompatibleGoals = this.findCacheCompatibleGoals(
								myGoal,
								relatedGoals,
								representativeSubst as Subst,
							);
						}

						await this.processFreshRows(
							goalId,
							queryObj,
							rows,
							batch as Subst[],
							observer,
							cacheCompatibleGoals,
						);

						batchIndex++;
					},
				});

				// Subscribe to input stream
				let active = 0;
				let completed = false;
				const subscription = input$.subscribe({
					next: async (subst: Subst) => {
						if (cancelled) return;
						active++;

						this.logger.log("GOAL_NEXT", {
							goalId,
							batchIndex,
							inputComplete,
							dataStore: this.dataStore.type,
						});

						// Check cache first if caching is enabled
						if (this.config.enableCaching) {
							const cachedRows = this.cacheManager.get(goalId, subst);
							if (cachedRows) {
								this.logger.log("CACHE_HIT_IMMEDIATE", {
									goalId,
									rowCount: cachedRows.length,
									relationIdentifier: this.relationIdentifier,
									dataStore: this.dataStore.type,
								});

								await this.processCachedRows(
									goalId,
									queryObj,
									cachedRows,
									subst,
									observer,
								);
								active--;
								if (completed && active === 0) observer.complete?.();

								return; // Don't add to batch if we had a cache hit
							}
						}

						// Cache miss - add to batch
						batchProcessor.addItem(subst);
						this.logger.log("CACHE_MISS_TO_BATCH", {
							goalId,
							inputComplete,
							dataStore: this.dataStore.type,
						});
						active--;
						if (completed && active === 0) observer.complete?.();
					},
					error: (err: any) => {
						if (!cancelled) observer.error?.(err);
					},
					complete: () => {
						this.logger.log("UPSTREAM_GOAL_COMPLETE", {
							goalId,
							batchIndex,
							inputComplete,
							cancelled,
							dataStore: this.dataStore.type,
						});

						inputComplete = true;
						batchProcessor
							.complete()
							.then(() => {
								this.logger.log("GOAL_COMPLETE", {
									goalId,
									batchIndex,
									inputComplete,
									cancelled,
									dataStore: this.dataStore.type,
								});

								completed = true;
								if (completed && active === 0) observer.complete?.();
							})
							.catch((e) => {
								console.error(e);
								// Silently handle completion errors to prevent unhandled rejections
								completed = true;
								if (completed && active === 0) observer.complete?.();
							});
					},
				});

				return () => {
					this.logger.log("GOAL_CANCELLED", {
						goalId,
						batchIndex,
						inputComplete,
						cancelled,
						dataStore: this.dataStore.type,
					});
					cancelled = true;
					batchProcessor.cancel();
					subscription.unsubscribe?.();
				};
			});
		};

		// Set up goal metadata
		const displayName = `${this.dataStore.type.toUpperCase()}_${this.relationIdentifier}_${goalId}`;
		goalFunction.displayName = displayName;
		observableToGoalId.set(goalFunction as unknown as Observable<any>, goalId);

		return goalFunction;
	}

	/**
	 * Execute query for a set of substitutions
	 */
	private async executeQueryForSubstitutions(
		goalId: number,
		queryObj: Record<string, Term>,
		substitutions: Subst[],
	): Promise<DataRow[]> {
		if (substitutions.length === 0) return [];

		this.logger.log("EXECUTING_UNIFIED_QUERY", {
			goalId,
			substitutionCount: substitutions.length,
			relationIdentifier: this.relationIdentifier,
			dataStore: this.dataStore.type,
		});

		const myGoal = this.goalManager.getGoalById(goalId);
		if (!myGoal) return [];

		const representativeSubst = substitutions[0];

		// Find related goals for merging and caching
		const relatedGoals = await this.findRelatedGoals(
			myGoal,
			representativeSubst,
		);
		const mergeCompatibleGoals = this.config.enableQueryMerging
			? this.findMergeCompatibleGoals(myGoal, relatedGoals)
			: [];
		const cacheCompatibleGoals = this.config.enableCaching
			? this.findCacheCompatibleGoals(myGoal, relatedGoals, representativeSubst)
			: [];

		// Build and execute query
		return await this.buildAndExecuteQuery(
			goalId,
			queryObj,
			substitutions,
			mergeCompatibleGoals,
			cacheCompatibleGoals,
		);
	}

	/**
	 * Build query parameters and execute via data store
	 */
	private async buildAndExecuteQuery(
		goalId: number,
		queryObj: Record<string, Term>,
		substitutions: Subst[],
		mergeCompatibleGoals: GoalRecord[],
		cacheCompatibleGoals: GoalRecord[],
	): Promise<DataRow[]> {
		// Collect WHERE clauses from substitutions
		const whereClauses = await this.collectWhereClausesFromSubstitutions(
			queryObj,
			substitutions,
		);

		// If we have merge-compatible goals, include their WHERE clauses too
		if (mergeCompatibleGoals.length > 0) {
			const myGoal = this.goalManager.getGoalById(goalId);
			if (myGoal) {
				const allGoalsToMerge = [myGoal, ...mergeCompatibleGoals];
				const goalWhereClauses = await this.collectAllWhereClauses(
					allGoalsToMerge,
					substitutions[0],
				);

				// Merge goal WHERE clauses with substitution WHERE clauses
				for (const [col, values] of Object.entries(goalWhereClauses)) {
					if (whereClauses[col]) {
						for (const value of values) {
							whereClauses[col].add(value);
						}
					} else {
						whereClauses[col] = new Set(values);
					}
				}
			}
		}

		// Collect columns from all relevant goals
		const columns = this.collectColumnsFromGoals(
			queryObj,
			cacheCompatibleGoals,
			mergeCompatibleGoals,
		);

		// Convert to data store format
		const whereConditions = this.buildWhereConditions(whereClauses);

		const mergeCompatibleGoalIds = mergeCompatibleGoals
			.map((x) => x.goalId)
			.join(",");
		const cacheCompatibleGoalIds = cacheCompatibleGoals
			.map((x) => x.goalId)
			.join(",");
		const iffmt = (v, fn) => (v ? fn(v) : "");
		const annotatedLogQuery = (queryString: string) =>
			this.goalManager.addQuery(
				`G:${goalId}${iffmt(mergeCompatibleGoalIds, (v) => ` M:${v}`)}${iffmt(cacheCompatibleGoalIds, (v) => ` C:${v}`)} - ${queryString}`,
			);

		const queryParams: QueryParams = {
			relationIdentifier: this.relationIdentifier,
			selectColumns: columns.columns,
			whereConditions,
			relationOptions: this._options,
			goalId,
			logQuery: annotatedLogQuery,
		};

		// Execute via data store (it will handle query logging)
		const rows = await this.dataStore.executeQuery(queryParams);

		this.logger.log("DB_QUERY_EXECUTED", {
			goalId,
			relationIdentifier: this.relationIdentifier,
			rowCount: rows.length,
			queryParams,
			dataStore: this.dataStore.type,
		});

		return rows;
	}

	/**
	 * Find related goals for merging and caching
	 */
	private async findRelatedGoals(
		myGoal: GoalRecord,
		s: Subst,
	): Promise<{ goal: GoalRecord; matchingIds: string[] }[]> {
		// Get goal groups from substitution (matches current SQL implementation)
		const innerGroupGoals = (s.get(GOAL_GROUP_CONJ_GOALS) as Goal[]) || [];
		const outerGroupGoals = (s.get(GOAL_GROUP_ALL_GOALS) as Goal[]) || [];

		// const goalsForCaching = outerGroupGoals.length > 0 ? outerGroupGoals : innerGroupGoals;
		const goalsForCaching = outerGroupGoals;

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
			.map((goalId) => this.goalManager.getGoalById(goalId))
			.filter((goal) => goal !== undefined) as GoalRecord[];

		this.logger.log("FOUND_RELATED_GOALS", {
			myGoalId: myGoal.goalId,
			myGoalQueryObj: myGoal.queryObj,
			foundOtherGoalIds: otherGoalIds,
			relatedGoals: otherGoals.map((g) => ({
				goalId: g.goalId,
				relationIdentifier: g.relationIdentifier,
				queryObj: g.queryObj,
			})),
			dataStore: this.dataStore.type,
		});

		return otherGoals.map((goal) => ({
			goal,
			matchingIds: [], // Empty for now - could implement variable matching logic
		}));
	}

	/**
	 * Find goals that are compatible for query merging
	 */
	private findMergeCompatibleGoals(
		myGoal: GoalRecord,
		relatedGoals: { goal: GoalRecord; matchingIds: string[] }[],
	): GoalRecord[] {
		const compatibleGoals: GoalRecord[] = [];
		for (const { goal } of relatedGoals) {
			if (
				goal.relationIdentifier === myGoal.relationIdentifier &&
				this.canMergeQueries(myGoal, goal)
			) {
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
			relationIdentifier: this.relationIdentifier,
			dataStore: this.dataStore.type,
		});

		return compatibleGoals;
	}

	/**
	 * Find goals that are compatible for result caching
	 */
	private findCacheCompatibleGoals(
		myGoal: GoalRecord,
		relatedGoals: { goal: GoalRecord; matchingIds: string[] }[],
		subst: Subst,
	): GoalRecord[] {
		const cacheBeneficiaryGoals: GoalRecord[] = [];
		const candidateGoalsWithCompatibility = [];

		for (const { goal } of relatedGoals) {
			const isCompatible = this.couldBenefitFromCache(myGoal, goal, subst);

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
			relationIdentifier: this.relationIdentifier,
			dataStore: this.dataStore.type,
		});

		return cacheBeneficiaryGoals;
	}

	/**
	 * Process cached rows
	 */
	private async processCachedRows(
		goalId: number,
		queryObj: Record<string, Term>,
		cachedRows: DataRow[],
		subst: Subst,
		observer: any,
	): Promise<void> {
		// Filter cached rows to match current substitution
		const filteredRows = cachedRows.filter((row) => {
			for (const [col, term] of Object.entries(queryObj)) {
				const grounded = walk(term, subst);
				if (!isVar(grounded) && row[col] !== grounded) {
					return false;
				}
			}
			return true;
		});

		this.logger.log("CACHE_ROWS_PROCESSED", {
			goalId,
			originalCount: cachedRows.length,
			filteredCount: filteredRows.length,
			relationIdentifier: this.relationIdentifier,
			dataStore: this.dataStore.type,
		});

		for (const row of filteredRows) {
			const unifiedSubst = this.unifyRowWithQuery(
				row,
				queryObj,
				new Map(subst),
			);
			if (unifiedSubst) {
				observer.next(unifiedSubst);
			}
			await new Promise((resolve) => setTimeout(() => resolve(undefined), 0));
		}
	}

	/**
	 * Process fresh query rows
	 */
	private async processFreshRows(
		goalId: number,
		queryObj: Record<string, Term>,
		rows: DataRow[],
		substitutions: Subst[],
		observer: any,
		cacheCompatibleGoals: GoalRecord[],
	): Promise<void> {
		for (const subst of substitutions) {
			// Clear any existing cache for this goal
			if (this.config.enableCaching) {
				this.cacheManager.clear(goalId);
			}

			if (rows.length === 0) {
				this.logger.log("DB_NO_ROWS", {
					goalId,
					queryObj,
					wasFromCache: false,
					relationIdentifier: this.relationIdentifier,
					dataStore: this.dataStore.type,
				});
				continue;
			}

			for (const row of rows) {
				const unifiedSubst = this.unifyRowWithQuery(
					row,
					queryObj,
					new Map(subst),
				);
				if (unifiedSubst) {
					// Cache rows for compatible goals
					if (this.config.enableCaching) {
						for (const otherGoal of cacheCompatibleGoals) {
							if (otherGoal.goalId !== goalId) {
								this.cacheManager.set(otherGoal.goalId, unifiedSubst, rows, {
									fromGoalId: goalId,
								});
								this.logger.log("CACHED_FOR_OTHER_GOAL", {
									myGoalId: goalId,
									otherGoalId: otherGoal.goalId,
									rowCount: rows.length,
									dataStore: this.dataStore.type,
								});
							}
						}
					}

					this.logger.log("UNIFY_SUCCESS", {
						goalId,
						queryObj,
						row,
						wasFromCache: false,
						relationIdentifier: this.relationIdentifier,
						dataStore: this.dataStore.type,
					});

					observer.next(unifiedSubst);
					await new Promise((resolve) => setTimeout(resolve, 0));
				} else {
					this.logger.log("UNIFY_FAILURE", {
						goalId,
						queryObj,
						row,
						wasFromCache: false,
						relationIdentifier: this.relationIdentifier,
						dataStore: this.dataStore.type,
					});
				}
			}
		}
	}

	// Helper method delegates
	private couldBenefitFromCache = couldBenefitFromCache;
	private canMergeQueries = canMergeQueries;
	private collectWhereClausesFromSubstitutions =
		collectWhereClausesFromSubstitutions;
	private collectAllWhereClauses = collectAllWhereClauses;
	private collectColumnsFromGoals = collectColumnsFromGoals;
	private buildWhereConditions = buildWhereConditions;
	private unifyRowWithQuery = unifyRowWithQuery;

	/**
	 * Create a batch processor utility
	 */
	private createBatchProcessor<T>(options: {
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

		const flushBatch = (): Promise<void> => {
			clearDebounce();
			if (flushingPromise) return flushingPromise;
			if (batch.length === 0 || cancelled) return Promise.resolve();

			const toFlush = batch;
			batch = [];
			flushingPromise = Promise.resolve(options.onFlush(toFlush)).finally(
				() => {
					flushingPromise = null;
				},
			);
			return flushingPromise;
		};

		const addItem = (item: T): void => {
			// if (cancelled) return;
			batch.push(item);
			if (batch.length >= options.batchSize) {
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
}
