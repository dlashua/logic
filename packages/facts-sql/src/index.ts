import type { AbstractRelationConfig } from "facts-abstract";
import { createAbstractRelationSystem } from "facts-abstract";
import type { Knex } from "knex";
import knex from "knex";
import type { BaseConfig as Configuration } from "logic";
import { getDefaultLogger, Logger } from "logic";
import { SqlDataStore } from "./sql-datastore.js";

/**
 * SQL implementation using the abstract data layer
 * This is a drop-in replacement for the old facts-sql module
 */
export const makeRelDB = async (
	knex_connect_options: Knex.Config,
	options?: Record<string, string>,
	configOverrides?: Partial<Configuration>,
) => {
	options ??= {};

	const logger = getDefaultLogger();
	const db = knex(knex_connect_options);

	// Create SQL data store
	const dataStore = new SqlDataStore(db);

	// Configure the abstract relation system
	const config: AbstractRelationConfig = {
		batchSize: 100,
		debounceMs: 50,
		enableCaching: true,
		enableQueryMerging: true,
		...configOverrides,
	};

	// Create the abstract relation system
	const relationSystem = createAbstractRelationSystem(
		dataStore,
		logger,
		config,
	);

	return {
		rel: relationSystem.rel,
		relSym: relationSystem.relSym,
		db,
		getQueries: relationSystem.getQueries,
		clearQueries: relationSystem.clearQueries,
		getQueryCount: relationSystem.getQueryCount,
		close: relationSystem.close,
	};
};
