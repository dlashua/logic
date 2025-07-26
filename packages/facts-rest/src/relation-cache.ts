import Knex, { type Knex as KnexType } from "knex";

export interface RelationCache {
	get(key: string): Promise<any | undefined>;
	set(key: string, value: any): Promise<void>;
	delete?(key: string): Promise<void>;
	clear?(): Promise<void>;
}

export interface SqlRelationCacheOptions {
	knexInstance?: KnexType;
	knexConfig?: KnexType.Config;
	tableName?: string;
	cachePrefix?: string;
	ttlSeconds?: number; // default 1800 (30 min)
}

export class SqlRelationCache implements RelationCache {
	private knex: KnexType;
	private table: string;
	private prefix: string;
	private ready: Promise<void>;
	private ttl: number;

	constructor(options: SqlRelationCacheOptions) {
		if (options.knexInstance) {
			this.knex = options.knexInstance;
		} else if (options.knexConfig) {
			this.knex = Knex(options.knexConfig);
		} else {
			throw new Error("Must provide knexInstance or knexConfig");
		}
		this.table = options.tableName || "relation_cache";
		this.prefix = options.cachePrefix || "";
		this.ttl = options.ttlSeconds ?? 1800;
		this.ready = this.ensureTable();
	}

	private async ensureTable() {
		const exists = await this.knex.schema.hasTable(this.table);
		if (!exists) {
			await this.knex.schema.createTable(this.table, (t) => {
				t.string("key").primary();
				t.text("value");
				t.timestamp("created_at").defaultTo(this.knex.fn.now());
			});
		}
	}

	private fullKey(key: string) {
		return this.prefix + key;
	}

	async get(key: string): Promise<any | undefined> {
		await this.ready;
		const row = await this.knex(this.table)
			.where({
				key: this.fullKey(key),
			})
			.first();
		if (!row) return undefined;
		// TTL check
		const created = row.created_at ? new Date(row.created_at).getTime() : 0;
		const now = Date.now();
		if (created && this.ttl > 0 && now - created > this.ttl * 1000) {
			// Expired, delete and return undefined
			await this.delete(key);
			return undefined;
		}
		try {
			return JSON.parse(row.value);
		} catch {
			return row.value;
		}
	}

	async set(key: string, value: any): Promise<void> {
		await this.ready;
		const val = JSON.stringify(value);
		await this.knex(this.table)
			.insert({
				key: this.fullKey(key),
				value: val,
				created_at: new Date(),
			})
			.onConflict("key")
			.merge({
				value: val,
				created_at: new Date(),
			});
	}

	async delete(key: string): Promise<void> {
		await this.ready;
		await this.knex(this.table)
			.where({
				key: this.fullKey(key),
			})
			.del();
	}

	async clear(): Promise<void> {
		await this.ready;
		await this.knex(this.table).del();
	}
}
