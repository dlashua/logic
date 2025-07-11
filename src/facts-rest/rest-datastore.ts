import type {
  DataStore,
  QueryParams,
  WhereCondition,
  DataRow,
  RestDataStoreConfig
} from "../facts-abstract/types.ts";
import type { RestRelationOptions } from "./types.ts";
import type { RelationCache } from "./relation-cache.ts";

/**
 * REST API implementation of DataStore
 * Demonstrates how the abstract interface can work with different backends
 */
export class RestDataStore implements DataStore {
  readonly type = "rest";

  private config: Required<RestDataStoreConfig>;
  private cache?: RelationCache | null;
  private cacheMethods: string[];
  private cachePrefix: string;

  constructor(config: RestDataStoreConfig & { cache?: RelationCache | null, cacheMethods?: string[], cachePrefix?: string }) {
    this.config = {
      baseUrl: config.baseUrl.replace(/\/$/, ''), // Remove trailing slash
      apiKey: config.apiKey ?? '',
      timeout: config.timeout ?? 30000,
      headers: config.headers ?? {},
      pagination: {
        limitParam: config.pagination?.limitParam ?? 'limit',
        offsetParam: config.pagination?.offsetParam ?? 'offset',
        maxPageSize: config.pagination?.maxPageSize ?? 1000,
        ...config.pagination
      },
      features: {
        primaryKeyInPath: config.features?.primaryKeyInPath ?? false,
        supportsInOperator: config.features?.supportsInOperator ?? true,
        supportsFieldSelection: config.features?.supportsFieldSelection ?? true,
        urlBuilder: config.features?.urlBuilder,
        queryParamFormatter: config.features?.queryParamFormatter,
        ...config.features
      }
    };
    this.cache = config.cache;
    this.cacheMethods = config.cacheMethods ?? ["GET"];
    this.cachePrefix = config.cachePrefix ?? "";
  }

  async executeQuery(params: QueryParams): Promise<DataRow[]> {
    // Detect a 'limit' whereCondition and move it to params.limit
    let limitFromWhere: number | undefined = undefined;
    const filteredWhere = params.whereConditions.filter(c => {
      if (c.column === 'limit' && c.operator === 'eq' && typeof c.value === 'number') {
        limitFromWhere = c.value;
        return false;
      }
      return true;
    });
    const effectiveParams = {
      ...params,
      limit: limitFromWhere ?? params.limit,
      whereConditions: filteredWhere,
      relationOptions: params.relationOptions as RestRelationOptions
    };

    // Handle IN operations that don't support comma-separated values
    if (!this.config.features.supportsInOperator) {
      return await this.executeQueryWithoutInOperator(effectiveParams);
    }

    // Build URL - check for primary key in path
    const { baseUrl, primaryKeyCondition, otherConditions } = this.buildUrl(effectiveParams);
    const url = new URL(baseUrl);

    // Add non-primary-key WHERE conditions as query parameters
    for (const condition of otherConditions) {
      this.addConditionToUrl(url, condition);
    }

    // Add field selection (if the API supports it)
    if (this.config.features.supportsFieldSelection && effectiveParams.selectColumns.length > 0) {
      url.searchParams.append('fields', effectiveParams.selectColumns.join(','));
    }

    // Add pagination
    // this.addPaginationToUrl(url, effectiveParams);

    // Execute the request
    return await this.executeHttpRequest(url.toString(), effectiveParams);
  }

  private async executeQueryWithoutInOperator(params: QueryParams): Promise<DataRow[]> {
    // Find IN conditions and split them into multiple queries
    const inConditions = params.whereConditions.filter(c => c.operator === 'in' && c.values);
    const otherConditions = params.whereConditions.filter(c => !(c.operator === 'in' && c.values));

    if (inConditions.length === 0) {
      // No IN conditions, execute normally but bypass the IN operator check
      return await this.executeQueryDirect({
        ...params,
        whereConditions: otherConditions 
      });
    }

    // Execute multiple queries for each IN condition value
    const allResults: DataRow[] = [];
    for (const inCondition of inConditions) {
      if (inCondition.values) {
        for (const value of inCondition.values) {
          const eqCondition = {
            column: inCondition.column,
            operator: 'eq' as const,
            value: value
          };
          const queryParams = {
            ...params,
            whereConditions: [...otherConditions, eqCondition]
          };
          const results = await this.executeQueryDirect(queryParams);
          // Tag each result row with the IN value for correct unification
          for (const row of results) {
            allResults.push({
              ...row,
              [inCondition.column]: value 
            });
          }
        }
      }
    }

    // Remove duplicates (in case of overlapping results)
    const uniqueResults = allResults.filter((row, index, self) => 
      index === self.findIndex(r => JSON.stringify(r) === JSON.stringify(row))
    );
    
    return uniqueResults;
  }

  private async executeQueryDirect(params: QueryParams): Promise<DataRow[]> {
    // Direct execution without IN operator check to avoid recursion
    const { baseUrl, primaryKeyCondition, otherConditions } = this.buildUrl(params);
    const url = new URL(baseUrl);

    // Add non-primary-key WHERE conditions as query parameters
    for (const condition of otherConditions) {
      this.addConditionToUrl(url, condition);
    }

    // Add field selection (if the API supports it)
    if (this.config.features.supportsFieldSelection && params.selectColumns.length > 0) {
      url.searchParams.append('fields', params.selectColumns.join(','));
    }

    // Add pagination
    // this.addPaginationToUrl(url, params);

    // Execute the request
    return await this.executeHttpRequest(url.toString(), params);
  }

  private buildUrl(params: QueryParams): {
    baseUrl: string;
    primaryKeyCondition: WhereCondition | null;
    otherConditions: WhereCondition[];
  } {
    // --- Flexible path template logic ---
    function fillPathTemplate(template: string, whereConditions: WhereCondition[]): { path: string, usedColumns: Set<string> } {
      const usedColumns = new Set<string>();
      let path = template.replace(/:([a-zA-Z0-9_]+)\??/g, (_, key) => {
        const cond = whereConditions.find(c => c.column === key && c.operator === 'eq');
        if (cond && cond.value !== undefined && cond.value !== null) {
          usedColumns.add(key);
          return encodeURIComponent(cond.value);
        }
        // If optional (ends with ?), remove segment if not present
        return '';
      });
      // Remove double slashes from missing optional params
      path = path.replace(/\/+/g, '/').replace(/\/$/, '');
      return {
        path,
        usedColumns 
      };
    }

    let path = '';
    let usedColumns = new Set<string>();

    if (params.relationIdentifier) {
      const filled = fillPathTemplate(params.relationIdentifier, params.whereConditions);
      path = filled.path;
      usedColumns = filled.usedColumns;
    } else if (this.config.features.primaryKeyInPath) {
      // Look for primary key condition using relation options
      const primaryKeyColumn = params.relationOptions?.restPrimaryKey || params.relationOptions?.primaryKey;
      let primaryKeyCondition: WhereCondition | null = null;
      let otherConditions: WhereCondition[] = [];
      if (primaryKeyColumn) {
        // Find condition that matches the primary key column
        primaryKeyCondition = params.whereConditions.find(c => 
          c.column === primaryKeyColumn && c.operator === 'eq'
        ) || null;
        
        if (primaryKeyCondition) {
          otherConditions = params.whereConditions.filter(c => c !== primaryKeyCondition);
        } else {
          otherConditions = params.whereConditions;
        }
      } else {
        otherConditions = params.whereConditions;
      }
      if (primaryKeyCondition) {
        path = `/${params.relationIdentifier}/${primaryKeyCondition.value}`;
        // @ts-expect-error
        usedColumns.add(primaryKeyColumn);
      } else {
        path = `/${params.relationIdentifier}`;
      }
    } else {
      path = `/${params.relationIdentifier}`;
    }
    // Remove used columns from query params
    const otherConditions = params.whereConditions.filter(c => !usedColumns.has(c.column));
    const baseUrl = `${this.config.baseUrl}${path}`;
    return {
      baseUrl,
      primaryKeyCondition: null,
      otherConditions
    };
  }

  private addConditionToUrl(url: URL, condition: WhereCondition): void {
    if (this.config.features.queryParamFormatter) {
      const formatted = this.config.features.queryParamFormatter(
        condition.column,
        condition.operator,
        condition.value
      );
      url.searchParams.append(formatted.key, formatted.value);
      return;
    }

    // Default behavior
    if (condition.operator === 'eq') {
      url.searchParams.append(condition.column, String(condition.value));
    } else if (condition.operator === 'in' && condition.values) {
      if (this.config.features.supportsInOperator) {
        url.searchParams.append(condition.column, condition.values.join(','));
      }
      // If doesn't support IN, this will be handled by executeQueryWithoutInOperator
    } else if (condition.operator === 'gt') {
      url.searchParams.append(`${condition.column}[gt]`, String(condition.value));
    } else if (condition.operator === 'lt') {
      url.searchParams.append(`${condition.column}[lt]`, String(condition.value));
    } else if (condition.operator === 'gte') {
      url.searchParams.append(`${condition.column}[gte]`, String(condition.value));
    } else if (condition.operator === 'lte') {
      url.searchParams.append(`${condition.column}[lte]`, String(condition.value));
    } else if (condition.operator === 'like') {
      url.searchParams.append(`${condition.column}[like]`, String(condition.value));
    }
  }

  private addPaginationToUrl(url: URL, params: QueryParams): void {
    if (params.limit && this.config.pagination.limitParam) {
      const limit = Math.min(params.limit, this.config.pagination.maxPageSize ?? params.limit);
      url.searchParams.append(this.config.pagination.limitParam, String(limit));
    }
    if (params.offset && this.config.pagination.offsetParam) {
      url.searchParams.append(this.config.pagination.offsetParam, String(params.offset));
    }
  }

  // Helper: auto-paginate for APIs with { results: [], page: number } format
  private async fetchAllPages(url: string, params: QueryParams, initialData: any, initialPage: number, pageSize: number, totalLimit: number): Promise<DataRow[]> {
    const allResults: DataRow[] = Array.isArray(initialData.results) ? initialData.results : [];
    let page = initialPage;
    const done = false;
    // Cache the initial page if caching is enabled
    const method = 'GET';
    const shouldCache = this.cache && this.cacheMethods.includes(method);
    const limitKey = params.limit !== undefined ? `:limit=${params.limit}` : '';
    if (shouldCache) {
      const cacheKey = this.cachePrefix + method + ':' + url + limitKey;
      await this.cache!.set(cacheKey, initialData.results);
    }
    while (!done && allResults.length < totalLimit) {
      page++;
      const pagedUrl = new URL(url);
      pagedUrl.searchParams.set("page", String(page));
      pagedUrl.searchParams.set("limit", String(pageSize));
      const pagedUrlStr = pagedUrl.toString();
      let pageResults: any[] | undefined = undefined;
      let fromCache = false;
      const pageLimitKey = params.limit !== undefined ? `:limit=${params.limit}` : '';
      if (shouldCache) {
        const cacheKey = this.cachePrefix + method + ':' + pagedUrlStr + pageLimitKey;
        const cached = await this.cache!.get(cacheKey);
        if (cached !== undefined) {
          pageResults = Array.isArray(cached) ? cached : (cached && typeof cached === 'object' && Array.isArray(cached.results) ? cached.results : []);
          fromCache = true;
        }
      }
      if (!fromCache) {
        const response = await fetch(pagedUrlStr, {
          method: 'GET',
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            ...this.config.headers,
            ...(this.config.apiKey ? {
              'Authorization': `Bearer ${this.config.apiKey}` 
            } : {})
          },
          signal: AbortSignal.timeout(this.config.timeout)
        });
        if (!response.ok) break;
        const data = await response.json();
        pageResults = Array.isArray(data.results) ? data.results : [];
        if (shouldCache) {
          const cacheKey = this.cachePrefix + method + ':' + pagedUrlStr + pageLimitKey;
          await this.cache!.set(cacheKey, pageResults);
        }
      }
      if (!pageResults || pageResults.length === 0) break;
      allResults.push(...pageResults);
      if (pageResults.length < pageSize) break; // last page
    }
    return allResults.slice(0, totalLimit);
  }

  private async executeHttpRequest(url: string, params: QueryParams): Promise<DataRow[]> {
    // Prepare headers
    const headers: Record<string, string> = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      ...this.config.headers
    };

    if (this.config.apiKey) {
      headers['Authorization'] = `Bearer ${this.config.apiKey}`;
    }

    // Determine HTTP method (default GET)
    const method = 'GET'; // TODO: support other methods if needed
    const limitKey = params.limit !== undefined ? `:limit=${params.limit}` : '';
    const cacheKey = this.cachePrefix + method + ':' + url + limitKey;

    const shouldCache = this.cache && this.cacheMethods.includes(method);

    if (shouldCache) {
      const cached = await this.cache!.get(cacheKey);
      if (cached !== undefined) {
        if (params.logQuery) {
          const headersStr = Object.keys(headers).length > 2 ? 
            ` Headers: ${JSON.stringify({
              ...headers,
              Authorization: undefined 
            })}` : '';
          params.logQuery(`[CACHED] ${method} ${url}${headersStr}`);
        }
        if (Array.isArray(cached)) {
          return cached;
        }
        if (typeof cached === 'string') {
          try {
            const arr = JSON.parse(cached);
            if (Array.isArray(arr)) return arr;
          } catch (e) {/* ignore parse error */}
        }
        if (cached && typeof cached === 'object') return [cached];
        return [];
      }
    }

    // Log the actual HTTP request
    if (params.logQuery) {
      const headersStr = Object.keys(headers).length > 2 ? 
        ` Headers: ${JSON.stringify({
          ...headers,
          Authorization: undefined 
        })}` : '';
      params.logQuery(`${method} ${url}${headersStr}`);
    }

    // Execute the request
    const response = await fetch(url, {
      method,
      headers,
      signal: AbortSignal.timeout(this.config.timeout)
    });

    if (!response.ok) {
      throw new Error(`REST API error: ${response.status} ${response.statusText} @${url}`);
    }

    const data = await response.json();

    // Auto-paginate if response is { results: [], page: number }
    const totalLimit = params.limit ?? 50;
    let result: DataRow[];
    if (data && Array.isArray(data.results) && typeof data.page === "number") {
      if (data.results.length >= totalLimit) {
        result = data.results.slice(0, totalLimit);
      } else {
        // Fetch more pages if needed
        const pageSize = data.results.length;
        if (pageSize === 0) return [];
        result = await this.fetchAllPages(url, params, data, data.page, pageSize, totalLimit);
      }
    } else if (Array.isArray(data)) {
      result = data;
    } else if (data.data && Array.isArray(data.data)) {
      result = data.data;
    } else if (data.results && Array.isArray(data.results)) {
      result = data.results;
    } else if (typeof data === 'object' && data !== null) {
      result = [data];
    } else {
      console.error("UNEXPECTED FORMAT");
      throw new Error(`Unexpected REST API response format: ${JSON.stringify(data)}`);
    }

    if (shouldCache) {
      await this.cache!.set(cacheKey, result);
    }
    return result;
  }

  async getColumns(relationIdentifier: string): Promise<string[]> {
    // This would typically require a schema endpoint
    // For now, we'll return an empty array or make a sample query
    try {
      const url = `${this.config.baseUrl}/${relationIdentifier}`;
      const headers: Record<string, string> = {
        'Accept': 'application/json',
        ...this.config.headers
      };

      if (this.config.apiKey) {
        headers['Authorization'] = `Bearer ${this.config.apiKey}`;
      }

      // Try to get first record to infer schema
      console.log("FETCH COLUMNS", url);
      const response = await fetch(`${url}?limit=1`, {
        method: 'GET',
        headers,
        signal: AbortSignal.timeout(this.config.timeout)
      });

      if (response.ok) {
        const data = await response.json();
        const records = Array.isArray(data) ? data : (data.data || data.results || []);
        if (records.length > 0) {
          return Object.keys(records[0]);
        }
      }
    } catch (error) {
      console.warn(`Could not infer columns for relationIdentifier ${relationIdentifier}:`, error);
    }

    return [];
  }

  buildWhereConditions(clauses: Record<string, Set<any>>): WhereCondition[] {
    const conditions: WhereCondition[] = [];
    
    for (const [column, values] of Object.entries(clauses)) {
      if (values.size === 1) {
        conditions.push({
          column,
          operator: 'eq',
          value: Array.from(values)[0]
        });
      } else if (values.size > 1) {
        conditions.push({
          column,
          operator: 'in',
          value: null,
          values: Array.from(values)
        });
      }
    }
    
    return conditions;
  }

  async close(): Promise<void> {
    // No cleanup needed for REST API
  }
}