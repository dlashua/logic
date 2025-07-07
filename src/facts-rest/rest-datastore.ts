import type {
  DataStore,
  QueryParams,
  WhereCondition,
  DataRow,
  RestDataStoreConfig
} from "../facts-abstract/types.ts";
import type { RestRelationOptions } from "./types.ts";

/**
 * REST API implementation of DataStore
 * Demonstrates how the abstract interface can work with different backends
 */
export class RestDataStore implements DataStore {
  readonly type = "rest";

  private config: Required<RestDataStoreConfig>;

  constructor(config: RestDataStoreConfig) {
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
  }

  async executeQuery(params: QueryParams): Promise<DataRow[]> {
    // Type assertion: treat relationOptions as RestRelationOptions for REST
    const restParams = {
      ...params,
      relationOptions: params.relationOptions as RestRelationOptions
    };

    // Handle IN operations that don't support comma-separated values
    if (!this.config.features.supportsInOperator) {
      return await this.executeQueryWithoutInOperator(restParams);
    }

    // Build URL - check for primary key in path
    const { baseUrl, primaryKeyCondition, otherConditions } = this.buildUrl(restParams);
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
    this.addPaginationToUrl(url, params);

    // Execute the request
    return await this.executeHttpRequest(url.toString(), params);
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
          allResults.push(...results);
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
    this.addPaginationToUrl(url, params);

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

    // Log the actual HTTP request
    if (params.logQuery) {
      const goalPrefix = params.goalId ? `G:${params.goalId}` : 'REST';
      const headersStr = Object.keys(headers).length > 2 ? 
        ` Headers: ${JSON.stringify({
          ...headers,
          Authorization: undefined 
        })}` : '';
      params.logQuery(`${goalPrefix} - GET ${url}${headersStr}`);
    }

    console.log("FETCHING", url);
    // Execute the request
    const response = await fetch(url, {
      method: 'GET',
      headers,
      signal: AbortSignal.timeout(this.config.timeout)
    });

    if (!response.ok) {
      throw new Error(`REST API error: ${response.status} ${response.statusText} @${url}`);
    }

    const data = await response.json();
    console.log("RESPONSE", response.status, url)

    // Handle different response formats
    if (Array.isArray(data)) {
      return data;
    } else if (data.data && Array.isArray(data.data)) {
      // Handle wrapped responses like { data: [...], total: 100 }
      return data.data;
    } else if (data.results && Array.isArray(data.results)) {
      // Handle pagination format like { results: [...], count: 100 }
      return data.results;
    } else if (typeof data === 'object' && data !== null) {
      // Handle single object responses (like Pokemon API individual records)
      return [data];
    } else {
      console.error("UNEXPECTED FORMAT");
      throw new Error(`Unexpected REST API response format: ${JSON.stringify(data)}`);
    }
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