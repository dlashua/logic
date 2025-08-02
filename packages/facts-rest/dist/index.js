// src/index.ts
import { createAbstractRelationSystem } from "@codespiral/facts-abstract";
import { getDefaultLogger } from "@codespiral/logic";

// src/rest-datastore.ts
var RestDataStore = class {
  type = "rest";
  config;
  cache;
  cacheMethods;
  cachePrefix;
  constructor(config) {
    this.config = {
      baseUrl: config.baseUrl.replace(/\/$/, ""),
      // Remove trailing slash
      apiKey: config.apiKey ?? "",
      timeout: config.timeout ?? 3e4,
      headers: config.headers ?? {},
      pagination: {
        limitParam: config.pagination?.limitParam ?? "limit",
        offsetParam: config.pagination?.offsetParam ?? "offset",
        maxPageSize: config.pagination?.maxPageSize ?? 1e3,
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
  async executeQuery(params) {
    let limitFromWhere;
    const filteredWhere = params.whereConditions.filter((c) => {
      if (c.column === "limit" && c.operator === "eq" && typeof c.value === "number") {
        limitFromWhere = c.value;
        return false;
      }
      return true;
    });
    const effectiveParams = {
      ...params,
      limit: limitFromWhere ?? params.limit,
      whereConditions: filteredWhere,
      relationOptions: params.relationOptions
    };
    if (!this.config.features.supportsInOperator) {
      return await this.executeQueryWithoutInOperator(effectiveParams);
    }
    const { baseUrl, primaryKeyCondition, otherConditions } = this.buildUrl(effectiveParams);
    const url = new URL(baseUrl);
    for (const condition of otherConditions) {
      this.addConditionToUrl(url, condition);
    }
    if (this.config.features.supportsFieldSelection && effectiveParams.selectColumns.length > 0) {
      url.searchParams.append(
        "fields",
        effectiveParams.selectColumns.join(",")
      );
    }
    return await this.executeHttpRequest(url.toString(), effectiveParams);
  }
  async executeQueryWithoutInOperator(params) {
    const inConditions = params.whereConditions.filter(
      (c) => c.operator === "in" && c.values
    );
    const otherConditions = params.whereConditions.filter(
      (c) => !(c.operator === "in" && c.values)
    );
    if (inConditions.length === 0) {
      return await this.executeQueryDirect({
        ...params,
        whereConditions: otherConditions
      });
    }
    const allResults = [];
    for (const inCondition of inConditions) {
      if (inCondition.values) {
        for (const value of inCondition.values) {
          const eqCondition = {
            column: inCondition.column,
            operator: "eq",
            value
          };
          const queryParams = {
            ...params,
            whereConditions: [...otherConditions, eqCondition]
          };
          const results = await this.executeQueryDirect(queryParams);
          for (const row of results) {
            allResults.push({
              ...row,
              [inCondition.column]: value
            });
          }
        }
      }
    }
    const uniqueResults = allResults.filter(
      (row, index, self) => index === self.findIndex((r) => JSON.stringify(r) === JSON.stringify(row))
    );
    return uniqueResults;
  }
  async executeQueryDirect(params) {
    const { baseUrl, primaryKeyCondition, otherConditions } = this.buildUrl(params);
    const url = new URL(baseUrl);
    for (const condition of otherConditions) {
      this.addConditionToUrl(url, condition);
    }
    if (this.config.features.supportsFieldSelection && params.selectColumns.length > 0) {
      url.searchParams.append("fields", params.selectColumns.join(","));
    }
    return await this.executeHttpRequest(url.toString(), params);
  }
  buildUrl(params) {
    function fillPathTemplate(template, whereConditions) {
      const usedColumns2 = /* @__PURE__ */ new Set();
      let path2 = template.replace(/:([a-zA-Z0-9_]+)\??/g, (_, key) => {
        const cond = whereConditions.find(
          (c) => c.column === key && c.operator === "eq"
        );
        if (cond && cond.value !== void 0 && cond.value !== null) {
          usedColumns2.add(key);
          return encodeURIComponent(cond.value);
        }
        return "";
      });
      path2 = path2.replace(/\/+/g, "/").replace(/\/$/, "");
      return {
        path: path2,
        usedColumns: usedColumns2
      };
    }
    let path = "";
    let usedColumns = /* @__PURE__ */ new Set();
    if (params.relationIdentifier) {
      const filled = fillPathTemplate(
        params.relationIdentifier,
        params.whereConditions
      );
      path = filled.path;
      usedColumns = filled.usedColumns;
    } else if (this.config.features.primaryKeyInPath) {
      const primaryKeyColumn = params.relationOptions?.restPrimaryKey || params.relationOptions?.primaryKey;
      let primaryKeyCondition = null;
      let otherConditions2 = [];
      if (primaryKeyColumn) {
        primaryKeyCondition = params.whereConditions.find(
          (c) => c.column === primaryKeyColumn && c.operator === "eq"
        ) || null;
        if (primaryKeyCondition) {
          otherConditions2 = params.whereConditions.filter(
            (c) => c !== primaryKeyCondition
          );
        } else {
          otherConditions2 = params.whereConditions;
        }
      } else {
        otherConditions2 = params.whereConditions;
      }
      if (primaryKeyCondition) {
        path = `/${params.relationIdentifier}/${primaryKeyCondition.value}`;
        usedColumns.add(primaryKeyColumn);
      } else {
        path = `/${params.relationIdentifier}`;
      }
    } else {
      path = `/${params.relationIdentifier}`;
    }
    const otherConditions = params.whereConditions.filter(
      (c) => !usedColumns.has(c.column)
    );
    const baseUrl = `${this.config.baseUrl}${path}`;
    return {
      baseUrl,
      primaryKeyCondition: null,
      otherConditions
    };
  }
  addConditionToUrl(url, condition) {
    if (this.config.features.queryParamFormatter) {
      const formatted = this.config.features.queryParamFormatter(
        condition.column,
        condition.operator,
        condition.value
      );
      url.searchParams.append(formatted.key, formatted.value);
      return;
    }
    if (condition.operator === "eq") {
      url.searchParams.append(condition.column, String(condition.value));
    } else if (condition.operator === "in" && condition.values) {
      if (this.config.features.supportsInOperator) {
        url.searchParams.append(condition.column, condition.values.join(","));
      }
    } else if (condition.operator === "gt") {
      url.searchParams.append(
        `${condition.column}[gt]`,
        String(condition.value)
      );
    } else if (condition.operator === "lt") {
      url.searchParams.append(
        `${condition.column}[lt]`,
        String(condition.value)
      );
    } else if (condition.operator === "gte") {
      url.searchParams.append(
        `${condition.column}[gte]`,
        String(condition.value)
      );
    } else if (condition.operator === "lte") {
      url.searchParams.append(
        `${condition.column}[lte]`,
        String(condition.value)
      );
    } else if (condition.operator === "like") {
      url.searchParams.append(
        `${condition.column}[like]`,
        String(condition.value)
      );
    }
  }
  addPaginationToUrl(url, params) {
    if (params.limit && this.config.pagination.limitParam) {
      const limit = Math.min(
        params.limit,
        this.config.pagination.maxPageSize ?? params.limit
      );
      url.searchParams.append(this.config.pagination.limitParam, String(limit));
    }
    if (params.offset && this.config.pagination.offsetParam) {
      url.searchParams.append(
        this.config.pagination.offsetParam,
        String(params.offset)
      );
    }
  }
  // Helper: auto-paginate for APIs with { results: [], page: number } format
  async fetchAllPages(url, params, initialData, initialPage, pageSize, totalLimit) {
    const allResults = Array.isArray(initialData.results) ? initialData.results : [];
    let page = initialPage;
    const done = false;
    const method = "GET";
    const shouldCache = this.cache && this.cacheMethods.includes(method);
    const limitKey = params.limit !== void 0 ? `:limit=${params.limit}` : "";
    if (shouldCache) {
      const cacheKey = this.cachePrefix + method + ":" + url + limitKey;
      await this.cache.set(cacheKey, initialData.results);
    }
    while (!done && allResults.length < totalLimit) {
      page++;
      const pagedUrl = new URL(url);
      pagedUrl.searchParams.set("page", String(page));
      pagedUrl.searchParams.set("limit", String(pageSize));
      const pagedUrlStr = pagedUrl.toString();
      let pageResults;
      let fromCache = false;
      const pageLimitKey = params.limit !== void 0 ? `:limit=${params.limit}` : "";
      if (shouldCache) {
        const cacheKey = this.cachePrefix + method + ":" + pagedUrlStr + pageLimitKey;
        const cached = await this.cache.get(cacheKey);
        if (cached !== void 0) {
          pageResults = Array.isArray(cached) ? cached : cached && typeof cached === "object" && Array.isArray(cached.results) ? cached.results : [];
          fromCache = true;
        }
      }
      if (!fromCache) {
        const response = await fetch(pagedUrlStr, {
          method: "GET",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
            ...this.config.headers,
            ...this.config.apiKey ? {
              Authorization: `Bearer ${this.config.apiKey}`
            } : {}
          },
          signal: AbortSignal.timeout(this.config.timeout)
        });
        if (!response.ok) break;
        const data = await response.json();
        pageResults = Array.isArray(data.results) ? data.results : [];
        if (shouldCache) {
          const cacheKey = this.cachePrefix + method + ":" + pagedUrlStr + pageLimitKey;
          await this.cache.set(cacheKey, pageResults);
        }
      }
      if (!pageResults || pageResults.length === 0) break;
      allResults.push(...pageResults);
      if (pageResults.length < pageSize) break;
    }
    return allResults.slice(0, totalLimit);
  }
  async executeHttpRequest(url, params) {
    const headers = {
      Accept: "application/json",
      "Content-Type": "application/json",
      ...this.config.headers
    };
    if (this.config.apiKey) {
      headers["Authorization"] = `Bearer ${this.config.apiKey}`;
    }
    const method = "GET";
    const limitKey = params.limit !== void 0 ? `:limit=${params.limit}` : "";
    const cacheKey = this.cachePrefix + method + ":" + url + limitKey;
    const shouldCache = this.cache && this.cacheMethods.includes(method);
    if (shouldCache) {
      const cached = await this.cache.get(cacheKey);
      if (cached !== void 0) {
        if (params.logQuery) {
          const headersStr = Object.keys(headers).length > 2 ? ` Headers: ${JSON.stringify({
            ...headers,
            Authorization: void 0
          })}` : "";
          params.logQuery(`[CACHED] ${method} ${url}${headersStr}`);
        }
        if (Array.isArray(cached)) {
          return cached;
        }
        if (typeof cached === "string") {
          try {
            const arr = JSON.parse(cached);
            if (Array.isArray(arr)) return arr;
          } catch (e) {
          }
        }
        if (cached && typeof cached === "object") return [cached];
        return [];
      }
    }
    if (params.logQuery) {
      const headersStr = Object.keys(headers).length > 2 ? ` Headers: ${JSON.stringify({
        ...headers,
        Authorization: void 0
      })}` : "";
      params.logQuery(`${method} ${url}${headersStr}`);
    }
    const response = await fetch(url, {
      method,
      headers,
      signal: AbortSignal.timeout(this.config.timeout)
    });
    if (!response.ok) {
      throw new Error(
        `REST API error: ${response.status} ${response.statusText} @${url}`
      );
    }
    const data = await response.json();
    const totalLimit = params.limit ?? 50;
    let result;
    if (data && Array.isArray(data.results) && typeof data.page === "number") {
      if (data.results.length >= totalLimit) {
        result = data.results.slice(0, totalLimit);
      } else {
        const pageSize = data.results.length;
        if (pageSize === 0) return [];
        result = await this.fetchAllPages(
          url,
          params,
          data,
          data.page,
          pageSize,
          totalLimit
        );
      }
    } else if (Array.isArray(data)) {
      result = data;
    } else if (data.data && Array.isArray(data.data)) {
      result = data.data;
    } else if (data.results && Array.isArray(data.results)) {
      result = data.results;
    } else if (typeof data === "object" && data !== null) {
      result = [data];
    } else {
      console.error("UNEXPECTED FORMAT");
      throw new Error(
        `Unexpected REST API response format: ${JSON.stringify(data)}`
      );
    }
    if (shouldCache) {
      await this.cache.set(cacheKey, result);
    }
    return result;
  }
  async getColumns(relationIdentifier) {
    try {
      const url = `${this.config.baseUrl}/${relationIdentifier}`;
      const headers = {
        Accept: "application/json",
        ...this.config.headers
      };
      if (this.config.apiKey) {
        headers["Authorization"] = `Bearer ${this.config.apiKey}`;
      }
      console.log("FETCH COLUMNS", url);
      const response = await fetch(`${url}?limit=1`, {
        method: "GET",
        headers,
        signal: AbortSignal.timeout(this.config.timeout)
      });
      if (response.ok) {
        const data = await response.json();
        const records = Array.isArray(data) ? data : data.data || data.results || [];
        if (records.length > 0) {
          return Object.keys(records[0]);
        }
      }
    } catch (error) {
      console.warn(
        `Could not infer columns for relationIdentifier ${relationIdentifier}:`,
        error
      );
    }
    return [];
  }
  buildWhereConditions(clauses) {
    const conditions = [];
    for (const [column, values] of Object.entries(clauses)) {
      if (values.size === 1) {
        conditions.push({
          column,
          operator: "eq",
          value: Array.from(values)[0]
        });
      } else if (values.size > 1) {
        conditions.push({
          column,
          operator: "in",
          value: null,
          values: Array.from(values)
        });
      }
    }
    return conditions;
  }
  async close() {
  }
};

// src/index.ts
var makeRelREST = async (restConfig, config) => {
  const logger = getDefaultLogger();
  const dataStore = new RestDataStore(restConfig);
  const systemConfig = {
    batchSize: 50,
    // Smaller batches for REST APIs
    debounceMs: 100,
    // Longer debounce for network calls
    enableCaching: true,
    enableQueryMerging: false,
    // REST APIs might not benefit from query merging
    ...config
  };
  const relationSystem = createAbstractRelationSystem(
    dataStore,
    logger,
    systemConfig
  );
  const origRel = relationSystem.rel;
  function rel(pathTemplate, options = {}) {
    if (Object.hasOwn(options, "cache")) {
      const relCache = options.cache;
      const relDataStore = new RestDataStore({
        ...restConfig,
        cache: relCache
      });
      const relSystem = createAbstractRelationSystem(
        relDataStore,
        logger,
        systemConfig
      );
      return relSystem.rel(pathTemplate, options);
    }
    return origRel(pathTemplate, options);
  }
  return {
    rel,
    relSym: relationSystem.relSym,
    getQueries: relationSystem.getQueries,
    clearQueries: relationSystem.clearQueries,
    getQueryCount: relationSystem.getQueryCount,
    close: relationSystem.close,
    getDataStore: relationSystem.getDataStore
  };
};
export {
  makeRelREST
};
//# sourceMappingURL=index.js.map