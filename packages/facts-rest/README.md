# REST API Data Store

The REST API data store provides a generic interface for working with various REST API patterns. It supports multiple configurations to handle different API designs.

## Features

### Primary Key in URL Path

Many REST APIs include the primary key in the URL path instead of query parameters:

```typescript
const api = await makeRelREST({
  baseUrl: "https://api.example.com/v1",
  features: {
    primaryKeyInPath: true,
  },
});

const users = api.rel("users", {
  restPrimaryKey: "id", // Specifies which field is the primary key
});

// This will generate: GET /users/123 instead of /users?id=123
users({ id: 123, name: $.name });
```

### Custom URL Builder

For complex URL patterns, you can provide a custom URL builder:

```typescript
const api = await makeRelREST({
  baseUrl: "https://api.example.com",
  features: {
    urlBuilder: (table, primaryKey, primaryKeyValue) => {
      if (primaryKey && primaryKeyValue) {
        return `https://api.example.com/${table}/${primaryKeyValue}/details`;
      }
      return `https://api.example.com/${table}`;
    },
  },
});
```

### IN Operator Support

Some APIs don't support comma-separated values for IN operations. When disabled, multiple requests are made:

```typescript
const api = await makeRelREST({
  baseUrl: "https://api.example.com",
  features: {
    supportsInOperator: false, // Will make separate requests for each value
  },
});

// This will make 3 separate requests instead of one with ?id=1,2,3
users({ id: [1, 2, 3], name: $.name });
```

### Field Selection Support

Some APIs don't support field selection. When disabled, the `fields` parameter is not sent:

```typescript
const api = await makeRelREST({
  baseUrl: "https://api.example.com",
  features: {
    supportsFieldSelection: false, // Won't send ?fields=name,email
  },
});
```

### Custom Query Parameter Formatting

For APIs with non-standard query parameter formats:

```typescript
const api = await makeRelREST({
  baseUrl: "https://api.example.com",
  features: {
    queryParamFormatter: (column, operator, value) => {
      if (operator === "gt") {
        return { key: `${column}_greater_than`, value: String(value) };
      }
      if (operator === "like") {
        return { key: `search_${column}`, value: String(value) };
      }
      return { key: column, value: String(value) };
    },
  },
});
```

## Configuration Options

```typescript
interface RestDataStoreConfig {
  baseUrl: string;
  apiKey?: string;
  timeout?: number;
  headers?: Record<string, string>;
  pagination?: {
    limitParam?: string;
    offsetParam?: string;
    maxPageSize?: number;
  };
  features?: {
    /** Whether to include primary key in URL path instead of query params */
    primaryKeyInPath?: boolean;
    /** Whether API supports comma-separated values for IN operations */
    supportsInOperator?: boolean;
    /** Whether API supports field selection via query params */
    supportsFieldSelection?: boolean;
    /** Custom URL builder for different API patterns */
    urlBuilder?: (
      table: string,
      primaryKey?: string,
      primaryKeyValue?: any,
    ) => string;
    /** Custom query parameter formatter */
    queryParamFormatter?: (
      column: string,
      operator: string,
      value: any,
    ) => { key: string; value: string };
  };
}
```

## Examples

See `src/test/rest-test.ts` for complete examples of different REST API patterns.
