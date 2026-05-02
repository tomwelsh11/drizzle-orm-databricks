# Architecture: drizzle-orm-adapter-databricks

## Approach: Standalone dialect from base Drizzle abstractions

This adapter extends directly from Drizzle ORM's base classes (`Column`, `ColumnBuilder`, `Table`) without depending on any dialect-specific module (`pg-core`, `mysql-core`, `sqlite-core`). This makes the adapter independent of any existing dialect's internals and gives us full control over Spark SQL type mapping and generation.

### Why standalone over extending an existing dialect

1. **Databricks is not MySQL or PostgreSQL.** Spark SQL has its own type system (STRING not TEXT, native BOOLEAN not TINYINT, VARIANT for semi-structured data, TIMESTAMP_NTZ, etc). Extending MySQL or PG types creates a conceptual mismatch and confusing error messages.

2. **No breaking change coupling.** Drizzle's internal dialect modules can change between versions. By extending only the stable base classes (`Column`, `ColumnBuilder`, `Table`), we're resilient to internal refactors.

3. **Accurate SQL generation.** Databricks uses backtick identifier quoting (like MySQL) and `?` ordinal parameters. Our `DatabricksDialect` class generates correct Spark SQL without translation layers.

## Module structure

```
src/
├── columns/
│   ├── common.ts       # DatabricksColumn, DatabricksColumnBuilder (extend base)
│   ├── string.ts       # STRING
│   ├── varchar.ts      # VARCHAR(n)
│   ├── char.ts         # CHAR(n)
│   ├── int.ts          # INT
│   ├── bigint.ts       # BIGINT
│   ├── smallint.ts     # SMALLINT
│   ├── tinyint.ts      # TINYINT
│   ├── float.ts        # FLOAT
│   ├── double.ts       # DOUBLE
│   ├── decimal.ts      # DECIMAL(p, s)
│   ├── boolean.ts      # BOOLEAN (native, not TINYINT)
│   ├── date.ts         # DATE
│   ├── timestamp.ts    # TIMESTAMP / TIMESTAMP_NTZ
│   ├── binary.ts       # BINARY
│   ├── variant.ts      # VARIANT (semi-structured JSON-like)
│   └── index.ts        # re-exports
├── query-builders/
│   ├── select.ts       # DatabricksSelectBuilder, DatabricksSelectBase
│   ├── insert.ts       # DatabricksInsertBuilder, DatabricksInsertBase
│   ├── update.ts       # DatabricksUpdateBuilder, DatabricksUpdateBase
│   ├── delete.ts       # DatabricksDeleteBase
│   └── index.ts        # re-exports
├── table.ts            # DatabricksTable, databricksTable(), databricksSchema()
├── dialect.ts          # DatabricksDialect (SQL building, escapeName, escapeParam)
├── session.ts          # DatabricksSession, DatabricksPreparedQuery
├── connection.ts       # SessionManager (lazy init, stale-session retry)
├── driver.ts           # DatabricksDatabase, drizzle() factory
├── errors.ts           # DatabricksUnsupportedError, DatabricksConnectionError
├── types.ts            # Config types
├── migrator.ts         # migrate() function
└── index.ts            # public API re-exports
```

## Key design decisions

### dialect: 'common'

Drizzle's type system has `Dialect = 'pg' | 'mysql' | 'sqlite' | 'common'`. We use `'common'` because `BuildColumn<..., 'common'>` resolves to base `Column`, which is compatible with our `DatabricksColumn`. The `sql` template tag and all Drizzle operators (`eq`, `and`, `or`, etc.) work with any `Column` type.

### Session management

`SessionManager` handles the `@databricks/sql` driver lifecycle:
- Lazy client/session creation (cold start is 2-6s for serverless warehouses)
- Stale session detection and automatic retry
- Clean shutdown of sessions and owned clients
- Support for both "bring your own client" and "connect from credentials"

### No RETURNING clause

Databricks does not support RETURNING on INSERT/UPDATE/DELETE. The adapter does not attempt to emulate it. Users should generate primary keys client-side (UUIDs) and query after insert if needed.

### No transactions

Databricks does not support multi-statement transactions (Public Preview as of 2026 for managed Delta tables). `session.transaction()` throws `DatabricksUnsupportedError`.

### Query API

The adapter provides two query APIs:

**Query builders** — `.select().from()`, `.insert().values()`, `.update().set()`, `.delete().where()` with full Drizzle operator support (`eq`, `and`, `or`, `gt`, `lt`, etc.) and type-safe results.

**Raw SQL** — `db.execute()` with Drizzle's `sql` template tag for type-safe SQL execution. The template tag handles column references, table references, ordinal parameters, and arbitrary SQL composition.

### Query builder architecture

The query builders follow the same pattern as Drizzle's MySQL dialect (closest to Spark SQL):

- `DatabricksDialect` builds SQL via `buildSelectQuery`, `buildInsertQuery`, `buildUpdateQuery`, `buildDeleteQuery`
- Builder classes (`DatabricksSelectBuilder`, `DatabricksInsertBuilder`, etc.) compose configuration objects
- Base classes (`DatabricksSelectBase`, `DatabricksInsertBase`, etc.) call the dialect to build SQL, then execute via `DatabricksSession.prepareQuery()`
- Result mapping converts Databricks' object-keyed rows to typed results using column decoders

Key adaptation: Databricks returns rows as `Record<string, unknown>` (objects), not positional arrays like MySQL. The select builder uses a custom result mapper that extracts values by column name and applies type decoders.
