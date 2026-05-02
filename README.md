# drizzle-orm-databricks

A standalone [Drizzle ORM](https://orm.drizzle.team) adapter for [Databricks SQL](https://www.databricks.com/product/databricks-sql) warehouses. Built from Drizzle's base abstractions — **zero dependency on `mysql-core`, `pg-core`, or `sqlite-core`** — with Databricks-native column types, Spark SQL generation, and the official `@databricks/sql` Node.js driver.

## Installation

```bash
pnpm add drizzle-orm-databricks drizzle-orm @databricks/sql
```

`drizzle-orm` and `@databricks/sql` are peer dependencies.

## Quick start

```ts
import {
  drizzle,
  databricksTable,
  string,
  bigint,
  timestamp,
  boolean,
} from "drizzle-orm-databricks";
import { eq } from "drizzle-orm";

const users = databricksTable("users", {
  id: string("id").primaryKey(),
  email: string("email").notNull(),
  loginCount: bigint("login_count").notNull(),
  active: boolean("active").notNull(),
  createdAt: timestamp("created_at").notNull(),
});

const db = drizzle({
  host: process.env.DATABRICKS_HOST!,
  path: process.env.DATABRICKS_SQL_PATH!,
  token: process.env.DATABRICKS_TOKEN!,
});

const rows = await db.select().from(users).where(eq(users.active, true));

await db.insert(users).values({
  id: crypto.randomUUID(),
  email: "a@b.com",
  loginCount: BigInt(0),
  active: true,
  createdAt: new Date(),
});

await db.update(users).set({ active: false }).where(eq(users.id, "u1"));

await db.delete(users).where(eq(users.id, "u1"));

await db.$close();
```

## Authentication

### Personal access token (PAT)

```ts
const db = drizzle({
  host: "adb-1234567890123456.7.azuredatabricks.net",
  path: "/sql/1.0/warehouses/abc123",
  token: "dapi...",
  catalog: "main", // optional — workspace default if omitted
  schema: "default", // optional
});
```

### Service principal (OAuth M2M)

```ts
const db = drizzle({
  host: "adb-1234567890123456.7.azuredatabricks.net",
  path: "/sql/1.0/warehouses/abc123",
  clientId: process.env.DATABRICKS_CLIENT_ID!,
  clientSecret: process.env.DATABRICKS_CLIENT_SECRET!,
  catalog: "main",
  schema: "default",
});
```

Uses Databricks OAuth machine-to-machine flow. Register a service principal in your workspace and grant it SQL warehouse access.

### Bring your own DBSQLClient

```ts
import { DBSQLClient } from "@databricks/sql";

const client = new DBSQLClient();
await client.connect({ host, path, token });

const db = drizzle({ client, catalog: "main", schema: "default" });
```

When you pass an existing client, `db.$close()` closes the session but leaves the client open — you own its lifecycle.

## Column types

All column types map directly to Spark SQL types:

```ts
import {
  databricksTable,
  string,
  varchar,
  char,
  int,
  bigint,
  smallint,
  tinyint,
  float,
  double,
  decimal,
  boolean,
  date,
  timestamp,
  timestampNtz,
  binary,
  variant,
} from "drizzle-orm-databricks";

const events = databricksTable("events", {
  id: string("id").primaryKey(),
  name: varchar("name", { length: 255 }).notNull(),
  count: bigint("count").notNull(),
  score: double("score"),
  price: decimal("price", { precision: 18, scale: 6 }),
  active: boolean("active").notNull(),
  occurredAt: timestamp("occurred_at").notNull(),
  createdDate: date("created_date"),
  metadata: variant("metadata"),
});
```

| Column function                 | Spark SQL type  | JS type          |
| ------------------------------- | --------------- | ---------------- |
| `string()`                      | `STRING`        | `string`         |
| `varchar({ length })`           | `VARCHAR(n)`    | `string`         |
| `char({ length })`              | `CHAR(n)`       | `string`         |
| `int()`                         | `INT`           | `number`         |
| `bigint()`                      | `BIGINT`        | `bigint`         |
| `smallint()`                    | `SMALLINT`      | `number`         |
| `tinyint()`                     | `TINYINT`       | `number`         |
| `float()`                       | `FLOAT`         | `number`         |
| `double()`                      | `DOUBLE`        | `number`         |
| `decimal({ precision, scale })` | `DECIMAL(p, s)` | `string`         |
| `boolean()`                     | `BOOLEAN`       | `boolean`        |
| `date()`                        | `DATE`          | `Date`           |
| `timestamp()`                   | `TIMESTAMP`     | `Date`           |
| `timestampNtz()`                | `TIMESTAMP_NTZ` | `Date`           |
| `binary()`                      | `BINARY`        | `Uint8Array`     |
| `variant()`                     | `VARIANT`       | `unknown` (JSON) |

All column builders accept an optional column name: `string("col_name")`. If omitted, the property key is used.

Column modifiers: `.primaryKey()`, `.notNull()`, `.default(value)`, `.$default(fn)`.

## Unity Catalog namespaces

Databricks Unity Catalog organises data in a 3-tier namespace: `catalog.schema.table`. The adapter supports all levels of qualification.

### Schema-qualified tables

```ts
import { databricksSchema, string, int } from "drizzle-orm-databricks";

const analytics = databricksSchema("analytics");

const events = analytics.table("events", {
  id: string("id").primaryKey(),
  name: string("name").notNull(),
  count: int("count"),
});

// SQL: SELECT `id`, `name`, `count` FROM `analytics`.`events`
await db.select().from(events);
```

### Fully qualified tables (catalog.schema.table)

```ts
import { databricksCatalog, string, int } from "drizzle-orm-databricks";

const prod = databricksCatalog("prod");

// catalog + schema + table
const users = prod.schema("analytics").table("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
});

// SQL: SELECT `id`, `name`, `age` FROM `prod`.`analytics`.`users`
await db.select().from(users);

// catalog + table (no schema)
const logs = prod.table("logs", {
  id: string("id"),
  message: string("message"),
});

// SQL: SELECT `id`, `message` FROM `prod`.`logs`
await db.select().from(logs);
```

### Per-query namespace overrides

Override the catalog and/or schema at query time without changing the table definition. Useful for routing queries to staging/production catalogs dynamically:

```ts
import { databricksTable, string, int } from "drizzle-orm-databricks";

const users = databricksTable("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
});

// Override on SELECT
await db.select().from(users, { catalog: "staging", schema: "raw" });
// SQL: SELECT ... FROM `staging`.`raw`.`users`

// Override on INSERT
await db
  .insert(users, { catalog: "staging", schema: "raw" })
  .values({ id: "u1", name: "Alice", age: 30 });

// Override on UPDATE
await db
  .update(users, { catalog: "staging", schema: "raw" })
  .set({ name: "Bob" })
  .where(eq(users.id, "u1"));

// Override on DELETE
await db.delete(users, { catalog: "staging", schema: "raw" }).where(eq(users.id, "u1"));
```

Overrides apply only to the primary table — joined tables keep their own namespace.

## Query builders

### Select

```ts
import { eq, gt, desc, sql } from "drizzle-orm";

// Select all columns
const allUsers = await db.select().from(users);

// Partial select
const names = await db
  .select({ id: users.id, email: users.email })
  .from(users)
  .where(gt(users.loginCount, BigInt(10)));

// Where, order, limit, offset
const page = await db
  .select()
  .from(users)
  .where(eq(users.active, true))
  .orderBy(desc(users.createdAt))
  .limit(10)
  .offset(20);

// Select distinct
const uniqueEmails = await db.selectDistinct({ email: users.email }).from(users);
```

### Insert

```ts
// Single row
await db.insert(users).values({
  id: "u1",
  email: "a@b.com",
  loginCount: BigInt(0),
  active: true,
  createdAt: new Date(),
});

// Batch insert
await db.insert(users).values([
  { id: "u2", email: "b@c.com", loginCount: BigInt(0), active: true, createdAt: new Date() },
  { id: "u3", email: "c@d.com", loginCount: BigInt(0), active: false, createdAt: new Date() },
]);

// INSERT INTO ... SELECT
await db.insert(archive).select(db.select().from(users).where(eq(users.active, false)));
```

### Update

```ts
await db.update(users).set({ active: false }).where(eq(users.id, "u1"));
```

### Delete

```ts
await db.delete(users).where(eq(users.id, "u1"));
```

### Joins

All standard join types are supported:

```ts
const result = await db
  .select({ userName: users.name, eventName: events.name })
  .from(users)
  .innerJoin(events, eq(users.id, events.userId));

// Also available: leftJoin, rightJoin, fullJoin, crossJoin
await db.select().from(users).leftJoin(events, eq(users.id, events.userId));
await db.select().from(users).crossJoin(events);
```

### Set operators

```ts
import {
  union,
  unionAll,
  intersect,
  intersectAll,
  except,
  exceptAll,
} from "drizzle-orm-databricks";

const combined = await union(
  db.select({ id: users.id }).from(users),
  db.select({ id: archivedUsers.id }).from(archivedUsers),
);
```

### Common table expressions (CTEs)

Use `$with` and `with` for CTE-based queries on SELECT, INSERT, UPDATE, and DELETE:

```ts
// CTE on SELECT
const adults = db.$with("adults").as(db.select().from(users).where(gt(users.age, 18)));

const result = await db.with(adults).select().from(adults);

// CTE on INSERT...SELECT
await db.with(adults).insert(archive).select(db.select().from(adults));

// CTE on UPDATE
await db.with(adults).update(users).set({ active: true }).where(eq(users.id, "u1"));

// CTE on DELETE
await db.with(adults).delete(users).where(eq(users.id, "u1"));
```

### Aggregates and SQL expressions

```ts
import { sql, count, sum, avg, min, max } from "drizzle-orm";

// Aggregates with GROUP BY and HAVING
const stats = await db
  .select({
    active: users.active,
    total: count(),
    avgAge: avg(users.age),
  })
  .from(users)
  .groupBy(users.active)
  .having(sql`count(*) > 5`);

// Subqueries
const subquery = db
  .select({ id: users.id })
  .from(users)
  .where(eq(users.active, true))
  .as("active_users");

const result = await db.select().from(subquery);

// CASE WHEN
const labeled = await db
  .select({
    id: users.id,
    tier: sql`CASE WHEN ${users.loginCount} > 100 THEN 'power' ELSE 'regular' END`,
  })
  .from(users);
```

### Raw SQL

```ts
// Execute arbitrary SQL
const raw = await db.execute(sql`SELECT COUNT(*) as cnt FROM ${users}`);

// sql.raw() for unparameterised fragments
await db.execute(sql.raw("SHOW TABLES"));

// sql.identifier() for safe identifier quoting
await db.execute(sql`SELECT * FROM ${sql.identifier("my_table")}`);
```

## Connection pooling

For workloads with concurrent queries, enable session pooling to manage multiple `IDBSQLSession` instances backed by a single `DBSQLClient`:

```ts
const db = drizzle({
  host: process.env.DATABRICKS_HOST!,
  path: process.env.DATABRICKS_SQL_PATH!,
  token: process.env.DATABRICKS_TOKEN!,
  pool: {
    max: 5, // max concurrent sessions (default: 10)
    acquireTimeoutMs: 30_000, // timeout waiting for a session (default: 30s)
    sessionMaxAgeMs: 1800000, // recycle sessions after 30 min (default)
  },
});

// Queries automatically acquire and release sessions from the pool
await Promise.all([db.select().from(users), db.select().from(events), db.select().from(logs)]);

// Drains all sessions and closes the client
await db.$close();
```

Sessions are created lazily up to `max`. When all sessions are in use, further requests queue until one is released or `acquireTimeoutMs` elapses. Stale sessions (closed, expired, or older than `sessionMaxAgeMs`) are automatically evicted and replaced.

Without the `pool` option, the adapter uses a single session with automatic stale-session retry (the default for low-concurrency workloads).

## Error handling

Driver errors are wrapped in `DrizzleQueryError` (from `drizzle-orm`) with the SQL string, parameters, and original error attached:

```ts
try {
  await db.execute(sql`SELECT * FROM nonexistent_table`);
} catch (err) {
  if (err instanceof DrizzleQueryError) {
    console.log(err.sql); // the SQL that failed
    console.log(err.params); // bound parameters
    console.log(err.cause); // original @databricks/sql error
  }
}
```

Adapter-specific errors:

| Error class                  | When                                                |
| ---------------------------- | --------------------------------------------------- |
| `DatabricksConnectionError`  | Failed to connect or open a session                 |
| `DatabricksUnsupportedError` | Called an unsupported feature (transactions, etc.)  |
| `PoolError`                  | Pool drained, acquire timeout, or invalid pool size |

## Migrations

```ts
import { migrate } from "drizzle-orm-databricks/migrator";

await migrate(db, { migrationsFolder: "./drizzle" });
```

The migrator tracks applied migrations in a `__drizzle_migrations` Delta table (customisable via `migrationsTable`). Write migration SQL in Spark SQL dialect — drizzle-kit does not yet generate Databricks-compatible DDL.

Example migration file (`drizzle/0001_create_users.sql`):

```sql
CREATE TABLE IF NOT EXISTS users (
  id STRING NOT NULL,
  email STRING NOT NULL,
  login_count BIGINT NOT NULL,
  active BOOLEAN NOT NULL,
  created_at TIMESTAMP NOT NULL
) USING DELTA;
```

## Environment variables

```bash
# Required
DATABRICKS_HOST=adb-1234567890123456.7.azuredatabricks.net
DATABRICKS_SQL_PATH=/sql/1.0/warehouses/abc123

# Auth: provide EITHER a PAT or service principal credentials
DATABRICKS_TOKEN=dapi...
# DATABRICKS_CLIENT_ID=...
# DATABRICKS_CLIENT_SECRET=...

# Optional
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default
```

`DATABRICKS_HOST` is the hostname only — no `https://` prefix.

## Compatibility

| Dependency        | Version  |
| ----------------- | -------- |
| `drizzle-orm`     | `>=0.45` |
| `@databricks/sql` | `>=1.8`  |
| Node.js           | `>=22`   |

Tested on Node 22 and 24. Ships both ESM and CJS with full TypeScript declarations.

## Limitations

- **No `RETURNING` clause.** Databricks does not support RETURNING on INSERT/UPDATE/DELETE. Generate primary keys client-side (UUIDs) and SELECT after insert if needed.
- **No relational queries.** The `db.query` API with `with` relations is not supported. Use query builders or `db.execute()` with joins.
- **No multi-statement transactions.** Databricks provides single-statement atomicity only. Calling `db.session.transaction()` throws `DatabricksUnsupportedError`.
- **No drizzle-kit support.** drizzle-kit does not understand Spark SQL. Write DDL manually and use the built-in migrator.
- **Foreign keys are informational only.** Databricks accepts FK syntax but does not enforce referential integrity.
- **Unique constraints are not enforced.** Databricks accepts UNIQUE syntax but does not enforce uniqueness.
- **No `AUTO_INCREMENT`.** Databricks `IDENTITY` columns disable concurrent writes — use UUIDs.
- **CTE parameter binding.** Ordinal parameter binding across CTE + DML boundaries can misalign on Databricks. Use `sql.raw()` for conditions inside CTEs on UPDATE/DELETE as a workaround.

## Roadmap

### Near-term

- [ ] `bigint`/`number` modes for `decimal` columns
- [ ] CTE parameter binding fix at the dialect level

### Medium-term

- [ ] Relational queries (`db.query` API with `with` relations)
- [ ] Prepared statements
- [ ] `MERGE INTO` (Databricks upsert)
- [ ] `RETURNING` emulation (SELECT-after-write for single-row inserts)

### Long-term

- [ ] drizzle-kit integration (schema push, introspection, migration generation)
- [ ] `ARRAY`, `MAP`, and `STRUCT` column types
- [ ] Databricks `IDENTITY` column support

## Development

```bash
pnpm check         # format (oxfmt) + lint (oxlint)
pnpm test          # unit + integration tests (vitest, mocked @databricks/sql)
pnpm test:e2e      # E2E against a real Databricks SQL warehouse
pnpm test:types    # tsc --noEmit
pnpm test:coverage # v8 coverage report
pnpm build         # ESM + CJS + DTS (vite+)
```

CI runs unit tests on Node 22/24 and E2E tests against a live Databricks warehouse using service principal authentication.

## License

MIT
