# drizzle-orm-adapter-databricks

A standalone [Drizzle ORM](https://orm.drizzle.team) adapter for [Databricks SQL](https://www.databricks.com/product/databricks-sql) warehouses. Built from Drizzle's base abstractions — **zero dependency on `mysql-core`, `pg-core`, or `sqlite-core`** — with Databricks-native column types, Spark SQL generation, and the official `@databricks/sql` Node.js driver.

## Installation

```bash
pnpm add drizzle-orm-adapter-databricks drizzle-orm @databricks/sql
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
} from "drizzle-orm-adapter-databricks";
import { sql } from "drizzle-orm";

// Define tables with Databricks-native column types
const users = databricksTable("users", {
  id: string("id").primaryKey(),
  email: string("email").notNull(),
  loginCount: bigint("login_count").notNull(),
  active: boolean("active").notNull(),
  createdAt: timestamp("created_at").notNull(),
});

// Connect with PAT or service principal
const db = drizzle({
  host: process.env.DATABRICKS_HOST!,
  path: process.env.DATABRICKS_SQL_PATH!,
  token: process.env.DATABRICKS_TOKEN!,
});

// Query builders — type-safe select, insert, update, delete
import { eq } from "drizzle-orm";

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
} from "drizzle-orm-adapter-databricks";

export const events = databricksTable("events", {
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

## Schema-qualified tables

```ts
import { databricksSchema, string } from "drizzle-orm-adapter-databricks";

const analytics = databricksSchema("analytics");

const events = analytics.table("events", {
  id: string("id").primaryKey(),
  name: string("name").notNull(),
});
```

## Query builders

Full Drizzle query builder support with type-safe select, insert, update, and delete:

```ts
import { eq, and, gt, desc, sql } from "drizzle-orm";

// Select with where, order, limit, offset
const activeUsers = await db
  .select()
  .from(users)
  .where(eq(users.active, true))
  .orderBy(desc(users.createdAt))
  .limit(10);

// Partial select — only the columns you need
const names = await db
  .select({ id: users.id, email: users.email })
  .from(users)
  .where(gt(users.loginCount, BigInt(10)));

// Insert single or multiple rows
await db.insert(users).values({
  id: "u1",
  email: "a@b.com",
  loginCount: BigInt(0),
  active: true,
  createdAt: new Date(),
});
await db.insert(users).values([
  { id: "u2", email: "b@c.com", loginCount: BigInt(0), active: true, createdAt: new Date() },
  { id: "u3", email: "c@d.com", loginCount: BigInt(0), active: false, createdAt: new Date() },
]);

// Update with where
await db.update(users).set({ active: false }).where(eq(users.id, "u1"));

// Delete with where
await db.delete(users).where(eq(users.id, "u1"));

// Joins
const result = await db
  .select({ userName: users.email, eventName: events.name })
  .from(users)
  .innerJoin(events, eq(users.id, events.id));

// Select distinct
const uniqueNames = await db.selectDistinct({ email: users.email }).from(users);

// You can still use db.execute() with the sql template tag for raw queries
const raw = await db.execute(sql`SELECT COUNT(*) as cnt FROM ${users}`);
```

## Migrations

```ts
import { migrate } from "drizzle-orm-adapter-databricks/migrator";

await migrate(db, { migrationsFolder: "./drizzle" });
```

The migrator tracks applied migrations in a `__drizzle_migrations` Delta table. Write migration SQL in Spark SQL dialect — drizzle-kit does not yet support Databricks.

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

Note: `DATABRICKS_HOST` is the hostname only — no `https://` prefix.

## Compatibility

| Dependency        | Version  |
| ----------------- | -------- |
| `drizzle-orm`     | `>=0.45` |
| `@databricks/sql` | `>=1.8`  |
| Node.js           | `>=22`   |

Tested on Node 22 and 24.

### What works

- **Query builders** — `select`, `selectDistinct`, `insert` (single & batch), `update`, `delete`
- **All join types** — `innerJoin`, `leftJoin`, `rightJoin`, `fullJoin` with WHERE, ORDER BY, LIMIT
- **Set operators** — `union`, `unionAll`, `intersect`, `intersectAll`, `except`, `exceptAll`
- **SQL features** — aggregates (SUM/AVG/MIN/MAX/COUNT), GROUP BY, HAVING, subqueries, CASE WHEN, LIMIT/OFFSET
- **Authentication** — PAT, OAuth M2M (service principal), bring-your-own `DBSQLClient`
- **Column types** — all 16 Spark SQL types (STRING through VARIANT)
- **Schema-qualified tables** — `databricksSchema("name").table(...)`
- **INSERT INTO ... SELECT** — compose inserts from subqueries
- **WITH (CTEs) on DML** — use CTEs with INSERT, UPDATE, DELETE
- **CROSS JOIN** — `crossJoin()` support
- **DrizzleQueryError** — wraps driver errors with SQL string, params, and stack traces
- **Migrations** — `migrate()` with Delta table tracking
- **Raw SQL** — `db.execute(sql`...`)`, `sql.raw()`, `sql.identifier()`
- **Session management** — lazy connection, automatic stale session retry, clean shutdown

### Limitations

- **No `RETURNING` clause.** Databricks does not support RETURNING on INSERT/UPDATE/DELETE. Generate primary keys client-side (UUIDs) and SELECT after insert.
- **No relational queries.** The `db.query` API with `with` relations is not supported. Use query builders or `db.execute()` with joins.
- **No multi-statement transactions.** `db.session.transaction()` throws `DatabricksUnsupportedError`. Databricks provides single-statement atomicity only.
- **No drizzle-kit support.** drizzle-kit does not understand Spark SQL. Write DDL manually.
- **Foreign keys are informational only.** Databricks accepts FK syntax but does not enforce referential integrity.
- **Unique constraints are not enforced.** Databricks accepts UNIQUE syntax but does not enforce uniqueness.
- **No `AUTO_INCREMENT`.** Databricks `IDENTITY` columns disable concurrent writes — use UUIDs.

## Roadmap

### Shipped

- [x] `INSERT INTO ... SELECT` — compose inserts from subqueries
- [x] `WITH` (CTEs) on DML — use CTEs with INSERT, UPDATE, DELETE
- [x] `CROSS JOIN` support
- [x] `DrizzleQueryError` wrapper — surface SQL string, params, and stack traces on driver errors

### In progress

- [ ] Unity Catalog 3-tier namespace — `databricksCatalog("cat").schema("sch").table(...)` and per-query overrides (#7)
- [ ] Connection pooling and multi-session support (#6)

### Near-term

- [ ] `bigint`/`number` modes for `decimal` columns (available since drizzle-orm 0.41.0)
- [ ] CTE parameter binding fix — ordinal params across CTE + DML boundaries on Databricks

### Medium-term

- [ ] Relational queries (`db.query` API with `with` relations)
- [ ] Prepared statements
- [ ] `MERGE INTO` (Databricks upsert)
- [ ] `RETURNING` emulation (SELECT-after-write for single-row inserts)

### Long-term

- [ ] drizzle-kit integration (schema push, introspection, migration generation)
- [ ] `ARRAY`, `MAP`, and `STRUCT` column types
- [ ] Databricks `IDENTITY` column support

## Testing

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
