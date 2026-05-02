# drizzle-orm-adapter-databricks

A standalone [Drizzle ORM](https://orm.drizzle.team) adapter for [Databricks SQL](https://www.databricks.com/product/databricks-sql) warehouses. Built from Drizzle's base abstractions — **zero dependency on `mysql-core`, `pg-core`, or `sqlite-core`** — with Databricks-native column types, Spark SQL generation, and the official `@databricks/sql` Node.js driver.

## Installation

```bash
pnpm add drizzle-orm-adapter-databricks drizzle-orm @databricks/sql
```

`drizzle-orm` and `@databricks/sql` are peer dependencies.

## Quick start

```ts
import { drizzle, databricksTable, string, bigint, timestamp, boolean } from 'drizzle-orm-adapter-databricks';
import { sql } from 'drizzle-orm';

// Define tables with Databricks-native column types
const users = databricksTable('users', {
  id: string('id').primaryKey(),
  email: string('email').notNull(),
  loginCount: bigint('login_count').notNull(),
  active: boolean('active').notNull(),
  createdAt: timestamp('created_at').notNull(),
});

// Connect with PAT or service principal
const db = drizzle({
  host: process.env.DATABRICKS_HOST!,
  path: process.env.DATABRICKS_SQL_PATH!,
  token: process.env.DATABRICKS_TOKEN!,
});

// Type-safe queries using table and column references
const rows = await db.execute<{ id: string; email: string }>(
  sql`SELECT ${users.id}, ${users.email} FROM ${users} WHERE ${users.active} = ${true}`
);

// Parameterised inserts — safe from SQL injection
await db.execute(
  sql`INSERT INTO ${users} (${users.id}, ${users.email}, ${users.loginCount}, ${users.active}, ${users.createdAt})
      VALUES (${crypto.randomUUID()}, ${'a@b.com'}, ${0}, ${true}, ${new Date().toISOString()})`
);

// Updates and deletes use the same column references
await db.execute(
  sql`UPDATE ${users} SET ${users.active} = ${false} WHERE ${users.id} = ${'u1'}`
);

await db.$close();
```

## Authentication

### Personal access token (PAT)

```ts
const db = drizzle({
  host: 'adb-1234567890123456.7.azuredatabricks.net',
  path: '/sql/1.0/warehouses/abc123',
  token: 'dapi...',
  catalog: 'main',     // optional — workspace default if omitted
  schema: 'default',   // optional
});
```

### Service principal (OAuth M2M)

```ts
const db = drizzle({
  host: 'adb-1234567890123456.7.azuredatabricks.net',
  path: '/sql/1.0/warehouses/abc123',
  clientId: process.env.DATABRICKS_CLIENT_ID!,
  clientSecret: process.env.DATABRICKS_CLIENT_SECRET!,
  catalog: 'main',
  schema: 'default',
});
```

Uses Databricks OAuth machine-to-machine flow. Register a service principal in your workspace and grant it SQL warehouse access.

### Bring your own DBSQLClient

```ts
import { DBSQLClient } from '@databricks/sql';

const client = new DBSQLClient();
await client.connect({ host, path, token });

const db = drizzle({ client, catalog: 'main', schema: 'default' });
```

When you pass an existing client, `db.$close()` closes the session but leaves the client open — you own its lifecycle.

## Column types

All column types map directly to Spark SQL types:

```ts
import {
  databricksTable,
  string, varchar, char,
  int, bigint, smallint, tinyint,
  float, double, decimal,
  boolean, date, timestamp, timestampNtz,
  binary, variant,
} from 'drizzle-orm-adapter-databricks';

export const events = databricksTable('events', {
  id: string('id').primaryKey(),
  name: varchar('name', { length: 255 }).notNull(),
  count: bigint('count').notNull(),
  score: double('score'),
  price: decimal('price', { precision: 18, scale: 6 }),
  active: boolean('active').notNull(),
  occurredAt: timestamp('occurred_at').notNull(),
  createdDate: date('created_date'),
  metadata: variant('metadata'),
});
```

| Column function | Spark SQL type | JS type |
|---|---|---|
| `string()` | `STRING` | `string` |
| `varchar({ length })` | `VARCHAR(n)` | `string` |
| `char({ length })` | `CHAR(n)` | `string` |
| `int()` | `INT` | `number` |
| `bigint()` | `BIGINT` | `bigint` |
| `smallint()` | `SMALLINT` | `number` |
| `tinyint()` | `TINYINT` | `number` |
| `float()` | `FLOAT` | `number` |
| `double()` | `DOUBLE` | `number` |
| `decimal({ precision, scale })` | `DECIMAL(p, s)` | `string` |
| `boolean()` | `BOOLEAN` | `boolean` |
| `date()` | `DATE` | `Date` |
| `timestamp()` | `TIMESTAMP` | `Date` |
| `timestampNtz()` | `TIMESTAMP_NTZ` | `Date` |
| `binary()` | `BINARY` | `Uint8Array` |
| `variant()` | `VARIANT` | `unknown` (JSON) |

## Schema-qualified tables

```ts
import { databricksSchema, string } from 'drizzle-orm-adapter-databricks';

const analytics = databricksSchema('analytics');

const events = analytics.table('events', {
  id: string('id').primaryKey(),
  name: string('name').notNull(),
});
```

## Migrations

```ts
import { migrate } from 'drizzle-orm-adapter-databricks/migrator';

await migrate(db, { migrationsFolder: './drizzle' });
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

## Limitations

- **No query builders yet.** This release provides `db.execute()` with Drizzle's `sql` template tag for type-safe table/column references and parameterised queries. Full query builder support (`.select().from()`, `.insert().values()`) is planned for the next release.
- **No `RETURNING` clause.** Databricks does not support RETURNING on INSERT/UPDATE/DELETE. Generate primary keys client-side (UUIDs) and SELECT after insert.
- **No multi-statement transactions.** `db.session.transaction()` throws `DatabricksUnsupportedError`. Databricks provides single-statement atomicity only.
- **No drizzle-kit support.** drizzle-kit does not understand Spark SQL. Write DDL manually.
- **Foreign keys are informational only.** Databricks accepts FK syntax but does not enforce referential integrity.
- **Unique constraints are not enforced.** Databricks accepts UNIQUE syntax but does not enforce uniqueness.
- **No `AUTO_INCREMENT`.** Databricks `IDENTITY` columns disable concurrent writes — use UUIDs.

## Testing

```bash
pnpm test          # 101 unit tests (mocked @databricks/sql)
pnpm test:e2e      # E2E against a real Databricks SQL warehouse
pnpm test:types    # tsc --noEmit
pnpm test:coverage # v8 coverage report
```

CI runs unit tests on Node 18/20/22 and E2E tests against a live Databricks warehouse using service principal authentication.

## License

MIT
