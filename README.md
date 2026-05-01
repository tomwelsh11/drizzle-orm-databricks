# drizzle-orm-adapter-databricks

A standalone [Drizzle ORM](https://orm.drizzle.team) adapter for [Databricks SQL](https://www.databricks.com/product/databricks-sql) warehouses. Built from Drizzle's base abstractions with Databricks-native column types and Spark SQL generation, it wraps the official `@databricks/sql` Node.js driver.

## Installation

```bash
pnpm add drizzle-orm-adapter-databricks drizzle-orm @databricks/sql
```

`drizzle-orm` and `@databricks/sql` are peer dependencies — install them in your application.

## Quick start

```ts
import { drizzle, databricksTable, string, bigint, timestamp, boolean } from 'drizzle-orm-adapter-databricks';
import { sql, eq } from 'drizzle-orm';

const users = databricksTable('users', {
  id: string('id').primaryKey(),
  email: string('email').notNull(),
  loginCount: bigint('login_count').notNull(),
  active: boolean('active').notNull(),
  createdAt: timestamp('created_at').notNull(),
});

const db = drizzle({
  host: process.env.DATABRICKS_HOST!,
  path: process.env.DATABRICKS_SQL_PATH!,
  token: process.env.DATABRICKS_TOKEN!,
  catalog: 'main',
  schema: 'analytics',
});

// Execute queries using the sql template tag
const rows = await db.execute(
  sql`SELECT * FROM ${users} WHERE ${users.email} = ${'a@b.com'}`
);

await db.execute(
  sql`INSERT INTO ${users} (${users.id}, ${users.email}, ${users.loginCount}, ${users.active}, ${users.createdAt})
      VALUES (${crypto.randomUUID()}, ${'a@b.com'}, ${0}, ${true}, ${new Date().toISOString()})`
);

await db.$close();
```

## Configuration

### Option 1: connect from credentials

```ts
const db = drizzle({
  host: 'adb-1234567890123456.7.azuredatabricks.net',
  path: '/sql/1.0/warehouses/abc123',
  token: 'dapi...',
  catalog: 'main',     // optional — workspace default if omitted
  schema: 'default',   // optional
});
```

### Option 2: bring your own DBSQLClient

```ts
import { DBSQLClient } from '@databricks/sql';

const client = new DBSQLClient();
await client.connect({ host, path, token });

const db = drizzle({ client, catalog: 'main', schema: 'default' });
```

When you pass an existing client, `db.$close()` will close the active session but leave the client open — you own its lifecycle.

## Databricks-native column types

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
import { databricksSchema } from 'drizzle-orm-adapter-databricks';

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

The migrator records applied migrations in a `__drizzle_migrations` Delta table. Write migration SQL in Spark SQL dialect manually — drizzle-kit does not yet support Databricks.

## Limitations

- **No `RETURNING` clause.** Databricks does not support RETURNING on INSERT/UPDATE/DELETE. Generate primary keys client-side (UUIDs) and SELECT after insert.
- **No multi-statement transactions.** `db.session.transaction()` will throw `DatabricksUnsupportedError`. Databricks provides single-statement atomicity only.
- **No drizzle-kit support.** drizzle-kit does not understand Spark SQL. Write DDL manually.
- **Foreign keys are informational only.** Databricks accepts FK syntax but does not enforce referential integrity.
- **Unique constraints are not enforced.** Databricks accepts UNIQUE syntax but does not enforce uniqueness.
- **No `AUTO_INCREMENT`.** Databricks `IDENTITY` columns disable concurrent writes — use UUIDs.
- **Query builders.** This adapter provides `db.execute()` with the `sql` template tag. Full query builder support (`.select().from()`, `.insert().values()`) is planned for a future release.

## Testing

```bash
pnpm test          # unit tests (mocked @databricks/sql)
pnpm test:e2e      # against a real Databricks SQL warehouse — requires .env
pnpm test:types    # tsc --noEmit
pnpm test:coverage # v8 coverage report
```

## License

MIT
