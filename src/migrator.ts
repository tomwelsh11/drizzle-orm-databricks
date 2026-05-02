import { sql } from "drizzle-orm";
import type { MigrationConfig } from "drizzle-orm/migrator";
import { readMigrationFiles } from "drizzle-orm/migrator";

import type { DatabricksDatabase } from "./driver";

export async function migrate<TSchema extends Record<string, unknown>>(
  db: DatabricksDatabase<TSchema>,
  config: MigrationConfig,
): Promise<void> {
  const migrations = readMigrationFiles(config);
  const migrationsTable = config.migrationsTable ?? "__drizzle_migrations";

  await db.execute(sql`
    CREATE TABLE IF NOT EXISTS ${sql.identifier(migrationsTable)} (
      hash STRING NOT NULL,
      created_at BIGINT NOT NULL
    ) USING DELTA
  `);

  const dbMigrations = (await db.execute(sql`
    SELECT hash, created_at FROM ${sql.identifier(migrationsTable)}
    ORDER BY created_at DESC LIMIT 1
  `)) as unknown as Array<{ hash: string; created_at: number | string }>;

  const last = dbMigrations[0];
  const lastMillis = last ? Number(last.created_at) : 0;

  for (const migration of migrations) {
    if (migration.folderMillis <= lastMillis) continue;

    for (const stmt of migration.sql) {
      await db.execute(sql.raw(stmt));
    }

    await db.execute(sql`
      INSERT INTO ${sql.identifier(migrationsTable)} (hash, created_at)
      VALUES (${migration.hash}, CAST(${String(migration.folderMillis)} AS BIGINT))
    `);
  }
}
