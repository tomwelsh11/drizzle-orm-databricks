import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { sql } from 'drizzle-orm';
import { afterAll, describe, expect, it } from 'vitest';

import { migrate } from '../../src/migrator';
import { closeDb, dropTable, getDb, hasCredentials } from './helpers';

type JournalEntry = {
  idx: number;
  version: string;
  when: number;
  tag: string;
  breakpoints: boolean;
};

function makeMigrationsDir(
  entries: Array<{ tag: string; when: number; sql: string }>,
): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'drizzle-databricks-mig-'));
  fs.mkdirSync(path.join(dir, 'meta'));

  const journal: { version: string; dialect: string; entries: JournalEntry[] } = {
    version: '7',
    dialect: 'postgresql',
    entries: entries.map((entry, idx) => ({
      idx,
      version: '7',
      when: entry.when,
      tag: entry.tag,
      breakpoints: true,
    })),
  };

  fs.writeFileSync(
    path.join(dir, 'meta', '_journal.json'),
    JSON.stringify(journal, null, 2),
  );

  for (const entry of entries) {
    fs.writeFileSync(path.join(dir, `${entry.tag}.sql`), entry.sql);
  }

  return dir;
}

function cleanup(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true });
}

describe.skipIf(!hasCredentials())('Migrator (e2e)', () => {
  const createdTables: string[] = [];
  const createdMigrationsTables: string[] = [];
  const tempDirs: string[] = [];

  afterAll(async () => {
    const db = getDb();
    for (const name of createdTables) {
      try {
        await dropTable(db, name);
      } catch {
        // best-effort
      }
    }
    for (const name of createdMigrationsTables) {
      try {
        await dropTable(db, name);
      } catch {
        // best-effort
      }
    }
    for (const dir of tempDirs) {
      cleanup(dir);
    }
    await closeDb();
  });

  it('creates the migration tracking table when there are no migrations', async () => {
    const db = getDb();
    const migrationsTable = 'mig_track';
    createdMigrationsTables.push(migrationsTable);

    const dir = makeMigrationsDir([]);
    tempDirs.push(dir);

    await migrate(db, { migrationsFolder: dir, migrationsTable });

    const rows = (await db.execute(
      sql`SELECT hash, created_at FROM ${sql.identifier(migrationsTable)}`,
    )) as unknown as Array<{ hash: string; created_at: number | string }>;
    expect(rows).toEqual([]);
  });

  it('applies a migration from disk', async () => {
    const db = getDb();
    const migrationsTable = 'mig_apply';
    const tableName = 'migrated';
    createdMigrationsTables.push(migrationsTable);
    createdTables.push(tableName);

    const dir = makeMigrationsDir([
      {
        tag: '0000_init',
        when: 1700000000000,
        sql: `CREATE TABLE IF NOT EXISTS \`${tableName}\` (id INT, name STRING) USING DELTA;`,
      },
    ]);
    tempDirs.push(dir);

    await migrate(db, { migrationsFolder: dir, migrationsTable });

    const tables = (await db.execute(
      sql.raw(`SHOW TABLES LIKE '${tableName}'`),
    )) as unknown as Array<Record<string, unknown>>;
    expect(tables.length).toBeGreaterThan(0);

    const recorded = (await db.execute(
      sql`SELECT hash, created_at FROM ${sql.identifier(migrationsTable)}`,
    )) as unknown as Array<{ hash: string; created_at: number | string }>;
    expect(recorded).toHaveLength(1);
    expect(Number(recorded[0]!.created_at)).toBe(1700000000000);
  });

  it('is idempotent across repeated runs', async () => {
    const db = getDb();
    const migrationsTable = 'mig_idem';
    const tableName = 'idem';
    createdMigrationsTables.push(migrationsTable);
    createdTables.push(tableName);

    const dir = makeMigrationsDir([
      {
        tag: '0000_init',
        when: 1700000000001,
        sql: `CREATE TABLE IF NOT EXISTS \`${tableName}\` (id INT) USING DELTA;`,
      },
    ]);
    tempDirs.push(dir);

    await migrate(db, { migrationsFolder: dir, migrationsTable });
    await migrate(db, { migrationsFolder: dir, migrationsTable });

    const recorded = (await db.execute(
      sql`SELECT hash, created_at FROM ${sql.identifier(migrationsTable)}`,
    )) as unknown as Array<{ hash: string; created_at: number | string }>;
    expect(recorded).toHaveLength(1);
  });

  it('applies multiple migrations in order', async () => {
    const db = getDb();
    const migrationsTable = 'mig_multi';
    const tableA = 'multi_a';
    const tableB = 'multi_b';
    createdMigrationsTables.push(migrationsTable);
    createdTables.push(tableA, tableB);

    const dir = makeMigrationsDir([
      {
        tag: '0000_first',
        when: 1700000000100,
        sql: `CREATE TABLE IF NOT EXISTS \`${tableA}\` (id INT) USING DELTA;`,
      },
      {
        tag: '0001_second',
        when: 1700000000200,
        sql: `CREATE TABLE IF NOT EXISTS \`${tableB}\` (id INT) USING DELTA;`,
      },
    ]);
    tempDirs.push(dir);

    await migrate(db, { migrationsFolder: dir, migrationsTable });

    const recorded = (await db.execute(
      sql`SELECT hash, created_at FROM ${sql.identifier(migrationsTable)} ORDER BY created_at ASC`,
    )) as unknown as Array<{ hash: string; created_at: number | string }>;
    expect(recorded).toHaveLength(2);
    expect(Number(recorded[0]!.created_at)).toBe(1700000000100);
    expect(Number(recorded[1]!.created_at)).toBe(1700000000200);

    const tablesA = (await db.execute(
      sql.raw(`SHOW TABLES LIKE '${tableA}'`),
    )) as unknown as Array<Record<string, unknown>>;
    const tablesB = (await db.execute(
      sql.raw(`SHOW TABLES LIKE '${tableB}'`),
    )) as unknown as Array<Record<string, unknown>>;
    expect(tablesA.length).toBeGreaterThan(0);
    expect(tablesB.length).toBeGreaterThan(0);
  });
});
