import { sql } from 'drizzle-orm';
import { afterAll, describe, expect, it } from 'vitest';

import { DatabricksUnsupportedError } from '../../src/errors';
import { closeDb, dropTable, getDb, hasCredentials, uniqueName } from './helpers';

describe.skipIf(!hasCredentials())('Error handling (e2e)', () => {
  const createdTables: string[] = [];

  afterAll(async () => {
    const db = getDb();
    for (const name of createdTables) {
      try {
        await dropTable(db, name);
      } catch {
        // best-effort
      }
    }
    await closeDb();
  });

  it('throws on invalid SQL syntax', async () => {
    const db = getDb();
    await expect(db.execute(sql.raw('SELECTTTT 1'))).rejects.toThrow();
  });

  it('throws when querying a non-existent table', async () => {
    const db = getDb();
    const missing = uniqueName('does_not_exist');
    await expect(
      db.execute(sql.raw(`SELECT * FROM \`${missing}\``)),
    ).rejects.toThrow();
  });

  it('errors or coerces when inserting wrong-typed parameter', async () => {
    const db = getDb();
    const tableName = uniqueName('typecheck');
    createdTables.push(tableName);

    await db.execute(
      sql.raw(`CREATE TABLE IF NOT EXISTS \`${tableName}\` (id INT) USING DELTA`),
    );

    let threw = false;
    let inserted: unknown;
    try {
      await db.execute(
        sql.raw(`INSERT INTO \`${tableName}\` (id) VALUES ('not-an-int')`),
      );
      const rows = (await db.execute(
        sql.raw(`SELECT id FROM \`${tableName}\``),
      )) as unknown as Array<{ id: unknown }>;
      inserted = rows[0]?.id;
    } catch {
      threw = true;
    }

    if (!threw) {
      expect(inserted === null || typeof inserted === 'number').toBe(true);
    } else {
      expect(threw).toBe(true);
    }
  });

  it('throws DatabricksUnsupportedError for transactions', async () => {
    const db = getDb();
    await expect(
      (db as unknown as { session: { transaction: (fn: () => Promise<unknown>) => Promise<unknown> } })
        .session.transaction(async () => undefined),
    ).rejects.toBeInstanceOf(DatabricksUnsupportedError);
  });

  it('returns NULL for division by zero rather than throwing', async () => {
    const db = getDb();
    const rows = (await db.execute(
      sql.raw('SELECT 1/0 AS result'),
    )) as unknown as Array<{ result: unknown }>;
    expect(rows).toHaveLength(1);
    expect(rows[0]!.result).toBeNull();
  });

  it('throws on duplicate column name in CREATE TABLE', async () => {
    const db = getDb();
    const tableName = uniqueName('dupcol');
    createdTables.push(tableName);
    await expect(
      db.execute(
        sql.raw(
          `CREATE TABLE \`${tableName}\` (id INT, id INT) USING DELTA`,
        ),
      ),
    ).rejects.toThrow();
  });
});
