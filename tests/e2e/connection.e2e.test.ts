import { DBSQLClient } from '@databricks/sql';
import { sql } from 'drizzle-orm';
import { afterAll, describe, expect, it } from 'vitest';

import { drizzle } from '../../src/driver';
import { closeDb, getDb, hasCredentials } from './helpers';

describe.skipIf(!hasCredentials())('connection lifecycle (e2e)', () => {
  afterAll(async () => {
    await closeDb();
  });

  it('runs a simple SELECT 1 query', async () => {
    const db = getDb();
    const rows = await db.execute<{ one: number }>(sql`SELECT 1 AS one`);
    expect(rows).toEqual([{ one: 1 }]);
  });

  it('accepts a bring-your-own DBSQLClient instance', async () => {
    const host = process.env['DATABRICKS_HOST']!;
    const path = process.env['DATABRICKS_SQL_PATH']!;
    const token = process.env['DATABRICKS_TOKEN']!;

    const client = new DBSQLClient();
    await client.connect({ host, path, token });

    const db2 = drizzle({
      client,
      catalog: process.env['DATABRICKS_CATALOG'],
      schema: process.env['DATABRICKS_SCHEMA'],
    });

    try {
      const rows = await db2.execute<{ one: number }>(sql`SELECT 1 AS one`);
      expect(rows).toEqual([{ one: 1 }]);

      await db2.$close();

      const rowsAfterClose = await client
        .openSession()
        .then(async (session) => {
          const op = await session.executeStatement('SELECT 2 AS two');
          const result = await op.fetchAll();
          await op.close();
          await session.close();
          return result as Array<{ two: number }>;
        });
      expect(rowsAfterClose).toEqual([{ two: 2 }]);
    } finally {
      await client.close();
    }
  });

  it('returns the configured catalog from current_catalog()', async () => {
    const db = getDb();
    const expected = process.env['DATABRICKS_CATALOG'];
    const rows = await db.execute<Record<string, string>>(sql`SELECT current_catalog() AS catalog`);
    expect(rows).toHaveLength(1);
    if (expected) {
      expect(rows[0]!['catalog']).toBe(expected);
    } else {
      expect(typeof rows[0]!['catalog']).toBe('string');
    }
  });

  it('reuses the session across multiple queries', async () => {
    const db = getDb();
    const r1 = await db.execute<{ n: number }>(sql`SELECT 1 AS n`);
    const r2 = await db.execute<{ n: number }>(sql`SELECT 2 AS n`);
    const r3 = await db.execute<{ n: number }>(sql`SELECT 3 AS n`);
    expect(r1).toEqual([{ n: 1 }]);
    expect(r2).toEqual([{ n: 2 }]);
    expect(r3).toEqual([{ n: 3 }]);
  });

  it('can close and reconstruct a db instance', async () => {
    const host = process.env['DATABRICKS_HOST']!;
    const path = process.env['DATABRICKS_SQL_PATH']!;
    const token = process.env['DATABRICKS_TOKEN']!;
    const catalog = process.env['DATABRICKS_CATALOG'];
    const schema = process.env['DATABRICKS_SCHEMA'];

    const first = drizzle({ host, path, token, catalog, schema });
    const r1 = await first.execute<{ n: number }>(sql`SELECT 1 AS n`);
    expect(r1).toEqual([{ n: 1 }]);
    await first.$close();

    const second = drizzle({ host, path, token, catalog, schema });
    try {
      const r2 = await second.execute<{ n: number }>(sql`SELECT 1 AS n`);
      expect(r2).toEqual([{ n: 1 }]);
    } finally {
      await second.$close();
    }
  });
});
