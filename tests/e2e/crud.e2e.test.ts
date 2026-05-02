import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { closeDb, dropTable, getDb, hasCredentials, uniqueName } from './helpers';

const bt = (n: string) => '`' + n + '`';

describe.skipIf(!hasCredentials())('CRUD operations (e2e)', () => {
  const tableName = uniqueName('crud');
  const tbl = bt(tableName);

  beforeAll(async () => {
    const db = getDb();
    await dropTable(db, tableName);
    await db.execute(
      sql.raw(
        `CREATE TABLE IF NOT EXISTS ${tbl} (
          id INT,
          name STRING,
          email STRING,
          age INT,
          active BOOLEAN
        ) USING DELTA`,
      ),
    );
  });

  afterAll(async () => {
    try {
      await dropTable(getDb(), tableName);
    } finally {
      await closeDb();
    }
  });

  it('inserts a single row and selects it back', async () => {
    const db = getDb();
    await db.execute(
      sql`INSERT INTO ${sql.raw(tbl)} VALUES (${1}, ${'Alice'}, ${'alice@example.com'}, ${30}, ${true})`,
    );
    const rows = await db.execute<{
      id: number;
      name: string;
      email: string;
      age: number;
      active: boolean;
    }>(sql`SELECT id, name, email, age, active FROM ${sql.raw(tbl)} WHERE id = ${1}`);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual({
      id: 1,
      name: 'Alice',
      email: 'alice@example.com',
      age: 30,
      active: true,
    });
  });

  it('inserts multiple rows in one statement', async () => {
    const db = getDb();
    await db.execute(
      sql.raw(
        `INSERT INTO ${tbl} VALUES
          (2, 'Bob',     'bob@example.com',     22, true),
          (3, 'Carol',   'carol@example.com',   45, false),
          (4, 'Dave',    'dave@example.com',    33, true)`,
      ),
    );
    const rows = await db.execute<{ cnt: number }>(
      sql`SELECT COUNT(*) AS cnt FROM ${sql.raw(tbl)}`,
    );
    expect(Number(rows[0]!.cnt)).toBe(4);
  });

  it('selects with parameterised WHERE', async () => {
    const db = getDb();
    const rows = await db.execute<{ id: number; name: string; age: number }>(
      sql`SELECT id, name, age FROM ${sql.raw(tbl)} WHERE age > ${25} ORDER BY id`,
    );
    const ages = rows.map((r) => r.age);
    expect(ages.every((a) => a > 25)).toBe(true);
    expect(rows.length).toBeGreaterThanOrEqual(3);
  });

  it('selects with ORDER BY and LIMIT', async () => {
    const db = getDb();
    const rows = await db.execute<{ id: number; age: number }>(
      sql`SELECT id, age FROM ${sql.raw(tbl)} ORDER BY age DESC LIMIT ${2}`,
    );
    expect(rows).toHaveLength(2);
    expect(rows[0]!.age).toBeGreaterThanOrEqual(rows[1]!.age);
  });

  it('selects with LIKE pattern', async () => {
    const db = getDb();
    const rows = await db.execute<{ name: string }>(
      sql`SELECT name FROM ${sql.raw(tbl)} WHERE name LIKE ${'A%'}`,
    );
    expect(rows.length).toBeGreaterThanOrEqual(1);
    expect(rows.every((r) => r.name.startsWith('A'))).toBe(true);
  });

  it('returns a count', async () => {
    const db = getDb();
    const rows = await db.execute<{ cnt: number }>(
      sql`SELECT COUNT(*) AS cnt FROM ${sql.raw(tbl)}`,
    );
    expect(rows).toHaveLength(1);
    expect(Number(rows[0]!.cnt)).toBeGreaterThanOrEqual(4);
  });

  it('updates a row and reads back the change', async () => {
    const db = getDb();
    await db.execute(
      sql`UPDATE ${sql.raw(tbl)} SET name = ${'Alicia'} WHERE id = ${1}`,
    );
    const rows = await db.execute<{ name: string }>(
      sql`SELECT name FROM ${sql.raw(tbl)} WHERE id = ${1}`,
    );
    expect(rows).toEqual([{ name: 'Alicia' }]);
  });

  it('deletes a row and verifies it is gone', async () => {
    const db = getDb();
    await db.execute(sql`DELETE FROM ${sql.raw(tbl)} WHERE id = ${4}`);
    const rows = await db.execute<{ id: number }>(
      sql`SELECT id FROM ${sql.raw(tbl)} WHERE id = ${4}`,
    );
    expect(rows).toEqual([]);
  });

  it('returns an empty array for an impossible WHERE', async () => {
    const db = getDb();
    const rows = await db.execute(
      sql`SELECT * FROM ${sql.raw(tbl)} WHERE id = ${-99999}`,
    );
    expect(rows).toEqual([]);
  });

  it('selects with multiple parameters combined via AND', async () => {
    const db = getDb();
    const rows = await db.execute<{ id: number; name: string; age: number }>(
      sql`SELECT id, name, age FROM ${sql.raw(tbl)} WHERE age > ${20} AND active = ${true} ORDER BY id`,
    );
    expect(rows.length).toBeGreaterThanOrEqual(1);
    expect(rows.every((r) => r.age > 20)).toBe(true);
  });
});
