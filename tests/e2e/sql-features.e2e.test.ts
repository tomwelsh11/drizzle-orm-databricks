import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { closeDb, dropTable, getDb, hasCredentials, uniqueName } from './helpers';

const bt = (n: string) => '`' + n + '`';

describe.skipIf(!hasCredentials())('SQL features & edge cases (e2e)', () => {
  const tableName = uniqueName('feat');
  const tbl = bt(tableName);

  beforeAll(async () => {
    const db = getDb();
    await dropTable(db, tableName);
    await db.execute(
      sql.raw(
        `CREATE TABLE IF NOT EXISTS ${tbl} (
          id INT,
          name STRING,
          category STRING,
          value INT,
          notes STRING
        ) USING DELTA`,
      ),
    );
    await db.execute(
      sql.raw(
        `INSERT INTO ${tbl} VALUES
          (1, 'apple',  'fruit',  10, 'red'),
          (2, 'banana', 'fruit',  20, 'yellow'),
          (3, 'carrot', 'veg',    15, 'orange'),
          (4, 'date',   'fruit',  25, NULL),
          (5, 'endive', 'veg',     5, NULL)`,
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

  it('handles NULL values on insert and select', async () => {
    const db = getDb();
    const rows = await db.execute<{ id: number; notes: string | null }>(
      sql`SELECT id, notes FROM ${sql.raw(tbl)} WHERE notes IS NULL ORDER BY id`,
    );
    expect(rows.length).toBeGreaterThanOrEqual(2);
    expect(rows.every((r) => r.notes === null)).toBe(true);
  });

  it('round-trips strings with special characters', async () => {
    const db = getDb();
    const tricky = "quote ' backtick ` newline \n unicode 🎉";
    await db.execute(
      sql`INSERT INTO ${sql.raw(tbl)} VALUES (${100}, ${tricky}, ${'special'}, ${0}, ${null})`,
    );
    const rows = await db.execute<{ name: string }>(
      sql`SELECT name FROM ${sql.raw(tbl)} WHERE id = ${100}`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]!.name).toBe(tricky);
  });

  it('parameter binding prevents SQL injection', async () => {
    const db = getDb();
    const malicious = `'; DROP TABLE ${tableName}; --`;
    await db.execute(
      sql`INSERT INTO ${sql.raw(tbl)} VALUES (${101}, ${malicious}, ${'attack'}, ${0}, ${null})`,
    );
    const rows = await db.execute<{ name: string }>(
      sql`SELECT name FROM ${sql.raw(tbl)} WHERE id = ${101}`,
    );
    expect(rows).toEqual([{ name: malicious }]);
    const stillThere = await db.execute<{ cnt: number }>(
      sql`SELECT COUNT(*) AS cnt FROM ${sql.raw(tbl)}`,
    );
    expect(Number(stillThere[0]!.cnt)).toBeGreaterThan(0);
  });

  it('runs aggregate functions SUM, AVG, MIN, MAX', async () => {
    const db = getDb();
    const rows = await db.execute<{
      s: number;
      a: number;
      mn: number;
      mx: number;
    }>(
      sql`SELECT SUM(value) AS s, AVG(value) AS a, MIN(value) AS mn, MAX(value) AS mx
          FROM ${sql.raw(tbl)} WHERE id <= ${5}`,
    );
    expect(rows).toHaveLength(1);
    const r = rows[0]!;
    expect(Number(r.s)).toBe(75);
    expect(Number(r.mn)).toBe(5);
    expect(Number(r.mx)).toBe(25);
    expect(Number(r.a)).toBe(15);
  });

  it('groups by a column', async () => {
    const db = getDb();
    const rows = await db.execute<{ category: string; cnt: number }>(
      sql`SELECT category, COUNT(*) AS cnt FROM ${sql.raw(tbl)}
          WHERE id <= ${5}
          GROUP BY category ORDER BY category`,
    );
    expect(rows).toHaveLength(2);
    const byCat = Object.fromEntries(rows.map((r) => [r.category, Number(r.cnt)]));
    expect(byCat['fruit']).toBe(3);
    expect(byCat['veg']).toBe(2);
  });

  it('selects DISTINCT values', async () => {
    const db = getDb();
    const rows = await db.execute<{ category: string }>(
      sql`SELECT DISTINCT category FROM ${sql.raw(tbl)} WHERE id <= ${5} ORDER BY category`,
    );
    const cats = rows.map((r) => r.category);
    expect(cats).toEqual(['fruit', 'veg']);
  });

  it('uses CASE WHEN expressions', async () => {
    const db = getDb();
    const rows = await db.execute<{ id: number; bucket: string }>(
      sql`SELECT id, CASE WHEN value >= ${20} THEN ${'high'}
                          WHEN value >= ${10} THEN ${'mid'}
                          ELSE ${'low'} END AS bucket
          FROM ${sql.raw(tbl)} WHERE id <= ${5} ORDER BY id`,
    );
    const buckets = Object.fromEntries(rows.map((r) => [r.id, r.bucket]));
    expect(buckets[1]).toBe('mid');
    expect(buckets[2]).toBe('high');
    expect(buckets[3]).toBe('mid');
    expect(buckets[4]).toBe('high');
    expect(buckets[5]).toBe('low');
  });

  it('runs a subquery with IN', async () => {
    const db = getDb();
    const rows = await db.execute<{ id: number; name: string }>(
      sql`SELECT id, name FROM ${sql.raw(tbl)}
          WHERE id IN (SELECT id FROM ${sql.raw(tbl)} WHERE category = ${'fruit'} AND id <= ${5})
          ORDER BY id`,
    );
    expect(rows.map((r) => r.id)).toEqual([1, 2, 4]);
  });

  it('combines two SELECTs with UNION', async () => {
    const db = getDb();
    const rows = await db.execute<{ id: number }>(
      sql`SELECT id FROM ${sql.raw(tbl)} WHERE id = ${1}
          UNION
          SELECT id FROM ${sql.raw(tbl)} WHERE id = ${2}
          ORDER BY id`,
    );
    expect(rows.map((r) => r.id)).toEqual([1, 2]);
  });

  it('uses sql.identifier for dynamic table/column names', async () => {
    const db = getDb();
    const rows = await db.execute<{ id: number }>(
      sql`SELECT ${sql.identifier('id')} FROM ${sql.identifier(tableName)} WHERE id = ${1}`,
    );
    expect(rows).toEqual([{ id: 1 }]);
  });

  it('runs raw SQL via sql.raw()', async () => {
    const db = getDb();
    const rows = await db.execute<{ n: number }>(sql.raw('SELECT 42 AS n'));
    expect(rows).toEqual([{ n: 42 }]);
  });

  it('composes a query from multiple sql fragments', async () => {
    const db = getDb();
    const select = sql`SELECT id, name`;
    const from = sql`FROM ${sql.raw(tbl)}`;
    const where = sql`WHERE id = ${1}`;
    const rows = await db.execute<{ id: number; name: string }>(
      sql`${select} ${from} ${where}`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]!.id).toBe(1);
  });

  it('round-trips a 10,000 character string', async () => {
    const db = getDb();
    const big = 'x'.repeat(10000);
    await db.execute(
      sql`INSERT INTO ${sql.raw(tbl)} VALUES (${200}, ${'big'}, ${'big'}, ${0}, ${big})`,
    );
    const rows = await db.execute<{ notes: string }>(
      sql`SELECT notes FROM ${sql.raw(tbl)} WHERE id = ${200}`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]!.notes.length).toBe(10000);
    expect(rows[0]!.notes).toBe(big);
  });

  it('selects with LIMIT and OFFSET', async () => {
    const db = getDb();
    const rows = await db.execute<{ id: number }>(
      sql`SELECT id FROM ${sql.raw(tbl)} WHERE id <= ${5} ORDER BY id LIMIT ${2} OFFSET ${2}`,
    );
    expect(rows.map((r) => r.id)).toEqual([3, 4]);
  });
});
