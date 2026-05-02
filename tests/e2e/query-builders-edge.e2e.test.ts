import {
  and,
  asc,
  between,
  desc,
  eq,
  gt,
  gte,
  inArray,
  isNotNull,
  isNull,
  like,
  lt,
  lte,
  ne,
  notInArray,
  or,
  sql,
} from 'drizzle-orm';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { boolean, databricksTable, double, int, string } from '../../src';
import { closeDb, dropTable, getDb, hasCredentials, uniqueName } from './helpers';

const tableName = uniqueName('qb_edge');

const items = databricksTable(tableName, {
  id: string('id'),
  name: string('name'),
  value: int('value'),
  active: boolean('active'),
  score: double('score'),
});

const bt = (n: string) => '`' + n + '`';

const SEED_IDS = ['s01', 's02', 's03', 's04', 's05', 's06', 's07', 's08'] as const;

describe.skipIf(!hasCredentials())('Query builder edge cases (e2e)', () => {
  beforeAll(async () => {
    const db = getDb();
    await dropTable(db, tableName);
    await db.execute(
      sql.raw(
        `CREATE TABLE IF NOT EXISTS ${bt(tableName)} (
          id STRING,
          name STRING,
          value INT,
          active BOOLEAN,
          score DOUBLE
        ) USING DELTA`,
      ),
    );
    // Seed 8 rows with diverse data: NULLs, empty string, zero, negative, large numbers.
    await db.execute(
      sql.raw(
        `INSERT INTO ${bt(tableName)} (id, name, value, active, score) VALUES
          ('s01', 'alpha',   10,           true,  1.5),
          ('s02', '',        0,            false, 0.0),
          ('s03', 'gamma',   -5,           true,  -2.25),
          ('s04', NULL,      NULL,         NULL,  NULL),
          ('s05', 'delta',   2147483000,   true,  1.7976931348623157e+100),
          ('s06', 'epsilon', -2147483000,  false, -3.14159265358979),
          ('s07', 'zeta',    100,          true,  99.99),
          ('s08', 'eta',     50,           false, 50.5)`,
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

  // 1. Empty result from SELECT
  it('returns an empty array when WHERE matches nothing', async () => {
    const db = getDb();
    const rows = await db.select().from(items).where(eq(items.id, 'no-such-id'));
    expect(rows).toEqual([]);
  });

  // 2. NULL handling in WHERE — isNull / isNotNull
  it('isNull / isNotNull select NULL rows correctly', async () => {
    const db = getDb();
    const nullRows = await db.select({ id: items.id }).from(items).where(isNull(items.name));
    expect(nullRows).toHaveLength(1);
    expect(nullRows[0]!.id).toBe('s04');

    const notNullRows = await db.select({ id: items.id }).from(items).where(isNotNull(items.name));
    expect(notNullRows).toHaveLength(7);
    expect(notNullRows.every((r) => r.id !== 's04')).toBe(true);
  });

  // 3. NULL values through query builder — select returns null
  it('selects NULL values back as null in result', async () => {
    const db = getDb();
    const rows = await db.select().from(items).where(eq(items.id, 's04'));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.name).toBeNull();
    expect(rows[0]!.value).toBeNull();
    expect(rows[0]!.active).toBeNull();
    expect(rows[0]!.score).toBeNull();
  });

  // 4. Empty string vs NULL
  it('treats empty string as not-null and distinct from NULL', async () => {
    const db = getDb();
    const rows = await db.select().from(items).where(eq(items.id, 's02'));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.name).toBe('');
    expect(rows[0]!.name).not.toBeNull();

    // Empty string should be returned by isNotNull
    const notNullNames = await db
      .select({ id: items.id })
      .from(items)
      .where(isNotNull(items.name));
    expect(notNullNames.map((r) => r.id)).toContain('s02');
  });

  // 5. Zero values — 0 distinct from null
  it('zero values are distinct from NULL', async () => {
    const db = getDb();
    const rows = await db.select().from(items).where(eq(items.value, 0));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.id).toBe('s02');
    expect(rows[0]!.value).toBe(0);
    expect(rows[0]!.score).toBe(0);

    const zeroNotNull = await db
      .select({ id: items.id })
      .from(items)
      .where(and(eq(items.value, 0), isNotNull(items.value)));
    expect(zeroNotNull).toHaveLength(1);
    expect(zeroNotNull[0]!.id).toBe('s02');
  });

  // 6. Negative numbers
  it('queries negative integers and doubles correctly', async () => {
    const db = getDb();
    const negInts = await db.select({ id: items.id, value: items.value }).from(items).where(lt(items.value, 0));
    expect(negInts.length).toBeGreaterThanOrEqual(2);
    expect(negInts.every((r) => (r.value ?? 0) < 0)).toBe(true);
    expect(negInts.map((r) => r.id).sort()).toEqual(expect.arrayContaining(['s03', 's06']));

    const negDoubles = await db
      .select({ id: items.id, score: items.score })
      .from(items)
      .where(lt(items.score, 0));
    expect(negDoubles.length).toBeGreaterThanOrEqual(2);
    expect(negDoubles.every((r) => (r.score ?? 0) < 0)).toBe(true);
  });

  // 7. Very large numbers near INT range
  it('handles INT values near the 32-bit boundary', async () => {
    const db = getDb();
    const rows = await db
      .select({ id: items.id, value: items.value })
      .from(items)
      .where(gte(items.value, 2_000_000_000));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.id).toBe('s05');
    expect(typeof rows[0]!.value).toBe('number');
    expect(rows[0]!.value).toBe(2147483000);
  });

  // 8. Large result sets — insert 100, select all
  it('handles large result sets (100 rows)', async () => {
    const db = getDb();
    const bulkTable = uniqueName('qb_edge_bulk');
    await dropTable(db, bulkTable);
    await db.execute(
      sql.raw(
        `CREATE TABLE IF NOT EXISTS ${bt(bulkTable)} (id STRING, value INT) USING DELTA`,
      ),
    );
    try {
      const tuples: string[] = [];
      for (let i = 0; i < 100; i++) {
        tuples.push(`('b${String(i).padStart(3, '0')}', ${i})`);
      }
      await db.execute(sql.raw(`INSERT INTO ${bt(bulkTable)} (id, value) VALUES ${tuples.join(', ')}`));

      const bulk = databricksTable(bulkTable, {
        id: string('id'),
        value: int('value'),
      });

      const rows = await db.select().from(bulk);
      expect(rows).toHaveLength(100);
      const sum = rows.reduce((acc, r) => acc + (r.value ?? 0), 0);
      expect(sum).toBe((99 * 100) / 2);
    } finally {
      await dropTable(db, bulkTable);
    }
  });

  // 9. LIMIT 0
  it('LIMIT 0 returns an empty array', async () => {
    const db = getDb();
    const rows = await db.select().from(items).limit(0);
    expect(rows).toEqual([]);
  });

  // 10. OFFSET larger than result set
  it('OFFSET greater than result set returns an empty array', async () => {
    const db = getDb();
    const rows = await db.select().from(items).orderBy(asc(items.id)).limit(10).offset(1000);
    expect(rows).toEqual([]);
  });

  // 11. Special characters in string values — quotes, backticks, newlines, unicode emoji
  it('round-trips special characters in string values', async () => {
    const db = getDb();
    const specialTable = uniqueName('qb_edge_special');
    await dropTable(db, specialTable);
    await db.execute(
      sql.raw(`CREATE TABLE IF NOT EXISTS ${bt(specialTable)} (id STRING, name STRING) USING DELTA`),
    );
    try {
      const sp = databricksTable(specialTable, {
        id: string('id'),
        name: string('name'),
      });

      const cases: Array<{ id: string; name: string }> = [
        { id: 'q1', name: "single 'quote' inside" },
        { id: 'q2', name: 'double "quote" inside' },
        { id: 'q3', name: 'back`tick`s' },
        { id: 'q4', name: 'multi\nline\nstring' },
        { id: 'q5', name: 'tab\tseparated' },
        { id: 'q6', name: 'emoji rocket and party' },
        { id: 'q7', name: 'unicode greek alpha-beta-gamma' },
        { id: 'q8', name: 'backslash\\nliteral' },
      ];
      for (const c of cases) {
        await db.insert(sp).values(c);
      }

      for (const c of cases) {
        const rows = await db.select().from(sp).where(eq(sp.id, c.id));
        expect(rows).toHaveLength(1);
        expect(rows[0]!.name).toBe(c.name);
      }
    } finally {
      await dropTable(db, specialTable);
    }
  });

  // 12. Boolean false vs NULL
  it('boolean false is not treated as NULL', async () => {
    const db = getDb();
    const falseRows = await db
      .select({ id: items.id })
      .from(items)
      .where(eq(items.active, false));
    const ids = falseRows.map((r) => r.id).sort();
    // s02, s06, s08 are false; s04 is NULL and must NOT be included
    expect(ids).toEqual(['s02', 's06', 's08']);
    expect(ids).not.toContain('s04');

    const notNullActive = await db
      .select({ id: items.id })
      .from(items)
      .where(isNotNull(items.active));
    expect(notNullActive.map((r) => r.id)).not.toContain('s04');
    expect(notNullActive.map((r) => r.id)).toEqual(
      expect.arrayContaining(['s02', 's06', 's08']),
    );
  });

  // 13. Multiple sequential operations: insert, select, update, select, delete, select
  it('runs insert -> select -> update -> select -> delete -> select sequentially', async () => {
    const db = getDb();
    const rowId = 'seq01';

    await db.insert(items).values({
      id: rowId,
      name: 'sequential',
      value: 42,
      active: true,
      score: 4.2,
    });

    const afterInsert = await db.select().from(items).where(eq(items.id, rowId));
    expect(afterInsert).toHaveLength(1);
    expect(afterInsert[0]!.name).toBe('sequential');
    expect(afterInsert[0]!.value).toBe(42);

    await db.update(items).set({ value: 99, name: 'updated' }).where(eq(items.id, rowId));

    const afterUpdate = await db.select().from(items).where(eq(items.id, rowId));
    expect(afterUpdate).toHaveLength(1);
    expect(afterUpdate[0]!.value).toBe(99);
    expect(afterUpdate[0]!.name).toBe('updated');

    await db.delete(items).where(eq(items.id, rowId));

    const afterDelete = await db.select().from(items).where(eq(items.id, rowId));
    expect(afterDelete).toEqual([]);
  });

  // 14. SELECT with sql`` template fragment in where
  it('accepts a raw sql`` fragment in where()', async () => {
    const db = getDb();
    const rows = await db
      .select()
      .from(items)
      .where(sql`${items.id} = ${'s01'}`);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.id).toBe('s01');
    expect(rows[0]!.name).toBe('alpha');
  });

  // 15. BETWEEN
  it('BETWEEN returns inclusive range', async () => {
    const db = getDb();
    const rows = await db
      .select({ id: items.id, value: items.value })
      .from(items)
      .where(between(items.value, 5, 100));
    const ids = rows.map((r) => r.id).sort();
    // s01 (10), s07 (100), s08 (50) match — s02 (0) excluded, s05 too large
    expect(ids).toEqual(['s01', 's07', 's08']);
    expect(rows.every((r) => (r.value ?? 0) >= 5 && (r.value ?? 0) <= 100)).toBe(true);
  });

  // 16. IN ARRAY
  it('inArray matches any of the listed values', async () => {
    const db = getDb();
    const rows = await db
      .select({ id: items.id })
      .from(items)
      .where(inArray(items.id, ['s01', 's03', 's07']));
    const ids = rows.map((r) => r.id).sort();
    expect(ids).toEqual(['s01', 's03', 's07']);
  });

  // 17. NOT IN ARRAY
  it('notInArray excludes the listed values', async () => {
    const db = getDb();
    const rows = await db
      .select({ id: items.id })
      .from(items)
      .where(notInArray(items.id, ['s01', 's02', 's03', 's04']));
    const ids = rows.map((r) => r.id).sort();
    // NULLs (s04) are typically excluded by NOT IN even when not in the list, but s04's id is the
    // exclusion list, so we expect the remaining non-NULL-id rows.
    expect(ids).toEqual(['s05', 's06', 's07', 's08']);
  });

  // 18. Complex nested AND/OR
  it('evaluates nested and(or(...), gt(...))', async () => {
    const db = getDb();
    const rows = await db
      .select({ id: items.id, value: items.value, active: items.active })
      .from(items)
      .where(
        and(
          or(eq(items.id, 's01'), eq(items.id, 's07'), eq(items.id, 's08')),
          gt(items.value, 20),
        ),
      );
    const ids = rows.map((r) => r.id).sort();
    // s01 value=10 fails gt; s07 value=100 ok; s08 value=50 ok
    expect(ids).toEqual(['s07', 's08']);

    // Add a more nested expression
    const rows2 = await db
      .select({ id: items.id })
      .from(items)
      .where(
        or(
          and(eq(items.active, true), gt(items.value, 50)),
          and(eq(items.active, false), lt(items.value, 0)),
        ),
      );
    const ids2 = rows2.map((r) => r.id).sort();
    // active=true & value>50: s05, s07; active=false & value<0: s06
    expect(ids2).toEqual(['s05', 's06', 's07']);
  });

  // 19. Double / float precision round-trip
  it('round-trips double precision values', async () => {
    const db = getDb();
    const precTable = uniqueName('qb_edge_prec');
    await dropTable(db, precTable);
    await db.execute(
      sql.raw(`CREATE TABLE IF NOT EXISTS ${bt(precTable)} (id STRING, score DOUBLE) USING DELTA`),
    );
    try {
      const prec = databricksTable(precTable, {
        id: string('id'),
        score: double('score'),
      });

      const samples: Array<{ id: string; score: number }> = [
        { id: 'p1', score: 0.1 + 0.2 },
        { id: 'p2', score: Math.PI },
        { id: 'p3', score: -Math.E },
        { id: 'p4', score: 1.234567890123456 },
        { id: 'p5', score: 1e-10 },
        { id: 'p6', score: 1.5e8 },
      ];
      for (const s of samples) {
        await db.insert(prec).values(s);
      }
      const rows = await db.select().from(prec).orderBy(asc(prec.id));
      expect(rows).toHaveLength(samples.length);
      for (const s of samples) {
        const row = rows.find((r) => r.id === s.id);
        expect(row).toBeDefined();
        expect(typeof row!.score).toBe('number');
        expect(row!.score).toBeCloseTo(s.score, 10);
      }
    } finally {
      await dropTable(db, precTable);
    }
  });

  // 20. ORDER BY with NULLs — verify sort is consistent
  it('ORDER BY places NULLs consistently', async () => {
    const db = getDb();
    const ascRows = await db
      .select({ id: items.id, value: items.value })
      .from(items)
      .orderBy(asc(items.value));
    expect(ascRows).toHaveLength(SEED_IDS.length);
    // Verify non-null portion is ascending and NULLs are clustered (either head or tail)
    const valuesAsc = ascRows.map((r) => r.value);
    const firstNonNullIdxAsc = valuesAsc.findIndex((v) => v !== null);
    const lastNonNullIdxAsc = (() => {
      for (let i = valuesAsc.length - 1; i >= 0; i--) {
        if (valuesAsc[i] !== null) return i;
      }
      return -1;
    })();
    // All NULLs must be on one contiguous side
    const nullsAtHead = valuesAsc.slice(0, firstNonNullIdxAsc).every((v) => v === null);
    const nullsAtTail = valuesAsc.slice(lastNonNullIdxAsc + 1).every((v) => v === null);
    expect(nullsAtHead || nullsAtTail).toBe(true);
    // Non-null section must be ascending
    for (let i = firstNonNullIdxAsc + 1; i <= lastNonNullIdxAsc; i++) {
      const prev = valuesAsc[i - 1];
      const curr = valuesAsc[i];
      if (prev !== null && curr !== null) {
        expect(curr).toBeGreaterThanOrEqual(prev);
      }
    }

    const descRows = await db
      .select({ id: items.id, value: items.value })
      .from(items)
      .orderBy(desc(items.value));
    expect(descRows).toHaveLength(SEED_IDS.length);
    const valuesDesc = descRows.map((r) => r.value);
    const firstNonNullIdxDesc = valuesDesc.findIndex((v) => v !== null);
    const lastNonNullIdxDesc = (() => {
      for (let i = valuesDesc.length - 1; i >= 0; i--) {
        if (valuesDesc[i] !== null) return i;
      }
      return -1;
    })();
    const nullsAtHeadDesc = valuesDesc.slice(0, firstNonNullIdxDesc).every((v) => v === null);
    const nullsAtTailDesc = valuesDesc.slice(lastNonNullIdxDesc + 1).every((v) => v === null);
    expect(nullsAtHeadDesc || nullsAtTailDesc).toBe(true);
    for (let i = firstNonNullIdxDesc + 1; i <= lastNonNullIdxDesc; i++) {
      const prev = valuesDesc[i - 1];
      const curr = valuesDesc[i];
      if (prev !== null && curr !== null) {
        expect(curr).toBeLessThanOrEqual(prev);
      }
    }
  });

  // Bonus: like with special characters
  it('LIKE handles wildcard pattern matches', async () => {
    const db = getDb();
    const rows = await db
      .select({ id: items.id, name: items.name })
      .from(items)
      .where(like(items.name, '%a%'));
    // alpha, gamma, delta, eta, zeta all contain 'a'
    expect(rows.length).toBeGreaterThanOrEqual(4);
    expect(rows.every((r) => (r.name ?? '').includes('a'))).toBe(true);
  });

  // Bonus: ne with NULL — SQL semantics: NULL <> x is NULL/false
  it('ne does not match NULL rows (SQL three-valued logic)', async () => {
    const db = getDb();
    const rows = await db
      .select({ id: items.id })
      .from(items)
      .where(ne(items.name, 'alpha'));
    const ids = rows.map((r) => r.id);
    expect(ids).not.toContain('s04'); // NULL name
    expect(ids).not.toContain('s01'); // matches 'alpha'
  });

  // Bonus: SELECT DISTINCT with NULLs — NULL is one distinct group
  it('SELECT DISTINCT treats NULL as a single distinct group', async () => {
    const db = getDb();
    const rows = await db.selectDistinct({ active: items.active }).from(items);
    // distinct values: true, false, null -> 3 groups
    expect(rows).toHaveLength(3);
    const seen = rows.map((r) => r.active);
    expect(seen).toContain(true);
    expect(seen).toContain(false);
    expect(seen.some((v) => v === null)).toBe(true);
  });
});
