import { sql } from 'drizzle-orm';
import { afterAll, describe, expect, it } from 'vitest';
import type { DatabricksDatabase } from '../../src/driver';
import { closeDb, dropTable, getDb, hasCredentials } from './helpers';

const createdTables: string[] = [];

async function createTable(db: DatabricksDatabase, name: string, columnsDdl: string): Promise<void> {
  createdTables.push(name);
  await db.execute(sql.raw(`CREATE TABLE IF NOT EXISTS \`${name}\` (${columnsDdl}) USING DELTA`));
}

describe.skipIf(!hasCredentials())('Column type round-trips', () => {
  afterAll(async () => {
    const db = getDb();
    for (const name of createdTables) {
      try {
        await dropTable(db, name);
      } catch {
        // best-effort cleanup
      }
    }
    await closeDb();
  });

  it('STRING round-trip', async () => {
    const db = getDb();
    const t = ('col_string');
    await createTable(db, t, 'id INT, val STRING');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, 'hello world')`));
    const rows = await db.execute<{ id: number; val: string }>(sql.raw(`SELECT * FROM \`${t}\``));
    expect(rows[0]?.val).toBe('hello world');
  });

  it('VARCHAR(255) round-trip', async () => {
    const db = getDb();
    const t = ('col_varchar');
    const value = 'a'.repeat(255);
    await createTable(db, t, 'id INT, val VARCHAR(255)');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, '${value}')`));
    const rows = await db.execute<{ id: number; val: string }>(sql.raw(`SELECT * FROM \`${t}\``));
    expect(rows[0]?.val).toBe(value);
    expect(rows[0]?.val.length).toBe(255);
  });

  it('CHAR(10) round-trip', async () => {
    const db = getDb();
    const t = ('col_char');
    await createTable(db, t, 'id INT, val CHAR(10)');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, 'abc')`));
    const rows = await db.execute<{ id: number; val: string }>(sql.raw(`SELECT * FROM \`${t}\``));
    expect(rows[0]?.val).toMatch(/^abc/);
  });

  it('INT round-trip', async () => {
    const db = getDb();
    const t = ('col_int');
    await createTable(db, t, 'id INT, val INT');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, 42)`));
    const rows = await db.execute<{ id: number; val: number }>(sql.raw(`SELECT * FROM \`${t}\``));
    expect(typeof rows[0]?.val).toBe('number');
    expect(rows[0]?.val).toBe(42);
  });

  it('BIGINT round-trip', async () => {
    const db = getDb();
    const t = ('col_bigint');
    await createTable(db, t, 'id INT, val BIGINT');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, 9007199254740000)`));
    const rows = await db.execute<{ id: number; val: number | string | bigint }>(
      sql.raw(`SELECT * FROM \`${t}\``),
    );
    const val = rows[0]?.val;
    expect(val).toBeDefined();
    expect(Number(val)).toBe(9007199254740000);
  });

  it('SMALLINT round-trip', async () => {
    const db = getDb();
    const t = ('col_smallint');
    await createTable(db, t, 'id INT, val SMALLINT');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, 32000)`));
    const rows = await db.execute<{ id: number; val: number }>(sql.raw(`SELECT * FROM \`${t}\``));
    expect(typeof rows[0]?.val).toBe('number');
    expect(rows[0]?.val).toBe(32000);
  });

  it('TINYINT round-trip', async () => {
    const db = getDb();
    const t = ('col_tinyint');
    await createTable(db, t, 'id INT, val TINYINT');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, 127)`));
    const rows = await db.execute<{ id: number; val: number }>(sql.raw(`SELECT * FROM \`${t}\``));
    expect(typeof rows[0]?.val).toBe('number');
    expect(rows[0]?.val).toBe(127);
  });

  it('FLOAT round-trip', async () => {
    const db = getDb();
    const t = ('col_float');
    await createTable(db, t, 'id INT, val FLOAT');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, 3.14)`));
    const rows = await db.execute<{ id: number; val: number }>(sql.raw(`SELECT * FROM \`${t}\``));
    expect(rows[0]?.val).toBeCloseTo(3.14, 2);
  });

  it('DOUBLE round-trip', async () => {
    const db = getDb();
    const t = ('col_double');
    await createTable(db, t, 'id INT, val DOUBLE');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, 3.141592653589793)`));
    const rows = await db.execute<{ id: number; val: number }>(sql.raw(`SELECT * FROM \`${t}\``));
    expect(rows[0]?.val).toBeCloseTo(3.141592653589793, 12);
  });

  it('DECIMAL(18, 6) round-trip preserves precision', async () => {
    const db = getDb();
    const t = ('col_decimal');
    await createTable(db, t, 'id INT, val DECIMAL(18, 6)');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, 123456.789012)`));
    const rows = await db.execute<{ id: number; val: string | number }>(
      sql.raw(`SELECT * FROM \`${t}\``),
    );
    expect(String(rows[0]?.val)).toBe('123456.789012');
  });

  it('BOOLEAN round-trip (true and false)', async () => {
    const db = getDb();
    const t = ('col_boolean');
    await createTable(db, t, 'id INT, val BOOLEAN');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, true), (2, false)`));
    const rows = await db.execute<{ id: number; val: boolean }>(
      sql.raw(`SELECT * FROM \`${t}\` ORDER BY id`),
    );
    expect(typeof rows[0]?.val).toBe('boolean');
    expect(rows[0]?.val).toBe(true);
    expect(rows[1]?.val).toBe(false);
  });

  it('DATE round-trip', async () => {
    const db = getDb();
    const t = ('col_date');
    await createTable(db, t, 'id INT, val DATE');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, DATE'2024-06-15')`));
    const rows = await db.execute<{ id: number; val: string | Date }>(
      sql.raw(`SELECT * FROM \`${t}\``),
    );
    const val = rows[0]?.val;
    expect(val).toBeDefined();
    const asString = val instanceof Date ? val.toISOString().slice(0, 10) : String(val);
    expect(asString).toContain('2024-06-15');
  });

  it('TIMESTAMP round-trip', async () => {
    const db = getDb();
    const t = ('col_timestamp');
    await createTable(db, t, 'id INT, val TIMESTAMP');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, TIMESTAMP'2024-06-15 10:30:00')`));
    const rows = await db.execute<{ id: number; val: string | Date }>(
      sql.raw(`SELECT * FROM \`${t}\``),
    );
    const val = rows[0]?.val;
    expect(val).toBeDefined();
    const asString = val instanceof Date ? val.toISOString() : String(val);
    expect(asString).toMatch(/2024-06-15/);
    expect(asString).toMatch(/10:30:00/);
  });

  it('TIMESTAMP_NTZ round-trip', async () => {
    const db = getDb();
    const t = ('col_timestamp_ntz');
    await createTable(db, t, 'id INT, val TIMESTAMP_NTZ');
    await db.execute(
      sql.raw(`INSERT INTO \`${t}\` VALUES (1, TIMESTAMP_NTZ'2024-06-15 10:30:00')`),
    );
    const rows = await db.execute<{ id: number; val: string | Date }>(
      sql.raw(`SELECT * FROM \`${t}\``),
    );
    const val = rows[0]?.val;
    expect(val).toBeDefined();
    const asString = val instanceof Date ? val.toISOString() : String(val);
    expect(asString).toMatch(/2024-06-15/);
    expect(asString).toMatch(/10:30:00/);
  });

  it('BINARY round-trip', async () => {
    const db = getDb();
    const t = ('col_binary');
    await createTable(db, t, "id INT, val BINARY");
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, X'DEADBEEF')`));
    const rows = await db.execute<{ id: number; val: Uint8Array | Buffer | string }>(
      sql.raw(`SELECT * FROM \`${t}\``),
    );
    const val = rows[0]?.val;
    expect(val).toBeDefined();
    let hex: string;
    if (val instanceof Uint8Array || Buffer.isBuffer(val as Buffer)) {
      hex = Buffer.from(val as Uint8Array).toString('hex').toUpperCase();
    } else {
      hex = String(val).toUpperCase().replace(/[^0-9A-F]/g, '');
    }
    expect(hex).toContain('DEADBEEF');
  });

  it('VARIANT round-trip', async () => {
    const db = getDb();
    const t = ('col_variant');
    await createTable(db, t, 'id INT, val VARIANT');
    await db.execute(
      sql.raw(`INSERT INTO \`${t}\` VALUES (1, PARSE_JSON('{"key": "value", "num": 42}'))`),
    );
    const rows = await db.execute<{ id: number; val: unknown }>(
      sql.raw(`SELECT * FROM \`${t}\``),
    );
    const val = rows[0]?.val;
    expect(val).toBeDefined();
    const parsed = typeof val === 'string' ? JSON.parse(val) : val;
    expect(parsed).toMatchObject({ key: 'value', num: 42 });
  });

  it('combined table with multiple column types', async () => {
    const db = getDb();
    const t = ('col_combined');
    await createTable(
      db,
      t,
      [
        'id INT',
        'name STRING',
        'count BIGINT',
        'ratio DOUBLE',
        'active BOOLEAN',
        'created_at TIMESTAMP',
        'amount DECIMAL(10, 2)',
      ].join(', '),
    );
    await db.execute(
      sql.raw(
        `INSERT INTO \`${t}\` VALUES (1, 'alice', 1000, 0.75, true, TIMESTAMP'2024-06-15 10:30:00', 99.99)`,
      ),
    );
    const rows = await db.execute<{
      id: number;
      name: string;
      count: number | string | bigint;
      ratio: number;
      active: boolean;
      created_at: string | Date;
      amount: string | number;
    }>(sql.raw(`SELECT * FROM \`${t}\``));
    const row = rows[0];
    expect(row).toBeDefined();
    expect(row?.id).toBe(1);
    expect(row?.name).toBe('alice');
    expect(String(row?.count)).toBe('1000');
    expect(row?.ratio).toBeCloseTo(0.75, 6);
    expect(row?.active).toBe(true);
    const tsString =
      row?.created_at instanceof Date ? row.created_at.toISOString() : String(row?.created_at);
    expect(tsString).toMatch(/2024-06-15/);
    expect(String(row?.amount)).toBe('99.99');
  });

  it('NULL value round-trip on a nullable column', async () => {
    const db = getDb();
    const t = ('col_null');
    await createTable(db, t, 'id INT, val STRING');
    await db.execute(sql.raw(`INSERT INTO \`${t}\` VALUES (1, NULL)`));
    const rows = await db.execute<{ id: number; val: string | null }>(
      sql.raw(`SELECT * FROM \`${t}\``),
    );
    expect(rows[0]?.val).toBeNull();
  });
});
