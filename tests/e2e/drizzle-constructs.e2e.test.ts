import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  databricksTable,
  databricksSchema,
  string,
  int,
  bigint,
  boolean,
  double,
  decimal,
  timestamp,
  timestampNtz,
  variant,
} from '../../src';
import { closeDb, dropTable, getDb, hasCredentials, uniqueName } from './helpers';

const usersName = uniqueName('dz_users');
const eventsName = uniqueName('dz_events');

const users = databricksTable(usersName, {
  id: string('id'),
  name: string('name'),
  age: int('age'),
  loginCount: bigint('login_count'),
  active: boolean('active'),
  score: double('score'),
  balance: decimal('balance', { precision: 12, scale: 2 }),
  createdAt: timestamp('created_at'),
});

const events = databricksTable(eventsName, {
  id: int('id'),
  userId: string('user_id'),
  payload: variant('payload'),
  occurredAt: timestampNtz('occurred_at'),
});

describe.skipIf(!hasCredentials())('Drizzle constructs (e2e)', () => {
  beforeAll(async () => {
    const db = getDb();
    await dropTable(db, usersName);
    await dropTable(db, eventsName);
    await db.execute(
      sql`CREATE TABLE IF NOT EXISTS ${users} (
        ${sql.identifier('id')} STRING,
        ${sql.identifier('name')} STRING,
        ${sql.identifier('age')} INT,
        ${sql.identifier('login_count')} BIGINT,
        ${sql.identifier('active')} BOOLEAN,
        ${sql.identifier('score')} DOUBLE,
        ${sql.identifier('balance')} DECIMAL(12, 2),
        ${sql.identifier('created_at')} TIMESTAMP
      ) USING DELTA`,
    );
    await db.execute(
      sql`CREATE TABLE IF NOT EXISTS ${events} (
        ${sql.identifier('id')} INT,
        ${sql.identifier('user_id')} STRING,
        ${sql.identifier('payload')} VARIANT,
        ${sql.identifier('occurred_at')} TIMESTAMP_NTZ
      ) USING DELTA`,
    );
  });

  afterAll(async () => {
    const db = getDb();
    try {
      await dropTable(db, usersName);
      await dropTable(db, eventsName);
    } finally {
      await closeDb();
    }
  });

  it('uses table reference in INSERT and SELECT', async () => {
    const db = getDb();
    await db.execute(
      sql`INSERT INTO ${users} (${users.id}, ${users.name}, ${users.age}, ${users.loginCount}, ${users.active}, ${users.score}, ${users.balance}, ${users.createdAt})
          VALUES (${'u1'}, ${'Alice'}, ${30}, ${100}, ${true}, ${3.14}, ${'999.99'}, ${'2024-06-15T10:30:00Z'})`,
    );

    const rows = await db.execute<Record<string, unknown>>(
      sql`SELECT ${users.id}, ${users.name}, ${users.age} FROM ${users} WHERE ${users.id} = ${'u1'}`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ id: 'u1', name: 'Alice', age: 30 });
  });

  it('references multiple columns in WHERE with AND', async () => {
    const db = getDb();
    await db.execute(
      sql`INSERT INTO ${users} (${users.id}, ${users.name}, ${users.age}, ${users.loginCount}, ${users.active}, ${users.score}, ${users.balance}, ${users.createdAt})
          VALUES (${'u2'}, ${'Bob'}, ${25}, ${50}, ${false}, ${2.71}, ${'123.45'}, ${'2024-07-01T08:00:00Z'})`,
    );

    const rows = await db.execute<Record<string, unknown>>(
      sql`SELECT ${users.name} FROM ${users} WHERE ${users.age} > ${20} AND ${users.active} = ${true}`,
    );
    expect(rows.length).toBeGreaterThanOrEqual(1);
    expect(rows.every((r) => r.name !== undefined)).toBe(true);
  });

  it('uses column references in ORDER BY', async () => {
    const db = getDb();
    const rows = await db.execute<Record<string, unknown>>(
      sql`SELECT ${users.id}, ${users.age} FROM ${users} ORDER BY ${users.age} DESC`,
    );
    expect(rows.length).toBeGreaterThanOrEqual(2);
    const ages = rows.map((r) => r.age as number);
    for (let i = 1; i < ages.length; i++) {
      expect(ages[i - 1]).toBeGreaterThanOrEqual(ages[i]!);
    }
  });

  it('uses column references in UPDATE SET and WHERE', async () => {
    const db = getDb();
    await db.execute(
      sql`UPDATE ${users} SET ${users.name} = ${'Alicia'} WHERE ${users.id} = ${'u1'}`,
    );
    const rows = await db.execute<Record<string, unknown>>(
      sql`SELECT ${users.name} FROM ${users} WHERE ${users.id} = ${'u1'}`,
    );
    expect(rows).toEqual([{ name: 'Alicia' }]);
  });

  it('uses column references in DELETE WHERE', async () => {
    const db = getDb();
    await db.execute(
      sql`INSERT INTO ${users} (${users.id}, ${users.name}, ${users.age}, ${users.loginCount}, ${users.active}, ${users.score}, ${users.balance}, ${users.createdAt})
          VALUES (${'u_del'}, ${'ToDelete'}, ${99}, ${0}, ${false}, ${0.0}, ${'0.00'}, ${'2024-01-01T00:00:00Z'})`,
    );
    await db.execute(
      sql`DELETE FROM ${users} WHERE ${users.id} = ${'u_del'}`,
    );
    const rows = await db.execute<Record<string, unknown>>(
      sql`SELECT ${users.id} FROM ${users} WHERE ${users.id} = ${'u_del'}`,
    );
    expect(rows).toEqual([]);
  });

  it('inserts and reads VARIANT via column reference', async () => {
    const db = getDb();
    const payload = JSON.stringify({ action: 'login', ip: '1.2.3.4' });
    await db.execute(
      sql`INSERT INTO ${events} (${events.id}, ${events.userId}, ${events.payload}, ${events.occurredAt})
          VALUES (${1}, ${'u1'}, PARSE_JSON(${payload}), ${'2024-06-15T10:30:00'})`,
    );
    const rows = await db.execute<Record<string, unknown>>(
      sql`SELECT ${events.id}, ${events.payload} FROM ${events} WHERE ${events.userId} = ${'u1'}`,
    );
    expect(rows).toHaveLength(1);
    const val = rows[0]!.payload;
    const parsed = typeof val === 'string' ? JSON.parse(val) : val;
    expect(parsed).toMatchObject({ action: 'login', ip: '1.2.3.4' });
  });

  it('composes fragments using table and column references', async () => {
    const db = getDb();
    const selectCols = sql`${users.id}, ${users.name}, ${users.score}`;
    const fromClause = sql`FROM ${users}`;
    const whereClause = sql`WHERE ${users.score} > ${2.0}`;

    const rows = await db.execute<Record<string, unknown>>(
      sql`SELECT ${selectCols} ${fromClause} ${whereClause} ORDER BY ${users.score} DESC`,
    );
    expect(rows.length).toBeGreaterThanOrEqual(1);
  });

  it('uses sql.identifier for dynamic column selection', async () => {
    const db = getDb();
    const dynamicCol = 'name';
    const rows = await db.execute<Record<string, unknown>>(
      sql`SELECT ${sql.identifier(dynamicCol)} FROM ${users} WHERE ${users.id} = ${'u1'}`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]![dynamicCol]).toBeDefined();
  });

  it('references table in aggregate with column ref in GROUP BY', async () => {
    const db = getDb();
    const rows = await db.execute<{ active: boolean; cnt: number }>(
      sql`SELECT ${users.active}, COUNT(*) AS cnt FROM ${users} GROUP BY ${users.active} ORDER BY ${users.active}`,
    );
    expect(rows.length).toBeGreaterThanOrEqual(1);
    expect(rows.every((r) => typeof r.cnt === 'number' || typeof r.cnt === 'string')).toBe(true);
  });

  it('joins two tables using column references', async () => {
    const db = getDb();
    const rows = await db.execute<{ name: string; event_id: number }>(
      sql`SELECT ${users.name}, ${events.id} AS event_id
          FROM ${users}
          INNER JOIN ${events} ON ${users.id} = ${events.userId}
          ORDER BY ${events.id}`,
    );
    expect(rows.length).toBeGreaterThanOrEqual(1);
    expect(rows[0]!.name).toBeDefined();
    expect(rows[0]!.event_id).toBeDefined();
  });

  it('uses databricksSchema for schema-qualified table reference', async () => {
    const db = getDb();
    const schemaName = process.env['DATABRICKS_SCHEMA'];
    if (!schemaName) return;

    const schema = databricksSchema(schemaName);
    const schemaUsers = schema.table(usersName, {
      id: string('id'),
      name: string('name'),
    });

    const rows = await db.execute<Record<string, unknown>>(
      sql`SELECT ${schemaUsers.id}, ${schemaUsers.name} FROM ${schemaUsers} WHERE ${schemaUsers.id} = ${'u1'}`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ id: 'u1', name: 'Alicia' });
  });
});
