import { eq, sql } from 'drizzle-orm';
import { entityKind } from 'drizzle-orm/entity';
import { describe, expect, it, vi } from 'vitest';

import { DatabricksDatabase, drizzle } from '../../src/driver';
import { databricksTable, string, int, boolean } from '../../src';
import { MockDBSQLClient } from '../mocks/databricks-sql';

const users = databricksTable('users', {
  id: string('id'),
  name: string('name'),
  age: int('age'),
  active: boolean('active'),
});

describe('drizzle()', () => {
  it('creates a DatabricksDatabase from a client config', () => {
    const mockClient = new MockDBSQLClient();
    const db = drizzle({ client: mockClient as never });
    expect(db).toBeInstanceOf(DatabricksDatabase);
  });

  it('runs a query via the underlying mock client', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ one: 1 }]);
    const db = drizzle({ client: mockClient as never });

    await db.execute(sql`SELECT 1`);

    expect(mockClient.recorded).toHaveLength(1);
    expect(mockClient.recorded[0]!.sql).toBe('SELECT 1');
  });

  it('$close() closes the session manager', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ x: 1 }]);
    const db = drizzle({ client: mockClient as never });

    await db.execute(sql`SELECT 1`);
    await db.$close();

    expect(mockClient.sessions[0]?.closed).toBe(true);
    // Provided client is not owned, so it must not be closed.
    expect(mockClient.closed).toBe(false);
  });

  it('enables a logger when logger: true is passed', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ one: 1 }]);
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const db = drizzle({ client: mockClient as never }, { logger: true });

    await db.execute(sql`SELECT ${1}`);

    expect(consoleSpy).toHaveBeenCalled();
    consoleSpy.mockRestore();
  });

  it('accepts a custom logger object', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([]);
    const logQuery = vi.fn();
    const db = drizzle(
      { client: mockClient as never },
      { logger: { logQuery } },
    );

    await db.execute(sql`SELECT ${'a'}`);

    expect(logQuery).toHaveBeenCalledWith('SELECT ?', ['a']);
  });
});

describe('DatabricksDatabase', () => {
  it('exposes entityKind as DatabricksDatabase', () => {
    expect(
      (DatabricksDatabase as unknown as Record<symbol, string>)[entityKind],
    ).toBe('DatabricksDatabase');
  });

  it('db.select().from() executes and maps results', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ id: 'u1', name: 'Alice', age: 30, active: true }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users);
    expect(rows).toEqual([{ id: 'u1', name: 'Alice', age: 30, active: true }]);
    expect(mockClient.recorded[0]!.sql).toBe('select `id`, `name`, `age`, `active` from `users`');
  });

  it('db.select(partial).from().where() executes correctly', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ id: 'u1', name: 'Alice' }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db
      .select({ id: users.id, name: users.name })
      .from(users)
      .where(eq(users.active, true));
    expect(rows).toEqual([{ id: 'u1', name: 'Alice' }]);
    expect(mockClient.recorded[0]!.sql).toContain('where');
    expect(mockClient.recorded[0]!.params).toEqual([true]);
  });

  it('db.insert(table).values() executes correctly', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([]);
    const db = drizzle({ client: mockClient as never });

    await db.insert(users).values({ id: 'u1', name: 'Alice', age: 30, active: true });
    expect(mockClient.recorded[0]!.sql).toBe(
      'insert into `users` (`id`, `name`, `age`, `active`) values (?, ?, ?, ?)',
    );
    expect(mockClient.recorded[0]!.params).toEqual(['u1', 'Alice', 30, true]);
  });

  it('db.update(table).set().where() executes correctly', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([]);
    const db = drizzle({ client: mockClient as never });

    await db.update(users).set({ name: 'Alicia' }).where(eq(users.id, 'u1'));
    expect(mockClient.recorded[0]!.sql).toBe(
      'update `users` set `name` = ? where `users`.`id` = ?',
    );
    expect(mockClient.recorded[0]!.params).toEqual(['Alicia', 'u1']);
  });

  it('db.selectDistinct().from() generates SELECT DISTINCT', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ name: 'Alice' }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.selectDistinct({ name: users.name }).from(users);
    expect(rows).toEqual([{ name: 'Alice' }]);
    expect(mockClient.recorded[0]!.sql).toContain('select distinct');
  });

  it('db.run() returns raw result shape', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ id: 1 }]);
    const db = drizzle({ client: mockClient as never });

    const result = await db.run(sql`INSERT INTO t VALUES (1)`);
    expect(result).toEqual([{ id: 1 }]);
  });

  it('db.all() returns row array directly', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ id: 1 }, { id: 2 }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.all(sql`SELECT * FROM t`);
    expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it('db.delete(table).where() executes correctly', async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([]);
    const db = drizzle({ client: mockClient as never });

    await db.delete(users).where(eq(users.id, 'u1'));
    expect(mockClient.recorded[0]!.sql).toBe(
      'delete from `users` where `users`.`id` = ?',
    );
    expect(mockClient.recorded[0]!.params).toEqual(['u1']);
  });
});
