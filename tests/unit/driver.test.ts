import { sql } from 'drizzle-orm';
import { entityKind } from 'drizzle-orm/entity';
import { describe, expect, it, vi } from 'vitest';

import { DatabricksDatabase, drizzle } from '../../src/driver';
import { MockDBSQLClient } from '../mocks/databricks-sql';

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
});
