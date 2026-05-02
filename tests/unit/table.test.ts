import { describe, expect, it } from 'vitest';
import { entityKind, is } from 'drizzle-orm/entity';
import { getTableName } from 'drizzle-orm/table';
import {
  bigint,
  boolean,
  decimal,
  int,
  string,
  timestamp,
  varchar,
} from '../../src/columns/index';
import { DatabricksTable, databricksSchema, databricksTable } from '../../src/table';

describe('databricksTable()', () => {
  it('creates a DatabricksTable instance', () => {
    const users = databricksTable('users', {
      id: int().primaryKey(),
      name: string(),
    });
    expect(is(users, DatabricksTable)).toBe(true);
  });

  it('has the DatabricksTable entityKind', () => {
    expect((DatabricksTable as any)[entityKind]).toBe('DatabricksTable');
  });

  it('reports the table name via getTableName', () => {
    const users = databricksTable('users', { id: int() });
    expect(getTableName(users)).toBe('users');
  });

  it('exposes columns as properties on the table', () => {
    const users = databricksTable('users', {
      id: int().primaryKey(),
      name: string(),
      email: varchar({ length: 255 }),
    });

    expect(users.id).toBeDefined();
    expect(users.name).toBeDefined();
    expect(users.email).toBeDefined();
  });

  it('column getSQLType() works on table columns', () => {
    const users = databricksTable('users', {
      id: int().primaryKey(),
      name: string(),
      age: bigint(),
      email: varchar({ length: 255 }),
      score: decimal({ precision: 10, scale: 2 }),
      active: boolean(),
      createdAt: timestamp(),
    });

    expect((users.id as any).getSQLType()).toBe('INT');
    expect((users.name as any).getSQLType()).toBe('STRING');
    expect((users.age as any).getSQLType()).toBe('BIGINT');
    expect((users.email as any).getSQLType()).toBe('VARCHAR(255)');
    expect((users.score as any).getSQLType()).toBe('DECIMAL(10, 2)');
    expect((users.active as any).getSQLType()).toBe('BOOLEAN');
    expect((users.createdAt as any).getSQLType()).toBe('TIMESTAMP');
  });

  it('assigns column names from object keys', () => {
    const users = databricksTable('users', {
      id: int(),
      displayName: string(),
    });
    expect((users.id as any).name).toBe('id');
    expect((users.displayName as any).name).toBe('displayName');
  });

  it('column.table points back to the parent table', () => {
    const users = databricksTable('users', { id: int() });
    expect((users.id as any).table).toBe(users);
  });

  it('preserves primaryKey/notNull config on built columns', () => {
    const users = databricksTable('users', {
      id: int().primaryKey().notNull(),
      name: string().notNull(),
    });
    expect((users.id as any).primary).toBe(true);
    expect((users.id as any).notNull).toBe(true);
    expect((users.name as any).notNull).toBe(true);
  });

  it('builds independent column instances per table', () => {
    const a = databricksTable('a', { id: int() });
    const b = databricksTable('b', { id: int() });
    expect((a.id as any).table).toBe(a);
    expect((b.id as any).table).toBe(b);
    expect(a.id).not.toBe(b.id);
  });
});

describe('databricksSchema()', () => {
  it('returns an object with the given schemaName and a .table() method', () => {
    const analytics = databricksSchema('analytics');
    expect(analytics.schemaName).toBe('analytics');
    expect(typeof analytics.table).toBe('function');
  });

  it('schema-qualified tables are DatabricksTable instances', () => {
    const analytics = databricksSchema('analytics');
    const events = analytics.table('events', {
      id: bigint().primaryKey(),
      eventName: string(),
    });
    expect(is(events, DatabricksTable)).toBe(true);
  });

  it('schema-qualified tables expose columns and getSQLType()', () => {
    const analytics = databricksSchema('analytics');
    const events = analytics.table('events', {
      id: bigint().primaryKey(),
      eventName: string(),
      ts: timestamp(),
    });

    expect(events.id).toBeDefined();
    expect(events.eventName).toBeDefined();
    expect(events.ts).toBeDefined();
    expect((events.id as any).getSQLType()).toBe('BIGINT');
    expect((events.eventName as any).getSQLType()).toBe('STRING');
    expect((events.ts as any).getSQLType()).toBe('TIMESTAMP');
  });

  it('multiple tables share the same schema name', () => {
    const analytics = databricksSchema('analytics');
    const events = analytics.table('events', { id: int() });
    const sessions = analytics.table('sessions', { id: int() });
    expect(is(events, DatabricksTable)).toBe(true);
    expect(is(sessions, DatabricksTable)).toBe(true);
    expect(getTableName(events)).toBe('events');
    expect(getTableName(sessions)).toBe('sessions');
  });
});
