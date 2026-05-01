import { sql } from 'drizzle-orm';
import { entityKind } from 'drizzle-orm/entity';
import { describe, expect, it } from 'vitest';

import { DatabricksDialect } from '../../src/dialect';

describe('DatabricksDialect', () => {
  it('escapes identifiers with backticks', () => {
    const dialect = new DatabricksDialect();
    expect(dialect.escapeName('table_name')).toBe('`table_name`');
  });

  it('escapes parameters as positional placeholders', () => {
    const dialect = new DatabricksDialect();
    expect(dialect.escapeParam(0, 'value')).toBe('?');
    expect(dialect.escapeParam(5, 42)).toBe('?');
  });

  it('escapes single quotes by doubling them', () => {
    const dialect = new DatabricksDialect();
    expect(dialect.escapeString("it's")).toBe("'it''s'");
    expect(dialect.escapeString('plain')).toBe("'plain'");
    expect(dialect.escapeString("a'b'c")).toBe("'a''b''c'");
  });

  it('compiles a sql template with a single param', () => {
    const dialect = new DatabricksDialect();
    const query = sql`SELECT * FROM users WHERE id = ${1}`;
    const result = dialect.sqlToQuery(query);
    expect(result.sql).toBe('SELECT * FROM users WHERE id = ?');
    expect(result.params).toEqual([1]);
  });

  it('compiles a sql template with multiple params', () => {
    const dialect = new DatabricksDialect();
    const query = sql`SELECT * FROM users WHERE id = ${1} AND name = ${'alice'} AND active = ${true}`;
    const result = dialect.sqlToQuery(query);
    expect(result.sql).toBe(
      'SELECT * FROM users WHERE id = ? AND name = ? AND active = ?',
    );
    expect(result.params).toEqual([1, 'alice', true]);
  });

  it('exposes entityKind as DatabricksDialect', () => {
    expect((DatabricksDialect as unknown as Record<symbol, string>)[entityKind]).toBe(
      'DatabricksDialect',
    );
  });
});
