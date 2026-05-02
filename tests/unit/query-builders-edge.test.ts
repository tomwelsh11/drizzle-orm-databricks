import {
  eq,
  and,
  or,
  ne,
  gt,
  lt,
  gte,
  lte,
  like,
  isNull,
  isNotNull,
  between,
  inArray,
  notInArray,
  asc,
  desc,
  sql,
} from 'drizzle-orm';
import { entityKind } from 'drizzle-orm/entity';
import { Param } from 'drizzle-orm/sql';
import { describe, expect, it } from 'vitest';

import {
  databricksTable,
  string,
  int,
  boolean,
  double,
  DatabricksDialect,
  DatabricksSelectBuilder,
  DatabricksInsertBuilder,
  DatabricksInsertBase,
  DatabricksUpdateBuilder,
  DatabricksUpdateBase,
  DatabricksSelectBase,
  DatabricksDeleteBase,
} from '../../src';

const users = databricksTable('users', {
  id: string('id'),
  name: string('name'),
  age: int('age'),
  active: boolean('active'),
  score: double('score'),
});

function makeSelectBuilder<T extends Record<string, unknown> | undefined = undefined>(
  fields?: T,
  distinct?: boolean,
) {
  const dialect = new DatabricksDialect();
  return new DatabricksSelectBuilder({
    fields: fields as T,
    session: null as any,
    dialect,
    distinct,
  });
}

function makeInsertBuilder() {
  const dialect = new DatabricksDialect();
  return new DatabricksInsertBuilder(users, null as any, dialect);
}

function makeUpdateBuilder() {
  const dialect = new DatabricksDialect();
  return new DatabricksUpdateBuilder(users, null as any, dialect);
}

function makeDeleteBase() {
  const dialect = new DatabricksDialect();
  return new DatabricksDeleteBase(users, null as any, dialect);
}

describe('SELECT edge cases', () => {
  it('multiple where() calls — last one wins', () => {
    const builder = makeSelectBuilder();
    const compiled = builder
      .from(users)
      .where(eq(users.id, 'a'))
      .where(eq(users.id, 'b'))
      .toSQL();
    expect(compiled.params).toEqual(['b']);
    expect(compiled.sql).toContain('where');
  });

  it('where() with or()', () => {
    const builder = makeSelectBuilder();
    const compiled = builder
      .from(users)
      .where(or(eq(users.id, 'a'), eq(users.id, 'b')))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain(' or ');
    expect(compiled.params).toEqual(['a', 'b']);
  });

  it('where() with ne', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .where(ne(users.id, 'x'))
      .toSQL();
    expect(compiled.sql).toContain('<>');
    expect(compiled.params).toEqual(['x']);
  });

  it('where() with gte and lte combined', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .where(and(gte(users.age, 18), lte(users.age, 65)))
      .toSQL();
    expect(compiled.sql).toContain('>=');
    expect(compiled.sql).toContain('<=');
    expect(compiled.params).toEqual([18, 65]);
  });

  it('where() with like', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .where(like(users.name, '%ali%'))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('like');
    expect(compiled.params).toEqual(['%ali%']);
  });

  it('where() with isNull', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .where(isNull(users.name))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('is null');
    expect(compiled.params).toEqual([]);
  });

  it('where() with isNotNull', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .where(isNotNull(users.name))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('is not null');
    expect(compiled.params).toEqual([]);
  });

  it('where() with inArray', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .where(inArray(users.id, ['a', 'b', 'c']))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain(' in ');
    expect(compiled.params).toEqual(['a', 'b', 'c']);
  });

  it('where() with notInArray', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .where(notInArray(users.id, ['a', 'b']))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('not in');
    expect(compiled.params).toEqual(['a', 'b']);
  });

  it('where() with between', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .where(between(users.age, 18, 65))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('between');
    expect(compiled.params).toEqual([18, 65]);
  });

  it('where() with complex nested AND/OR', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .where(and(or(eq(users.id, 'a'), eq(users.id, 'b')), gt(users.age, 21)))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain(' or ');
    expect(compiled.sql.toLowerCase()).toContain(' and ');
    expect(compiled.sql).toContain('>');
    expect(compiled.params).toEqual(['a', 'b', 21]);
  });

  it('orderBy() with explicit asc()', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .orderBy(asc(users.age))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('order by');
    expect(compiled.sql.toLowerCase()).toContain(' asc');
  });

  it('orderBy() with multiple columns mixing asc and desc', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .orderBy(asc(users.age), desc(users.name))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('order by');
    const orderByPart = compiled.sql.toLowerCase().slice(compiled.sql.toLowerCase().indexOf('order by'));
    expect(orderByPart).toContain('`age` asc');
    expect(orderByPart).toContain('`name` desc');
    expect(orderByPart.indexOf('`age`')).toBeLessThan(orderByPart.indexOf('`name`'));
  });

  it('select with sql template field', () => {
    const cnt = sql<number>`count(*)`.as('cnt');
    const compiled = makeSelectBuilder({ cnt })
      .from(users)
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('count(*)');
    expect(compiled.sql.toLowerCase()).toContain('as `cnt`');
  });

  it('groupBy() with multiple columns', () => {
    const compiled = makeSelectBuilder({ active: users.active, age: users.age })
      .from(users)
      .groupBy(users.active, users.age)
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('group by');
    expect(compiled.sql).toContain('`active`');
    expect(compiled.sql).toContain('`age`');
  });

  it('having() with condition', () => {
    const cnt = sql<number>`count(*)`.as('cnt');
    const compiled = makeSelectBuilder({ active: users.active, cnt })
      .from(users)
      .groupBy(users.active)
      .having(gt(cnt, 1))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('having');
    expect(compiled.params).toEqual([1]);
  });

  it('limit() and offset() together', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .limit(10)
      .offset(5)
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('limit');
    expect(compiled.sql.toLowerCase()).toContain('offset');
    expect(compiled.params).toEqual([10, 5]);
  });

  it('offset() without limit() — valid SQL output', () => {
    const compiled = makeSelectBuilder()
      .from(users)
      .offset(20)
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('offset');
    expect(compiled.sql.toLowerCase()).not.toContain('limit');
    expect(compiled.params).toEqual([20]);
  });

  it('chains where + groupBy + having + orderBy + limit + offset', () => {
    const cnt = sql<number>`count(*)`.as('cnt');
    const compiled = makeSelectBuilder({ active: users.active, cnt })
      .from(users)
      .where(eq(users.active, true))
      .groupBy(users.active)
      .having(gt(cnt, 1))
      .orderBy(desc(users.active))
      .limit(10)
      .offset(5)
      .toSQL();
    const lower = compiled.sql.toLowerCase();
    expect(lower).toContain('where');
    expect(lower).toContain('group by');
    expect(lower).toContain('having');
    expect(lower).toContain('order by');
    expect(lower).toContain('limit');
    expect(lower).toContain('offset');
    expect(lower.indexOf('where')).toBeLessThan(lower.indexOf('group by'));
    expect(lower.indexOf('group by')).toBeLessThan(lower.indexOf('having'));
    expect(lower.indexOf('having')).toBeLessThan(lower.indexOf('order by'));
    expect(lower.indexOf('order by')).toBeLessThan(lower.indexOf('limit'));
    expect(lower.indexOf('limit')).toBeLessThan(lower.indexOf('offset'));
  });
});

describe('INSERT edge cases', () => {
  it('insert with raw sql expression as a value', () => {
    const compiled = makeInsertBuilder()
      .values({
        id: 'u1',
        name: 'Alice',
        age: 30,
        active: true,
        score: sql`CURRENT_TIMESTAMP()`,
      })
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('current_timestamp()');
    expect(compiled.params).toEqual(['u1', 'Alice', 30, true]);
  });

  it('throws when values() is called with empty array', () => {
    const builder = makeInsertBuilder();
    expect(() => builder.values([])).toThrow(
      /at least one value/,
    );
  });

  it('insert subset of columns — omitted columns default, only provided columns produce params', () => {
    const compiled = makeInsertBuilder()
      .values({ id: 'u1', name: 'Alice' })
      .toSQL();
    expect(compiled.sql).toContain('`id`');
    expect(compiled.sql).toContain('`name`');
    expect(compiled.sql.toLowerCase()).toContain('default');
    expect(compiled.params).toEqual(['u1', 'Alice']);
  });
});

describe('UPDATE edge cases', () => {
  it('update with or() in WHERE', () => {
    const compiled = makeUpdateBuilder()
      .set({ active: false })
      .where(or(eq(users.id, 'a'), eq(users.id, 'b')))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('update');
    expect(compiled.sql.toLowerCase()).toContain(' or ');
    expect(compiled.params).toEqual([false, 'a', 'b']);
  });

  it('update with complex AND/OR WHERE', () => {
    const compiled = makeUpdateBuilder()
      .set({ active: false })
      .where(and(or(eq(users.id, 'a'), eq(users.id, 'b')), gt(users.age, 21)))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain(' or ');
    expect(compiled.sql.toLowerCase()).toContain(' and ');
    expect(compiled.params).toEqual([false, 'a', 'b', 21]);
  });

  it('update sets multiple columns at once', () => {
    const compiled = makeUpdateBuilder()
      .set({ name: 'Alicia', age: 31, active: false })
      .where(eq(users.id, 'u1'))
      .toSQL();
    expect(compiled.sql).toContain('`name`');
    expect(compiled.sql).toContain('`age`');
    expect(compiled.sql).toContain('`active`');
    expect(compiled.params).toEqual(['Alicia', 31, false, 'u1']);
  });

  it('update can set a column to null', () => {
    const compiled = makeUpdateBuilder()
      .set({ name: null })
      .where(eq(users.id, 'u1'))
      .toSQL();
    expect(compiled.sql).toContain('`name` = ?');
    expect(compiled.params).toEqual([null, 'u1']);
  });
});

describe('DELETE edge cases', () => {
  it('delete with complex AND WHERE', () => {
    const compiled = makeDeleteBase()
      .where(and(gt(users.age, 18), lt(users.age, 65)))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('delete from');
    expect(compiled.sql.toLowerCase()).toContain(' and ');
    expect(compiled.params).toEqual([18, 65]);
  });

  it('delete with or() in WHERE', () => {
    const compiled = makeDeleteBase()
      .where(or(eq(users.id, 'a'), eq(users.id, 'b')))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain(' or ');
    expect(compiled.params).toEqual(['a', 'b']);
  });

  it('delete with between in WHERE', () => {
    const compiled = makeDeleteBase()
      .where(between(users.age, 18, 30))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('between');
    expect(compiled.params).toEqual([18, 30]);
  });
});

describe('DISTINCT edge cases', () => {
  it('selectDistinct with partial fields', () => {
    const compiled = makeSelectBuilder({ name: users.name, age: users.age }, true)
      .from(users)
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain('select distinct');
    expect(compiled.sql).toContain('`name`');
    expect(compiled.sql).toContain('`age`');
    expect(compiled.sql).not.toContain('`active`');
  });
});

describe('General builder properties', () => {
  it('toSQL() returns object with sql and params properties', () => {
    const compiled = makeSelectBuilder().from(users).toSQL();
    expect(compiled).toHaveProperty('sql');
    expect(compiled).toHaveProperty('params');
    expect(typeof compiled.sql).toBe('string');
    expect(Array.isArray(compiled.params)).toBe(true);
  });

  it('builder classes expose entityKind symbols', () => {
    expect((DatabricksSelectBuilder as any)[entityKind]).toBe('DatabricksSelectBuilder');
    expect((DatabricksSelectBase as any)[entityKind]).toBe('DatabricksSelect');
    expect((DatabricksInsertBuilder as any)[entityKind]).toBe('DatabricksInsertBuilder');
    expect((DatabricksInsertBase as any)[entityKind]).toBe('DatabricksInsert');
    expect((DatabricksUpdateBuilder as any)[entityKind]).toBe('DatabricksUpdateBuilder');
    expect((DatabricksUpdateBase as any)[entityKind]).toBe('DatabricksUpdate');
    expect((DatabricksDeleteBase as any)[entityKind]).toBe('DatabricksDelete');
  });
});
