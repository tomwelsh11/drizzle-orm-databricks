import { eq, and, gt, sql, asc, desc } from 'drizzle-orm';
import { Param } from 'drizzle-orm/sql';
import { describe, expect, it } from 'vitest';

import {
  databricksTable,
  string,
  int,
  boolean,
  DatabricksDialect,
  DatabricksSelectBuilder,
  DatabricksInsertBuilder,
  DatabricksUpdateBuilder,
  DatabricksDeleteBase,
} from '../../src';

const users = databricksTable('users', {
  id: string('id'),
  name: string('name'),
  age: int('age'),
  active: boolean('active'),
});

const posts = databricksTable('posts', {
  id: int('id'),
  userId: string('user_id'),
  title: string('title'),
});

function toSQL(sqlObj: any): { sql: string; params: unknown[] } {
  const dialect = new DatabricksDialect();
  const compiled = dialect.sqlToQuery(sqlObj.getSQL());
  return { sql: compiled.sql, params: compiled.params };
}

describe('Select query builder', () => {
  const dialect = new DatabricksDialect();

  it('builds SELECT * FROM table', () => {
    const selectConfig = {
      withList: undefined,
      fields: { id: users.id, name: users.name, age: users.age, active: users.active },
      table: users,
      joins: undefined,
      orderBy: undefined,
      groupBy: undefined,
      limit: undefined,
      offset: undefined,
      distinct: undefined,
      setOperators: [],
    };
    const query = dialect.buildSelectQuery(selectConfig);
    const compiled = dialect.sqlToQuery(query);
    expect(compiled.sql).toBe('select `id`, `name`, `age`, `active` from `users`');
    expect(compiled.params).toEqual([]);
  });

  it('builds SELECT with WHERE clause', () => {
    const query = dialect.buildSelectQuery({
      fields: { id: users.id, name: users.name },
      table: users,
      where: eq(users.id, 'u1'),
      setOperators: [],
    });
    const compiled = dialect.sqlToQuery(query);
    expect(compiled.sql).toBe('select `id`, `name` from `users` where `users`.`id` = ?');
    expect(compiled.params).toEqual(['u1']);
  });

  it('builds SELECT with ORDER BY and LIMIT', () => {
    const query = dialect.buildSelectQuery({
      fields: { id: users.id, name: users.name, age: users.age },
      table: users,
      orderBy: [desc(users.age)],
      limit: 10,
      setOperators: [],
    });
    const compiled = dialect.sqlToQuery(query);
    expect(compiled.sql).toBe(
      'select `id`, `name`, `age` from `users` order by `users`.`age` desc limit ?',
    );
    expect(compiled.params).toEqual([10]);
  });

  it('builds SELECT DISTINCT', () => {
    const query = dialect.buildSelectQuery({
      fields: { name: users.name },
      table: users,
      distinct: true,
      setOperators: [],
    });
    const compiled = dialect.sqlToQuery(query);
    expect(compiled.sql).toBe('select distinct `name` from `users`');
  });

  it('builds SELECT with GROUP BY and HAVING', () => {
    const cnt = sql<number>`count(*)`.as('cnt');
    const query = dialect.buildSelectQuery({
      fields: { active: users.active, cnt },
      table: users,
      groupBy: [users.active],
      having: gt(cnt, 1),
      setOperators: [],
    });
    const compiled = dialect.sqlToQuery(query);
    expect(compiled.sql).toContain('group by');
    expect(compiled.sql).toContain('having');
  });

  it('builds SELECT with OFFSET', () => {
    const query = dialect.buildSelectQuery({
      fields: { id: users.id },
      table: users,
      limit: 5,
      offset: 10,
      setOperators: [],
    });
    const compiled = dialect.sqlToQuery(query);
    expect(compiled.sql).toBe('select `id` from `users` limit ? offset ?');
    expect(compiled.params).toEqual([5, 10]);
  });
});

describe('Insert query builder', () => {
  const dialect = new DatabricksDialect();

  it('builds INSERT with single row', () => {
    const { sql: insertSql } = dialect.buildInsertQuery({
      table: users,
      values: [
        {
          id: new Param('u1', users.id),
          name: new Param('Alice', users.name),
          age: new Param(30, users.age),
          active: new Param(true, users.active),
        },
      ],
    });
    const compiled = dialect.sqlToQuery(insertSql);
    expect(compiled.sql).toBe(
      'insert into `users` (`id`, `name`, `age`, `active`) values (?, ?, ?, ?)',
    );
    expect(compiled.params).toEqual(['u1', 'Alice', 30, true]);
  });

  it('builds INSERT with multiple rows', () => {
    const { sql: insertSql } = dialect.buildInsertQuery({
      table: users,
      values: [
        {
          id: new Param('u1', users.id),
          name: new Param('Alice', users.name),
          age: new Param(30, users.age),
          active: new Param(true, users.active),
        },
        {
          id: new Param('u2', users.id),
          name: new Param('Bob', users.name),
          age: new Param(25, users.age),
          active: new Param(false, users.active),
        },
      ],
    });
    const compiled = dialect.sqlToQuery(insertSql);
    expect(compiled.sql).toBe(
      'insert into `users` (`id`, `name`, `age`, `active`) values (?, ?, ?, ?), (?, ?, ?, ?)',
    );
    expect(compiled.params).toEqual(['u1', 'Alice', 30, true, 'u2', 'Bob', 25, false]);
  });
});

describe('Update query builder', () => {
  const dialect = new DatabricksDialect();

  it('builds UPDATE with SET and WHERE', () => {
    const query = dialect.buildUpdateQuery({
      table: users,
      set: {
        name: new Param('Alicia', users.name),
      },
      where: eq(users.id, 'u1'),
    });
    const compiled = dialect.sqlToQuery(query);
    expect(compiled.sql).toBe('update `users` set `name` = ? where `users`.`id` = ?');
    expect(compiled.params).toEqual(['Alicia', 'u1']);
  });

  it('builds UPDATE without WHERE (all rows)', () => {
    const query = dialect.buildUpdateQuery({
      table: users,
      set: {
        active: new Param(false, users.active),
      },
    });
    const compiled = dialect.sqlToQuery(query);
    expect(compiled.sql).toBe('update `users` set `active` = ?');
    expect(compiled.params).toEqual([false]);
  });
});

describe('Delete query builder', () => {
  const dialect = new DatabricksDialect();

  it('builds DELETE with WHERE', () => {
    const query = dialect.buildDeleteQuery({
      table: users,
      where: eq(users.id, 'u1'),
    });
    const compiled = dialect.sqlToQuery(query);
    expect(compiled.sql).toBe('delete from `users` where `users`.`id` = ?');
    expect(compiled.params).toEqual(['u1']);
  });

  it('builds DELETE without WHERE', () => {
    const query = dialect.buildDeleteQuery({
      table: users,
    });
    const compiled = dialect.sqlToQuery(query);
    expect(compiled.sql).toBe('delete from `users`');
  });
});

describe('Database query builder integration (mocked)', () => {
  it('db.select().from(table) generates correct SQL', async () => {
    const dialect = new DatabricksDialect();

    const builder = new DatabricksSelectBuilder({
      fields: undefined,
      session: null as any,
      dialect,
    });
    const selectBase = builder.from(users);
    const compiled = selectBase.toSQL();
    expect(compiled.sql).toBe('select `id`, `name`, `age`, `active` from `users`');
  });

  it('db.select(partial).from(table).where() generates correct SQL', async () => {
    const dialect = new DatabricksDialect();

    const builder = new DatabricksSelectBuilder({
      fields: { id: users.id, name: users.name },
      session: null as any,
      dialect,
    });
    const selectBase = builder.from(users).where(eq(users.active, true));
    const compiled = selectBase.toSQL();
    expect(compiled.sql).toBe(
      'select `id`, `name` from `users` where `users`.`active` = ?',
    );
    expect(compiled.params).toEqual([true]);
  });

  it('db.insert(table).values() generates correct SQL', () => {
    const dialect = new DatabricksDialect();

    const builder = new DatabricksInsertBuilder(users, null as any, dialect);
    const insertBase = builder.values({ id: 'u1', name: 'Alice', age: 30, active: true });
    const compiled = insertBase.toSQL();
    expect(compiled.sql).toBe(
      'insert into `users` (`id`, `name`, `age`, `active`) values (?, ?, ?, ?)',
    );
    expect(compiled.params).toEqual(['u1', 'Alice', 30, true]);
  });

  it('db.update(table).set().where() generates correct SQL', () => {
    const dialect = new DatabricksDialect();

    const builder = new DatabricksUpdateBuilder(users, null as any, dialect);
    const updateBase = builder.set({ name: 'Alicia' }).where(eq(users.id, 'u1'));
    const compiled = updateBase.toSQL();
    expect(compiled.sql).toBe('update `users` set `name` = ? where `users`.`id` = ?');
    expect(compiled.params).toEqual(['Alicia', 'u1']);
  });

  it('db.delete(table).where() generates correct SQL', () => {
    const dialect = new DatabricksDialect();

    const deleteBase = new DatabricksDeleteBase(users, null as any, dialect);
    deleteBase.where(eq(users.id, 'u1'));
    const compiled = deleteBase.toSQL();
    expect(compiled.sql).toBe('delete from `users` where `users`.`id` = ?');
    expect(compiled.params).toEqual(['u1']);
  });

  it('db.select().from().orderBy().limit().offset() generates correct SQL', () => {
    const dialect = new DatabricksDialect();

    const builder = new DatabricksSelectBuilder({
      fields: undefined,
      session: null as any,
      dialect,
    });
    const selectBase = builder
      .from(users)
      .where(eq(users.active, true))
      .orderBy(desc(users.age))
      .limit(10)
      .offset(20);
    const compiled = selectBase.toSQL();
    expect(compiled.sql).toBe(
      'select `id`, `name`, `age`, `active` from `users` where `users`.`active` = ? order by `users`.`age` desc limit ? offset ?',
    );
    expect(compiled.params).toEqual([true, 10, 20]);
  });

  it('db.insert(table).values([...]) generates correct SQL for multiple rows', () => {
    const dialect = new DatabricksDialect();

    const builder = new DatabricksInsertBuilder(users, null as any, dialect);
    const insertBase = builder.values([
      { id: 'u1', name: 'Alice', age: 30, active: true },
      { id: 'u2', name: 'Bob', age: 25, active: false },
    ]);
    const compiled = insertBase.toSQL();
    expect(compiled.sql).toBe(
      'insert into `users` (`id`, `name`, `age`, `active`) values (?, ?, ?, ?), (?, ?, ?, ?)',
    );
  });

  it('db.selectDistinct().from(table) generates correct SQL', () => {
    const dialect = new DatabricksDialect();

    const builder = new DatabricksSelectBuilder({
      fields: { name: users.name },
      session: null as any,
      dialect,
      distinct: true,
    });
    const selectBase = builder.from(users);
    const compiled = selectBase.toSQL();
    expect(compiled.sql).toBe('select distinct `name` from `users`');
  });
});
