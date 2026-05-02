import { eq, gt, sql } from "drizzle-orm";
import { describe, expect, it } from "vitest";

import { boolean, databricksTable, double, int, string } from "../../src";
import { drizzle } from "../../src/driver";
import { MockDBSQLClient } from "../mocks/databricks-sql";

const users = databricksTable("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
  active: boolean("active"),
  score: double("score"),
});

const usersArchive = databricksTable("users_archive", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
  active: boolean("active"),
  score: double("score"),
});

function createDb() {
  const mockClient = new MockDBSQLClient();
  mockClient.queueResponse([]);
  const db = drizzle({ client: mockClient as never });
  return { db, mockClient };
}

describe("INSERT ... SELECT", () => {
  it("supports db.insert(table).select(subquery) with a Drizzle select", async () => {
    const { db, mockClient } = createDb();
    await db.insert(usersArchive).select(db.select().from(users));
    expect(mockClient.recorded[0]!.sql).toBe(
      "insert into `users_archive` (`id`, `name`, `age`, `active`, `score`) " +
        "select `id`, `name`, `age`, `active`, `score` from `users`",
    );
    expect(mockClient.recorded[0]!.params).toEqual([]);
  });

  it("supports db.insert(table).select(qb => ...) callback form", async () => {
    const { db, mockClient } = createDb();
    await db
      .insert(usersArchive)
      .select((qb) => qb.select().from(users).where(eq(users.active, true)));
    expect(mockClient.recorded[0]!.sql).toBe(
      "insert into `users_archive` (`id`, `name`, `age`, `active`, `score`) " +
        "select `id`, `name`, `age`, `active`, `score` from `users` where `users`.`active` = ?",
    );
    expect(mockClient.recorded[0]!.params).toEqual([true]);
  });

  it("supports db.insert(table).select(rawSql)", async () => {
    const { db, mockClient } = createDb();
    await db.insert(usersArchive).select(sql`select * from \`users\``);
    expect(mockClient.recorded[0]!.sql).toBe(
      "insert into `users_archive` (`id`, `name`, `age`, `active`, `score`) select * from `users`",
    );
  });

  it("throws when selected fields do not match table columns", async () => {
    const { db } = createDb();
    expect(() =>
      db.insert(usersArchive).select(db.select({ id: users.id, name: users.name }).from(users)),
    ).toThrow(/Insert select error/);
  });
});

describe("WITH (CTE) on DML", () => {
  it("supports db.with(cte).insert(table).select(...)", async () => {
    const { db, mockClient } = createDb();
    const adults = db.$with("adults").as(db.select().from(users).where(gt(users.age, 17)));
    await db.with(adults).insert(usersArchive).select(db.select().from(adults));
    expect(mockClient.recorded[0]!.sql).toBe(
      "with `adults` as (select `id`, `name`, `age`, `active`, `score` from `users` " +
        "where `users`.`age` > ?) " +
        "insert into `users_archive` (`id`, `name`, `age`, `active`, `score`) " +
        "select `id`, `name`, `age`, `active`, `score` from `adults`",
    );
    expect(mockClient.recorded[0]!.params).toEqual([17]);
  });

  it("supports db.with(cte).update(table).set(...)", async () => {
    const { db, mockClient } = createDb();
    const seniors = db.$with("seniors").as(db.select().from(users).where(gt(users.age, 65)));
    await db.with(seniors).update(users).set({ active: false }).where(eq(users.id, "u1"));
    expect(mockClient.recorded[0]!.sql).toBe(
      "with `seniors` as (select `id`, `name`, `age`, `active`, `score` from `users` " +
        "where `users`.`age` > ?) " +
        "update `users` set `active` = ? where `users`.`id` = ?",
    );
    expect(mockClient.recorded[0]!.params).toEqual([65, false, "u1"]);
  });

  it("supports db.with(cte).delete(table)", async () => {
    const { db, mockClient } = createDb();
    const stale = db.$with("stale").as(db.select().from(users).where(eq(users.active, false)));
    await db.with(stale).delete(users).where(eq(users.id, "u1"));
    expect(mockClient.recorded[0]!.sql).toBe(
      "with `stale` as (select `id`, `name`, `age`, `active`, `score` from `users` " +
        "where `users`.`active` = ?) " +
        "delete from `users` where `users`.`id` = ?",
    );
    expect(mockClient.recorded[0]!.params).toEqual([false, "u1"]);
  });

  it("supports multiple CTEs on a DML statement", async () => {
    const { db, mockClient } = createDb();
    const a = db.$with("a").as(db.select().from(users).where(eq(users.active, true)));
    const b = db.$with("b").as(db.select().from(users).where(eq(users.active, false)));
    await db.with(a, b).delete(users);
    const recorded = mockClient.recorded[0]!.sql;
    expect(recorded).toContain("with `a` as (");
    expect(recorded).toContain("), `b` as (");
    expect(recorded).toContain("delete from `users`");
  });

  it("supports db.with(cte).select() (existing behavior preserved)", async () => {
    const { db, mockClient } = createDb();
    const cte = db.$with("cte").as(db.select().from(users));
    await db.with(cte).select().from(cte);
    expect(mockClient.recorded[0]!.sql).toContain("with `cte` as (");
    expect(mockClient.recorded[0]!.sql).toContain("from `cte`");
  });
});
