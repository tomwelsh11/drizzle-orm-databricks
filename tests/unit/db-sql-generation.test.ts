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
} from "drizzle-orm";
import { describe, expect, it } from "vitest";

import { databricksTable, string, int, boolean, double } from "../../src";
import { drizzle } from "../../src/driver";
import { MockDBSQLClient } from "../mocks/databricks-sql";

const users = databricksTable("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
  active: boolean("active"),
  score: double("score"),
});

const posts = databricksTable("posts", {
  id: int("id"),
  userId: string("user_id"),
  title: string("title"),
  likes: int("likes"),
});

function createDb() {
  const mockClient = new MockDBSQLClient();
  mockClient.queueResponse([]);
  const db = drizzle({ client: mockClient as never });
  return { db, mockClient };
}

function createDbWithRows(rows: Record<string, unknown>[]) {
  const mockClient = new MockDBSQLClient();
  mockClient.queueResponse(rows);
  const db = drizzle({ client: mockClient as never });
  return { db, mockClient };
}

describe("db.select() SQL generation", () => {
  it("db.select().from(table) generates SELECT *", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users);
    expect(mockClient.recorded[0]!.sql).toBe(
      "select `id`, `name`, `age`, `active`, `score` from `users`",
    );
    expect(mockClient.recorded[0]!.params).toEqual([]);
  });

  it("db.select(partial).from(table) generates partial SELECT", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select({ id: users.id, name: users.name }).from(users);
    expect(mockClient.recorded[0]!.sql).toBe("select `id`, `name` from `users`");
  });

  it("db.select().from(table).where(eq()) generates WHERE =", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).where(eq(users.id, "u1"));
    expect(mockClient.recorded[0]!.sql).toBe(
      "select `id`, `name`, `age`, `active`, `score` from `users` where `users`.`id` = ?",
    );
    expect(mockClient.recorded[0]!.params).toEqual(["u1"]);
  });

  it("db.select().from(table).where(ne()) generates WHERE <>", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).where(ne(users.id, "u1"));
    expect(mockClient.recorded[0]!.sql).toContain("<>");
    expect(mockClient.recorded[0]!.params).toEqual(["u1"]);
  });

  it("db.select().from(table).where(gt()) generates WHERE >", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).where(gt(users.age, 25));
    expect(mockClient.recorded[0]!.sql).toBe(
      "select `id`, `name`, `age`, `active`, `score` from `users` where `users`.`age` > ?",
    );
    expect(mockClient.recorded[0]!.params).toEqual([25]);
  });

  it("db.select().from(table).where(lt()) generates WHERE <", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).where(lt(users.age, 25));
    expect(mockClient.recorded[0]!.sql).toContain("< ?");
    expect(mockClient.recorded[0]!.params).toEqual([25]);
  });

  it("db.select().from(table).where(gte()) generates WHERE >=", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).where(gte(users.age, 18));
    expect(mockClient.recorded[0]!.sql).toContain(">= ?");
    expect(mockClient.recorded[0]!.params).toEqual([18]);
  });

  it("db.select().from(table).where(lte()) generates WHERE <=", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).where(lte(users.age, 65));
    expect(mockClient.recorded[0]!.sql).toContain("<= ?");
    expect(mockClient.recorded[0]!.params).toEqual([65]);
  });

  it("db.select().from(table).where(and()) generates WHERE ... AND ...", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db
      .select()
      .from(users)
      .where(and(eq(users.active, true), gt(users.age, 25)));
    expect(mockClient.recorded[0]!.sql).toContain("and");
    expect(mockClient.recorded[0]!.params).toEqual([true, 25]);
  });

  it("db.select().from(table).where(or()) generates WHERE ... OR ...", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db
      .select()
      .from(users)
      .where(or(eq(users.id, "a"), eq(users.id, "b")));
    expect(mockClient.recorded[0]!.sql).toContain("or");
    expect(mockClient.recorded[0]!.params).toEqual(["a", "b"]);
  });

  it("db.select().from(table).where(like()) generates WHERE LIKE", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).where(like(users.name, "%ali%"));
    expect(mockClient.recorded[0]!.sql).toContain("like");
    expect(mockClient.recorded[0]!.params).toEqual(["%ali%"]);
  });

  it("db.select().from(table).where(isNull()) generates WHERE IS NULL", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).where(isNull(users.name));
    expect(mockClient.recorded[0]!.sql).toContain("is null");
    expect(mockClient.recorded[0]!.params).toEqual([]);
  });

  it("db.select().from(table).where(isNotNull()) generates WHERE IS NOT NULL", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).where(isNotNull(users.name));
    expect(mockClient.recorded[0]!.sql).toContain("is not null");
    expect(mockClient.recorded[0]!.params).toEqual([]);
  });

  it("db.select().from(table).where(inArray()) generates WHERE IN", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db
      .select()
      .from(users)
      .where(inArray(users.id, ["a", "b", "c"]));
    expect(mockClient.recorded[0]!.sql).toContain("in (");
    expect(mockClient.recorded[0]!.params).toEqual(["a", "b", "c"]);
  });

  it("db.select().from(table).where(notInArray()) generates WHERE NOT IN", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db
      .select()
      .from(users)
      .where(notInArray(users.id, ["x", "y"]));
    expect(mockClient.recorded[0]!.sql).toContain("not in (");
    expect(mockClient.recorded[0]!.params).toEqual(["x", "y"]);
  });

  it("db.select().from(table).where(between()) generates WHERE BETWEEN", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db
      .select()
      .from(users)
      .where(between(users.age, 18, 65));
    expect(mockClient.recorded[0]!.sql).toContain("between");
    expect(mockClient.recorded[0]!.params).toEqual([18, 65]);
  });

  it("db.select().from(table).where(nested and/or) generates complex WHERE", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db
      .select()
      .from(users)
      .where(and(or(eq(users.name, "Alice"), eq(users.name, "Bob")), gt(users.age, 20)));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("or");
    expect(s).toContain("and");
    expect(mockClient.recorded[0]!.params).toEqual(["Alice", "Bob", 20]);
  });

  it("db.select().from(table).orderBy(asc()) generates ORDER BY ASC", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).orderBy(asc(users.age));
    expect(mockClient.recorded[0]!.sql).toContain("order by `users`.`age` asc");
  });

  it("db.select().from(table).orderBy(desc()) generates ORDER BY DESC", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).orderBy(desc(users.age));
    expect(mockClient.recorded[0]!.sql).toContain("order by `users`.`age` desc");
  });

  it("db.select().from(table).orderBy(multi) generates compound ORDER BY", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).orderBy(asc(users.active), desc(users.score));
    expect(mockClient.recorded[0]!.sql).toContain(
      "order by `users`.`active` asc, `users`.`score` desc",
    );
  });

  it("db.select().from(table).limit() generates LIMIT", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).limit(10);
    expect(mockClient.recorded[0]!.sql).toContain("limit ?");
    expect(mockClient.recorded[0]!.params).toEqual([10]);
  });

  it("db.select().from(table).limit().offset() generates LIMIT + OFFSET", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).limit(10).offset(20);
    expect(mockClient.recorded[0]!.sql).toContain("limit ? offset ?");
    expect(mockClient.recorded[0]!.params).toEqual([10, 20]);
  });

  it("db.selectDistinct().from(table) generates SELECT DISTINCT", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.selectDistinct({ active: users.active }).from(users);
    expect(mockClient.recorded[0]!.sql).toBe("select distinct `active` from `users`");
  });

  it("db.select() with sql template in fields generates aliased expression", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select({ cnt: sql<number>`count(*)`.as("cnt") }).from(users);
    expect(mockClient.recorded[0]!.sql).toBe("select count(*) as `cnt` from `users`");
  });

  it("db.select().from().groupBy() generates GROUP BY", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db
      .select({ active: users.active, cnt: sql<number>`count(*)`.as("cnt") })
      .from(users)
      .groupBy(users.active);
    expect(mockClient.recorded[0]!.sql).toContain("group by");
  });

  it("chained where + orderBy + limit + offset generates full query", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db
      .select()
      .from(users)
      .where(eq(users.active, true))
      .orderBy(desc(users.age))
      .limit(10)
      .offset(5);
    const s = mockClient.recorded[0]!.sql;
    expect(s).toBe(
      "select `id`, `name`, `age`, `active`, `score` from `users` where `users`.`active` = ? order by `users`.`age` desc limit ? offset ?",
    );
    expect(mockClient.recorded[0]!.params).toEqual([true, 10, 5]);
  });
});

describe("db.insert() SQL generation", () => {
  it("db.insert(table).values(single) generates INSERT", async () => {
    const { db, mockClient } = createDb();
    await db.insert(users).values({ id: "u1", name: "Alice", age: 30, active: true, score: 9.5 });
    expect(mockClient.recorded[0]!.sql).toBe(
      "insert into `users` (`id`, `name`, `age`, `active`, `score`) values (?, ?, ?, ?, ?)",
    );
    expect(mockClient.recorded[0]!.params).toEqual(["u1", "Alice", 30, true, 9.5]);
  });

  it("db.insert(table).values(multi) generates multi-row INSERT", async () => {
    const { db, mockClient } = createDb();
    await db.insert(users).values([
      { id: "u1", name: "Alice", age: 30, active: true, score: 9.0 },
      { id: "u2", name: "Bob", age: 25, active: false, score: 8.0 },
    ]);
    expect(mockClient.recorded[0]!.sql).toBe(
      "insert into `users` (`id`, `name`, `age`, `active`, `score`) values (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)",
    );
    expect(mockClient.recorded[0]!.params).toEqual([
      "u1",
      "Alice",
      30,
      true,
      9.0,
      "u2",
      "Bob",
      25,
      false,
      8.0,
    ]);
  });

  it("db.insert(table).values with null generates NULL param", async () => {
    const { db, mockClient } = createDb();
    await db.insert(users).values({ id: "u1", name: null, age: null, active: null, score: null });
    expect(mockClient.recorded[0]!.params).toEqual(["u1", null, null, null, null]);
  });
});

describe("db.update() SQL generation", () => {
  it("db.update(table).set().where(eq()) generates UPDATE with WHERE", async () => {
    const { db, mockClient } = createDb();
    await db.update(users).set({ name: "Alicia" }).where(eq(users.id, "u1"));
    expect(mockClient.recorded[0]!.sql).toBe(
      "update `users` set `name` = ? where `users`.`id` = ?",
    );
    expect(mockClient.recorded[0]!.params).toEqual(["Alicia", "u1"]);
  });

  it("db.update(table).set(multi) generates multi-column SET", async () => {
    const { db, mockClient } = createDb();
    await db.update(users).set({ name: "Bob", age: 99, active: false }).where(eq(users.id, "u1"));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("`name` = ?");
    expect(s).toContain("`age` = ?");
    expect(s).toContain("`active` = ?");
    expect(s).toContain("where");
  });

  it("db.update(table).set() without where generates UPDATE without WHERE", async () => {
    const { db, mockClient } = createDb();
    await db.update(users).set({ active: false });
    expect(mockClient.recorded[0]!.sql).toBe("update `users` set `active` = ?");
    expect(mockClient.recorded[0]!.params).toEqual([false]);
  });

  it("db.update(table).set({ col: null }) generates NULL param", async () => {
    const { db, mockClient } = createDb();
    await db.update(users).set({ name: null }).where(eq(users.id, "u1"));
    expect(mockClient.recorded[0]!.params).toEqual([null, "u1"]);
  });

  it("db.update with or() in WHERE generates correct SQL", async () => {
    const { db, mockClient } = createDb();
    await db
      .update(users)
      .set({ active: true })
      .where(or(eq(users.id, "a"), eq(users.id, "b")));
    expect(mockClient.recorded[0]!.sql).toContain("or");
    expect(mockClient.recorded[0]!.params).toEqual([true, "a", "b"]);
  });
});

describe("db.delete() SQL generation", () => {
  it("db.delete(table).where(eq()) generates DELETE with WHERE", async () => {
    const { db, mockClient } = createDb();
    await db.delete(users).where(eq(users.id, "u1"));
    expect(mockClient.recorded[0]!.sql).toBe("delete from `users` where `users`.`id` = ?");
    expect(mockClient.recorded[0]!.params).toEqual(["u1"]);
  });

  it("db.delete(table) without where generates DELETE without WHERE", async () => {
    const { db, mockClient } = createDb();
    await db.delete(users);
    expect(mockClient.recorded[0]!.sql).toBe("delete from `users`");
    expect(mockClient.recorded[0]!.params).toEqual([]);
  });

  it("db.delete with and() in WHERE generates correct SQL", async () => {
    const { db, mockClient } = createDb();
    await db.delete(users).where(and(eq(users.active, false), lt(users.age, 18)));
    expect(mockClient.recorded[0]!.sql).toContain("and");
    expect(mockClient.recorded[0]!.params).toEqual([false, 18]);
  });

  it("db.delete with complex nested WHERE generates correct SQL", async () => {
    const { db, mockClient } = createDb();
    await db.delete(users).where(and(or(eq(users.id, "a"), eq(users.id, "b")), gt(users.age, 30)));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("or");
    expect(s).toContain("and");
    expect(mockClient.recorded[0]!.params).toEqual(["a", "b", 30]);
  });
});

describe("db.select() with JOINs SQL generation", () => {
  it("db.select().from().innerJoin() generates INNER JOIN", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).innerJoin(posts, eq(users.id, posts.userId));
    expect(mockClient.recorded[0]!.sql).toContain("inner join `posts`");
    expect(mockClient.recorded[0]!.sql).toContain("on");
  });

  it("db.select().from().leftJoin() generates LEFT JOIN", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).leftJoin(posts, eq(users.id, posts.userId));
    expect(mockClient.recorded[0]!.sql).toContain("left join `posts`");
  });

  it("db.select().from().rightJoin() generates RIGHT JOIN", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).rightJoin(posts, eq(users.id, posts.userId));
    expect(mockClient.recorded[0]!.sql).toContain("right join `posts`");
  });

  it("db.select().from().fullJoin() generates FULL JOIN", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db.select().from(users).fullJoin(posts, eq(users.id, posts.userId));
    expect(mockClient.recorded[0]!.sql).toContain("full join `posts`");
  });

  it("partial select with join generates flat column list", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db
      .select({ userName: users.name, postTitle: posts.title })
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("`users`.`name`");
    expect(s).toContain("`posts`.`title`");
    expect(s).toContain("inner join");
  });

  it("join with where generates JOIN + WHERE", async () => {
    const { db, mockClient } = createDbWithRows([]);
    await db
      .select()
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId))
      .where(gt(posts.likes, 10));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("inner join");
    expect(s).toContain("where");
    expect(mockClient.recorded[0]!.params).toEqual([10]);
  });
});
