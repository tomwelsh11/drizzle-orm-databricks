import { eq, sql, desc } from "drizzle-orm";
import { describe, expect, it, vi } from "vitest";

import { databricksTable, string, int, boolean, double, DatabricksDialect } from "../../src";
import { drizzle } from "../../src/driver";
import { MockDBSQLClient } from "../mocks/databricks-sql";

const users = databricksTable("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
  active: boolean("active"),
});

const posts = databricksTable("posts", {
  id: int("id"),
  userId: string("user_id"),
  title: string("title"),
});

describe("Result mapping", () => {
  it("maps NULL values from row to null in result", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ id: "u1", name: null, age: 30, active: true }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users);
    expect(rows).toEqual([{ id: "u1", name: null, age: 30, active: true }]);
  });

  it("maps a row where every column is NULL", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ id: null, name: null, age: null, active: null }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users);
    expect(rows).toEqual([{ id: null, name: null, age: null, active: null }]);
  });

  it("returns an empty array for an empty result set", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users);
    expect(rows).toEqual([]);
  });

  it("maps multiple rows correctly", async () => {
    const mockClient = new MockDBSQLClient();
    const queued = [
      { id: "u1", name: "Alice", age: 30, active: true },
      { id: "u2", name: "Bob", age: 25, active: false },
      { id: "u3", name: "Carol", age: 40, active: true },
      { id: "u4", name: "Dave", age: 22, active: false },
      { id: "u5", name: "Eve", age: 35, active: true },
    ];
    mockClient.queueResponse(queued);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users);
    expect(rows).toEqual(queued);
    expect(rows).toHaveLength(5);
  });

  it("maps a partial select where the selected column is NULL", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ name: null }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select({ name: users.name }).from(users);
    expect(rows).toEqual([{ name: null }]);
  });

  it("maps boolean false (not null/0)", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ id: "u1", name: "Alice", age: 30, active: false }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users);
    expect(rows[0]!.active).toBe(false);
    expect(rows[0]!.active).not.toBeNull();
  });

  it("maps numeric zero (not null/false)", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ id: "u1", name: "Alice", age: 0, active: true }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users);
    expect(rows[0]!.age).toBe(0);
    expect(rows[0]!.age).not.toBeNull();
    expect(rows[0]!.age).not.toBe(false);
  });

  it("maps strings with special characters (quotes, backticks, unicode)", async () => {
    const mockClient = new MockDBSQLClient();
    const tricky = `O'Brien \`backtick\` 日本語 🚀 "double" \\ slash`;
    mockClient.queueResponse([{ id: "u1", name: tricky, age: 1, active: true }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users);
    expect(rows[0]!.name).toBe(tricky);
  });

  it("maps empty string to empty string (not null)", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ id: "u1", name: "", age: 1, active: true }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users);
    expect(rows[0]!.name).toBe("");
    expect(rows[0]!.name).not.toBeNull();
  });
});

describe("JOIN result mapping", () => {
  it("maps inner join full select to nested shape", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([
      {
        users__id: 1,
        users__name: "Alice",
        users__age: 30,
        users__active: true,
        posts__id: 1,
        posts__user_id: "u1",
        posts__title: "Post1",
      },
    ]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users).innerJoin(posts, eq(users.id, posts.userId));

    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual({
      users: { id: 1, name: "Alice", age: 30, active: true },
      posts: { id: 1, userId: "u1", title: "Post1" },
    });
  });

  it("maps left join with all-null right side to null", async () => {
    const profiles = databricksTable("profiles", {
      userId: string("user_id"),
      bio: string("bio"),
    });
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([
      {
        users__id: "u1",
        users__name: "Alice",
        users__age: 30,
        users__active: true,
        profiles__user_id: null,
        profiles__bio: null,
      },
    ]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users).leftJoin(profiles, eq(users.id, profiles.userId));

    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual({
      users: { id: "u1", name: "Alice", age: 30, active: true },
      profiles: null,
    });
  });

  it("maps a partial select with join to a flat shape", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ users__name: "Alice", posts__title: "Hello" }]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db
      .select({ userName: users.name, postTitle: posts.title })
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId));

    expect(rows).toEqual([{ userName: "Alice", postTitle: "Hello" }]);
  });
});

describe("Dialect edge cases", () => {
  it("escapeName escapes backticks inside identifiers", () => {
    const dialect = new DatabricksDialect();
    expect(dialect.escapeName("a`b")).toBe("`a``b`");
    expect(dialect.escapeName("``")).toBe("``````");
    expect(dialect.escapeName("plain")).toBe("`plain`");
  });

  it("escapeParam always returns ? regardless of index/value", () => {
    const dialect = new DatabricksDialect();
    expect(dialect.escapeParam(0, "value")).toBe("?");
    expect(dialect.escapeParam(1, null)).toBe("?");
    expect(dialect.escapeParam(99, undefined)).toBe("?");
    expect(dialect.escapeParam(-1, { complex: "object" })).toBe("?");
  });

  it("escapeString doubles single quotes", () => {
    const dialect = new DatabricksDialect();
    expect(dialect.escapeString("a'b")).toBe("'a''b'");
    expect(dialect.escapeString("''")).toBe("''''''");
    expect(dialect.escapeString("no quotes")).toBe("'no quotes'");
  });

  it("buildLimit with 0 still produces a limit clause", () => {
    const dialect = new DatabricksDialect();
    const result = dialect.buildLimit(0);
    expect(result).toBeDefined();
    const compiled = dialect.sqlToQuery(result!);
    expect(compiled.sql).toBe(" limit ?");
    expect(compiled.params).toEqual([0]);
  });

  it("buildLimit with undefined returns undefined", () => {
    const dialect = new DatabricksDialect();
    expect(dialect.buildLimit(undefined)).toBeUndefined();
  });

  it("buildOrderBy with empty array returns undefined", () => {
    const dialect = new DatabricksDialect();
    expect(dialect.buildOrderBy([])).toBeUndefined();
  });

  it("buildOrderBy with undefined returns undefined", () => {
    const dialect = new DatabricksDialect();
    expect(dialect.buildOrderBy(undefined)).toBeUndefined();
  });
});

describe("Driver integration edge cases", () => {
  it("db.execute with sql template extracts params correctly", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ x: 1 }]);
    const db = drizzle({ client: mockClient as never });

    await db.execute(sql`SELECT * FROM users WHERE id = ${"u1"} AND age > ${20}`);

    expect(mockClient.recorded[0]!.sql).toBe("SELECT * FROM users WHERE id = ? AND age > ?");
    expect(mockClient.recorded[0]!.params).toEqual(["u1", 20]);
  });

  it("runs db.select then db.insert sequentially", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ id: "u1", name: "Alice", age: 30, active: true }]);
    mockClient.queueResponse([]);
    const db = drizzle({ client: mockClient as never });

    const selected = await db.select().from(users);
    expect(selected).toEqual([{ id: "u1", name: "Alice", age: 30, active: true }]);

    await db.insert(users).values({ id: "u2", name: "Bob", age: 25, active: false });

    expect(mockClient.recorded).toHaveLength(2);
    expect(mockClient.recorded[0]!.sql).toBe("select `id`, `name`, `age`, `active` from `users`");
    expect(mockClient.recorded[1]!.sql).toBe(
      "insert into `users` (`id`, `name`, `age`, `active`) values (?, ?, ?, ?)",
    );
  });

  it("propagates errors from the underlying client wrapped in DrizzleQueryError", async () => {
    const { DrizzleQueryError } = await import("drizzle-orm/errors");
    const mockClient = new MockDBSQLClient();
    mockClient.queueError(new Error("fail"));
    const db = drizzle({ client: mockClient as never });

    const err = await db
      .select()
      .from(users)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(DrizzleQueryError);
    expect((err as InstanceType<typeof DrizzleQueryError>).cause!.message).toBe("fail");
    expect((err as InstanceType<typeof DrizzleQueryError>).query).toContain("select");
  });

  it("logger receives correct SQL and params", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([]);
    const logQuery = vi.fn();
    const db = drizzle({ client: mockClient as never }, { logger: { logQuery } });

    await db.select().from(users).where(eq(users.id, "u1"));

    expect(logQuery).toHaveBeenCalledTimes(1);
    expect(logQuery).toHaveBeenCalledWith(
      "select `id`, `name`, `age`, `active` from `users` where `users`.`id` = ?",
      ["u1"],
    );
  });

  it("selectDistinct generates DISTINCT keyword in SQL", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([{ name: "Alice" }]);
    const db = drizzle({ client: mockClient as never });

    await db.selectDistinct({ name: users.name }).from(users);

    expect(mockClient.recorded[0]!.sql).toBe("select distinct `name` from `users`");
  });
});

describe("Result mapping with extra column types", () => {
  it("maps double values correctly including zero and negative", async () => {
    const measurements = databricksTable("measurements", {
      id: string("id"),
      value: double("value"),
    });
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([
      { id: "m1", value: 0 },
      { id: "m2", value: -1.5 },
      { id: "m3", value: 3.14159 },
      { id: "m4", value: null },
    ]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(measurements);
    expect(rows).toEqual([
      { id: "m1", value: 0 },
      { id: "m2", value: -1.5 },
      { id: "m3", value: 3.14159 },
      { id: "m4", value: null },
    ]);
  });

  it("respects orderBy/desc against mocked rows (driver-side ordering not checked)", async () => {
    const mockClient = new MockDBSQLClient();
    mockClient.queueResponse([
      { id: "u1", name: "Alice", age: 40, active: true },
      { id: "u2", name: "Bob", age: 30, active: false },
    ]);
    const db = drizzle({ client: mockClient as never });

    const rows = await db.select().from(users).orderBy(desc(users.age)).limit(2);

    expect(mockClient.recorded[0]!.sql).toBe(
      "select `id`, `name`, `age`, `active` from `users` order by `users`.`age` desc limit ?",
    );
    expect(mockClient.recorded[0]!.params).toEqual([2]);
    expect(rows).toHaveLength(2);
  });
});
