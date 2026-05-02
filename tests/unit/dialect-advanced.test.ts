import { eq, sql } from "drizzle-orm";
import { describe, expect, it } from "vitest";

import { databricksTable, string, int, DatabricksDialect } from "../../src";
import { drizzle } from "../../src/driver";
import { MockDBSQLClient } from "../mocks/databricks-sql";

const users = databricksTable("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
});

const posts = databricksTable("posts", {
  id: int("id"),
  userId: string("user_id"),
  title: string("title"),
});

function createDb(rows: Record<string, unknown>[] = []) {
  const mockClient = new MockDBSQLClient();
  mockClient.queueResponse(rows);
  const db = drizzle({ client: mockClient as never });
  return { db, mockClient };
}

describe("Dialect advanced SQL generation", () => {
  describe("JOIN column aliasing", () => {
    it("full-select JOIN aliases every column as tableName__colName", async () => {
      const { db, mockClient } = createDb();
      await db.select().from(users).innerJoin(posts, eq(users.id, posts.userId));
      const s = mockClient.recorded[0]!.sql;
      expect(s).toContain("`users`.`id` as `users__id`");
      expect(s).toContain("`users`.`name` as `users__name`");
      expect(s).toContain("`users`.`age` as `users__age`");
      expect(s).toContain("`posts`.`id` as `posts__id`");
      expect(s).toContain("`posts`.`user_id` as `posts__user_id`");
      expect(s).toContain("`posts`.`title` as `posts__title`");
    });

    it("single-table SELECT does not add aliases", async () => {
      const { db, mockClient } = createDb();
      await db.select().from(users);
      const s = mockClient.recorded[0]!.sql;
      expect(s).not.toContain(" as ");
      expect(s).toBe("select `id`, `name`, `age` from `users`");
    });

    it("partial select with JOIN uses column aliases too", async () => {
      const { db, mockClient } = createDb();
      await db
        .select({ userName: users.name, postTitle: posts.title })
        .from(users)
        .innerJoin(posts, eq(users.id, posts.userId));
      const s = mockClient.recorded[0]!.sql;
      expect(s).toContain("`users`.`name` as `users__name`");
      expect(s).toContain("`posts`.`title` as `posts__title`");
    });
  });

  describe("buildLimit edge cases", () => {
    it("limit(0) generates LIMIT 0", async () => {
      const { db, mockClient } = createDb();
      await db.select().from(users).limit(0);
      expect(mockClient.recorded[0]!.sql).toContain("limit ?");
      expect(mockClient.recorded[0]!.params).toEqual([0]);
    });
  });

  describe("sqlToQuery", () => {
    it("handles sql.raw() with no params", () => {
      const dialect = new DatabricksDialect();
      const result = dialect.sqlToQuery(sql.raw("SELECT 1"));
      expect(result.sql).toBe("SELECT 1");
      expect(result.params).toEqual([]);
    });

    it("handles nested sql`` fragments", () => {
      const dialect = new DatabricksDialect();
      const inner = sql`age > ${18}`;
      const outer = sql`SELECT * FROM users WHERE ${inner}`;
      const result = dialect.sqlToQuery(outer);
      expect(result.sql).toBe("SELECT * FROM users WHERE age > ?");
      expect(result.params).toEqual([18]);
    });

    it("handles null parameter", () => {
      const dialect = new DatabricksDialect();
      const result = dialect.sqlToQuery(sql`SELECT * FROM users WHERE name = ${null}`);
      expect(result.sql).toBe("SELECT * FROM users WHERE name = ?");
      expect(result.params).toEqual([null]);
    });
  });
});
