import { eq, sql } from "drizzle-orm";
import { describe, expect, it } from "vitest";

import {
  databricksTable,
  databricksSchema,
  databricksCatalog,
  string,
  int,
  DatabricksDialect,
} from "../../src";
import { drizzle } from "../../src/driver";
import { MockDBSQLClient } from "../mocks/databricks-sql";

const plainUsers = databricksTable("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
});

const schemaUsers = databricksSchema("analytics").table("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
});

const catalog = databricksCatalog("prod");

const catalogSchemaUsers = catalog.schema("analytics").table("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
});

const catalogOnlyUsers = catalog.table("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
});

const catalogSchemaPosts = catalog.schema("analytics").table("posts", {
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

describe("Unity Catalog namespace qualification", () => {
  describe("qualifyTable helper", () => {
    const dialect = new DatabricksDialect();

    it("plain table: `table`", () => {
      const result = dialect.sqlToQuery(dialect.qualifyTable(plainUsers));
      expect(result.sql).toBe("`users`");
    });

    it("schema-only: `schema`.`table`", () => {
      const result = dialect.sqlToQuery(dialect.qualifyTable(schemaUsers));
      expect(result.sql).toBe("`analytics`.`users`");
    });

    it("catalog + schema: `catalog`.`schema`.`table`", () => {
      const result = dialect.sqlToQuery(dialect.qualifyTable(catalogSchemaUsers));
      expect(result.sql).toBe("`prod`.`analytics`.`users`");
    });

    it("catalog without schema: `catalog`.`table`", () => {
      const result = dialect.sqlToQuery(dialect.qualifyTable(catalogOnlyUsers));
      expect(result.sql).toBe("`prod`.`users`");
    });
  });

  describe("SELECT", () => {
    it("schema-qualified table in FROM clause", async () => {
      const { db, mockClient } = createDb();
      await db.select().from(schemaUsers);
      expect(mockClient.recorded[0]!.sql).toBe(
        "select `id`, `name`, `age` from `analytics`.`users`",
      );
    });

    it("catalog.schema.table in FROM clause", async () => {
      const { db, mockClient } = createDb();
      await db.select().from(catalogSchemaUsers);
      expect(mockClient.recorded[0]!.sql).toBe(
        "select `id`, `name`, `age` from `prod`.`analytics`.`users`",
      );
    });

    it("catalog-only table in FROM clause", async () => {
      const { db, mockClient } = createDb();
      await db.select().from(catalogOnlyUsers);
      expect(mockClient.recorded[0]!.sql).toBe("select `id`, `name`, `age` from `prod`.`users`");
    });

    it("catalog.schema.table with WHERE", async () => {
      const { db, mockClient } = createDb();
      await db.select().from(catalogSchemaUsers).where(eq(catalogSchemaUsers.id, "u1"));
      expect(mockClient.recorded[0]!.sql).toBe(
        "select `id`, `name`, `age` from `prod`.`analytics`.`users` where `analytics`.`users`.`id` = ?",
      );
    });
  });

  describe("INSERT", () => {
    it("schema-qualified table", async () => {
      const { db, mockClient } = createDb();
      await db.insert(schemaUsers).values({ id: "u1", name: "Alice", age: 30 });
      expect(mockClient.recorded[0]!.sql).toBe(
        "insert into `analytics`.`users` (`id`, `name`, `age`) values (?, ?, ?)",
      );
    });

    it("catalog.schema.table", async () => {
      const { db, mockClient } = createDb();
      await db.insert(catalogSchemaUsers).values({ id: "u1", name: "Alice", age: 30 });
      expect(mockClient.recorded[0]!.sql).toBe(
        "insert into `prod`.`analytics`.`users` (`id`, `name`, `age`) values (?, ?, ?)",
      );
    });
  });

  describe("UPDATE", () => {
    it("schema-qualified table", async () => {
      const { db, mockClient } = createDb();
      await db.update(schemaUsers).set({ name: "Bob" }).where(eq(schemaUsers.id, "u1"));
      expect(mockClient.recorded[0]!.sql).toBe(
        "update `analytics`.`users` set `name` = ? where `analytics`.`users`.`id` = ?",
      );
    });

    it("catalog.schema.table", async () => {
      const { db, mockClient } = createDb();
      await db
        .update(catalogSchemaUsers)
        .set({ name: "Bob" })
        .where(eq(catalogSchemaUsers.id, "u1"));
      expect(mockClient.recorded[0]!.sql).toBe(
        "update `prod`.`analytics`.`users` set `name` = ? where `analytics`.`users`.`id` = ?",
      );
    });
  });

  describe("DELETE", () => {
    it("schema-qualified table", async () => {
      const { db, mockClient } = createDb();
      await db.delete(schemaUsers).where(eq(schemaUsers.id, "u1"));
      expect(mockClient.recorded[0]!.sql).toBe(
        "delete from `analytics`.`users` where `analytics`.`users`.`id` = ?",
      );
    });

    it("catalog.schema.table", async () => {
      const { db, mockClient } = createDb();
      await db.delete(catalogSchemaUsers).where(eq(catalogSchemaUsers.id, "u1"));
      expect(mockClient.recorded[0]!.sql).toBe(
        "delete from `prod`.`analytics`.`users` where `analytics`.`users`.`id` = ?",
      );
    });
  });

  describe("JOINs", () => {
    it("JOIN between two catalog.schema tables", async () => {
      const { db, mockClient } = createDb();
      await db
        .select()
        .from(catalogSchemaUsers)
        .innerJoin(catalogSchemaPosts, eq(catalogSchemaUsers.id, catalogSchemaPosts.userId));
      const s = mockClient.recorded[0]!.sql;
      expect(s).toContain("from `prod`.`analytics`.`users`");
      expect(s).toContain("inner join `prod`.`analytics`.`posts`");
    });

    it("JOIN between plain and catalog-qualified tables", async () => {
      const { db, mockClient } = createDb();
      await db
        .select()
        .from(plainUsers)
        .innerJoin(catalogSchemaPosts, eq(plainUsers.id, catalogSchemaPosts.userId));
      const s = mockClient.recorded[0]!.sql;
      expect(s).toContain("from `users`");
      expect(s).toContain("inner join `prod`.`analytics`.`posts`");
    });
  });

  describe("databricksCatalog API", () => {
    it("exposes catalogName", () => {
      const cat = databricksCatalog("my_catalog");
      expect(cat.catalogName).toBe("my_catalog");
    });

    it("schema() returns a DatabricksSchema", () => {
      const cat = databricksCatalog("my_catalog");
      const sch = cat.schema("my_schema");
      expect(sch.schemaName).toBe("my_schema");
    });

    it("table() creates a catalog-only qualified table", () => {
      const dialect = new DatabricksDialect();
      const t = databricksCatalog("cat").table("tbl", { id: string("id") });
      const result = dialect.sqlToQuery(dialect.qualifyTable(t));
      expect(result.sql).toBe("`cat`.`tbl`");
    });

    it("schema().table() creates a fully qualified table", () => {
      const dialect = new DatabricksDialect();
      const t = databricksCatalog("cat")
        .schema("sch")
        .table("tbl", { id: string("id") });
      const result = dialect.sqlToQuery(dialect.qualifyTable(t));
      expect(result.sql).toBe("`cat`.`sch`.`tbl`");
    });
  });

  describe("special characters in identifiers", () => {
    it("escapes backticks in catalog, schema, and table names", async () => {
      const t = databricksCatalog("my`cat")
        .schema("my`sch")
        .table("my`tbl", {
          id: string("id"),
        });
      const { db, mockClient } = createDb();
      await db.select().from(t);
      expect(mockClient.recorded[0]!.sql).toBe("select `id` from `my``cat`.`my``sch`.`my``tbl`");
    });
  });
});
