import { eq, sql } from "drizzle-orm";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { databricksCatalog, databricksTable, int, string } from "../../src";
import { closeDb, getDb, hasCredentials } from "./helpers";

const env = {
  catalog: process.env["DATABRICKS_CATALOG"],
  schema: process.env["DATABRICKS_SCHEMA"],
};

const canRun = () => hasCredentials() && !!env.catalog && !!env.schema;

const bt = (n: string) => "`" + n + "`";

const tableName = "e2e_unity_catalog";

const catalog = databricksCatalog(env.catalog ?? "main");
const qualifiedTable = catalog.schema(env.schema ?? "default").table(tableName, {
  id: string("id"),
  name: string("name"),
  age: int("age"),
});

const plainTable = databricksTable(tableName, {
  id: string("id"),
  name: string("name"),
  age: int("age"),
});

describe.skipIf(!canRun())("Unity Catalog namespace (e2e)", () => {
  beforeAll(async () => {
    const db = getDb();
    await db.execute(
      sql.raw(
        `CREATE TABLE IF NOT EXISTS ${bt(env.catalog!)}.${bt(env.schema!)}.${bt(tableName)} (
          id STRING,
          name STRING,
          age INT
        ) USING DELTA`,
      ),
    );
    await db.execute(
      sql.raw(`DELETE FROM ${bt(env.catalog!)}.${bt(env.schema!)}.${bt(tableName)}`),
    );
  });

  afterAll(async () => {
    try {
      const db = getDb();
      await db.execute(
        sql.raw(`DROP TABLE IF EXISTS ${bt(env.catalog!)}.${bt(env.schema!)}.${bt(tableName)}`),
      );
    } finally {
      await closeDb();
    }
  });

  it("SELECT from catalog.schema.table", async () => {
    const db = getDb();
    await db.insert(qualifiedTable).values({ id: "u1", name: "Alice", age: 30 });

    const rows = await db.select().from(qualifiedTable).where(eq(qualifiedTable.id, "u1"));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.name).toBe("Alice");
  });

  it("INSERT into catalog.schema.table", async () => {
    const db = getDb();
    await db.insert(qualifiedTable).values({ id: "u2", name: "Bob", age: 25 });

    const rows = await db.select().from(qualifiedTable).where(eq(qualifiedTable.id, "u2"));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.name).toBe("Bob");
  });

  it("UPDATE on catalog.schema.table", async () => {
    const db = getDb();
    await db.update(qualifiedTable).set({ age: 31 }).where(eq(qualifiedTable.id, "u1"));

    const rows = await db.select().from(qualifiedTable).where(eq(qualifiedTable.id, "u1"));
    expect(rows[0]!.age).toBe(31);
  });

  it("DELETE from catalog.schema.table", async () => {
    const db = getDb();
    await db.insert(qualifiedTable).values({ id: "u3", name: "Carol", age: 40 });
    await db.delete(qualifiedTable).where(eq(qualifiedTable.id, "u3"));

    const rows = await db.select().from(qualifiedTable).where(eq(qualifiedTable.id, "u3"));
    expect(rows).toHaveLength(0);
  });

  it("per-query namespace override on plain table", async () => {
    const db = getDb();
    const rows = await db.select().from(plainTable, { catalog: env.catalog!, schema: env.schema! });
    expect(Array.isArray(rows)).toBe(true);
    expect(rows.length).toBeGreaterThan(0);
  });

  it("per-query override on INSERT", async () => {
    const db = getDb();
    await db
      .insert(plainTable, { catalog: env.catalog!, schema: env.schema! })
      .values({ id: "u4", name: "Dave", age: 50 });

    const rows = await db.select().from(qualifiedTable).where(eq(qualifiedTable.id, "u4"));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.name).toBe("Dave");
  });

  it("per-query override on UPDATE", async () => {
    const db = getDb();
    await db
      .update(plainTable, { catalog: env.catalog!, schema: env.schema! })
      .set({ age: 51 })
      .where(eq(plainTable.id, "u4"));

    const rows = await db.select().from(qualifiedTable).where(eq(qualifiedTable.id, "u4"));
    expect(rows[0]!.age).toBe(51);
  });

  it("per-query override on DELETE", async () => {
    const db = getDb();
    await db
      .delete(plainTable, { catalog: env.catalog!, schema: env.schema! })
      .where(eq(plainTable.id, "u4"));

    const rows = await db.select().from(qualifiedTable).where(eq(qualifiedTable.id, "u4"));
    expect(rows).toHaveLength(0);
  });
});
