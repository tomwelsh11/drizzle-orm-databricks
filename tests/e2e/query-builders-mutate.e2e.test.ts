import { and, eq, gt, isNull, or, sql } from "drizzle-orm";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { boolean, databricksTable, double, int, string } from "../../src";
import { closeDb, dropTable, getDb, hasCredentials } from "./helpers";

const bt = (n: string) => "`" + n + "`";

const tableName = "qb_mutate";
const tbl = bt(tableName);

const items = databricksTable(tableName, {
  id: string("id"),
  name: string("name"),
  value: int("value"),
  active: boolean("active"),
  score: double("score"),
});

describe.skipIf(!hasCredentials())("Query builder INSERT/UPDATE/DELETE (e2e)", () => {
  beforeAll(async () => {
    const db = getDb();
    await dropTable(db, tableName);
    await db.execute(
      sql.raw(
        `CREATE TABLE IF NOT EXISTS ${tbl} (
        id STRING,
        name STRING,
        value INT,
        active BOOLEAN,
        score DOUBLE
      ) USING DELTA`,
      ),
    );

    await db.insert(items).values([
      { id: "seed-1", name: "Seed One", value: 10, active: true, score: 1.5 },
      { id: "seed-2", name: "Seed Two", value: 20, active: false, score: 2.5 },
      { id: "seed-3", name: "Seed Three", value: 30, active: true, score: 3.5 },
    ]);
  });

  afterAll(async () => {
    try {
      await dropTable(getDb(), tableName);
    } finally {
      await closeDb();
    }
  });

  // ---------- INSERT ----------

  describe("INSERT", () => {
    it("inserts a single row and reads it back", async () => {
      const db = getDb();
      await db.insert(items).values({
        id: "insert-single",
        name: "Single Row",
        value: 42,
        active: true,
        score: 9.99,
      });

      const rows = await db.select().from(items).where(eq(items.id, "insert-single"));

      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        id: "insert-single",
        name: "Single Row",
        value: 42,
        active: true,
        score: 9.99,
      });
    });

    it("inserts multiple rows in one call", async () => {
      const db = getDb();
      await db.insert(items).values([
        { id: "insert-multi-1", name: "Multi 1", value: 1, active: true, score: 0.1 },
        { id: "insert-multi-2", name: "Multi 2", value: 2, active: false, score: 0.2 },
        { id: "insert-multi-3", name: "Multi 3", value: 3, active: true, score: 0.3 },
      ]);

      const rows = await db.execute<{ cnt: number }>(
        sql`SELECT COUNT(*) AS cnt FROM ${items} WHERE ${items.id} LIKE ${"insert-multi-%"}`,
      );
      expect(Number(rows[0]!.cnt)).toBe(3);
    });

    it("inserts a row with NULL values", async () => {
      const db = getDb();
      await db.insert(items).values({
        id: "insert-null",
        name: null,
        value: null,
        active: null,
        score: null,
      });

      const rows = await db.select().from(items).where(eq(items.id, "insert-null"));

      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        id: "insert-null",
        name: null,
        value: null,
        active: null,
        score: null,
      });
    });

    it("round-trips all column types", async () => {
      const db = getDb();
      await db.insert(items).values({
        id: "insert-types",
        name: "AllTypes",
        value: -123,
        active: false,
        score: 3.14159,
      });

      const rows = await db.select().from(items).where(eq(items.id, "insert-types"));

      expect(rows).toHaveLength(1);
      const row = rows[0]!;
      expect(typeof row.id).toBe("string");
      expect(typeof row.name).toBe("string");
      expect(typeof row.value).toBe("number");
      expect(typeof row.active).toBe("boolean");
      expect(typeof row.score).toBe("number");
      expect(row.value).toBe(-123);
      expect(row.active).toBe(false);
      expect(row.score).toBeCloseTo(3.14159);
    });

    it("allows duplicate rows (Databricks does not enforce unique)", async () => {
      const db = getDb();
      const dup = { id: "insert-dup", name: "Dup", value: 7, active: true, score: 7.7 };
      await db.insert(items).values(dup);
      await db.insert(items).values(dup);

      const rows = await db.select().from(items).where(eq(items.id, "insert-dup"));

      expect(rows).toHaveLength(2);
    });

    it("inserts an empty string value", async () => {
      const db = getDb();
      await db.insert(items).values({
        id: "insert-empty",
        name: "",
        value: 0,
        active: true,
        score: 0.0,
      });

      const rows = await db.select().from(items).where(eq(items.id, "insert-empty"));

      expect(rows).toHaveLength(1);
      expect(rows[0]!.name).toBe("");
    });

    it("inserts values containing special characters", async () => {
      const db = getDb();
      const tricky = `a'b"c\`d\\e — é 中`;
      await db.insert(items).values({
        id: "insert-special",
        name: tricky,
        value: 1,
        active: true,
        score: 1.0,
      });

      const rows = await db.select().from(items).where(eq(items.id, "insert-special"));

      expect(rows).toHaveLength(1);
      expect(rows[0]!.name).toBe(tricky);
    });
  });

  // ---------- UPDATE ----------

  describe("UPDATE", () => {
    it("updates a single column with WHERE and leaves others unchanged", async () => {
      const db = getDb();
      await db.insert(items).values({
        id: "update-single",
        name: "Original",
        value: 100,
        active: true,
        score: 5.5,
      });

      await db.update(items).set({ name: "Renamed" }).where(eq(items.id, "update-single"));

      const rows = await db.select().from(items).where(eq(items.id, "update-single"));

      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        id: "update-single",
        name: "Renamed",
        value: 100,
        active: true,
        score: 5.5,
      });
    });

    it("updates multiple columns at once", async () => {
      const db = getDb();
      await db.insert(items).values({
        id: "update-multi",
        name: "Before",
        value: 1,
        active: false,
        score: 0.0,
      });

      await db
        .update(items)
        .set({ name: "After", value: 999, active: true, score: 8.25 })
        .where(eq(items.id, "update-multi"));

      const rows = await db.select().from(items).where(eq(items.id, "update-multi"));

      expect(rows[0]).toMatchObject({
        id: "update-multi",
        name: "After",
        value: 999,
        active: true,
        score: 8.25,
      });
    });

    it("updates with complex WHERE using and()/or()", async () => {
      const db = getDb();
      await db.insert(items).values([
        { id: "update-complex-1", name: "A", value: 50, active: true, score: 1.0 },
        { id: "update-complex-2", name: "B", value: 60, active: true, score: 2.0 },
        { id: "update-complex-3", name: "C", value: 70, active: false, score: 3.0 },
      ]);

      await db
        .update(items)
        .set({ name: "Matched" })
        .where(
          and(
            or(eq(items.id, "update-complex-1"), eq(items.id, "update-complex-2")),
            eq(items.active, true),
          ),
        );

      const rows = await db.execute<{ id: string; name: string }>(
        sql`SELECT ${items.id}, ${items.name} FROM ${items} WHERE ${items.id} LIKE ${"update-complex-%"} ORDER BY ${items.id}`,
      );
      expect(rows).toEqual([
        { id: "update-complex-1", name: "Matched" },
        { id: "update-complex-2", name: "Matched" },
        { id: "update-complex-3", name: "C" },
      ]);
    });

    it("updates all rows when WHERE is omitted", async () => {
      const db = getDb();
      const ids = ["update-all-1", "update-all-2", "update-all-3"];
      await db
        .insert(items)
        .values(ids.map((id) => ({ id, name: "pre", value: 1, active: true, score: 0.0 })));

      // Use an isolated marker so we don't accidentally clobber other rows.
      // To genuinely update "all" rows for these three only, scope via WHERE.
      // Then test a no-WHERE update on a tiny set by deleting other rows first
      // would be destructive. Instead, validate the no-WHERE form against
      // a freshly-created scratch table.
      const scratchName = "qb_update_all";
      const scratch = bt(scratchName);
      const scratchTable = databricksTable(scratchName, {
        id: string("id"),
        name: string("name"),
        value: int("value"),
        active: boolean("active"),
        score: double("score"),
      });
      try {
        await db.execute(
          sql.raw(
            `CREATE TABLE IF NOT EXISTS ${scratch} (
            id STRING, name STRING, value INT, active BOOLEAN, score DOUBLE
          ) USING DELTA`,
          ),
        );
        await db.insert(scratchTable).values([
          { id: "a", name: "x", value: 1, active: true, score: 0.0 },
          { id: "b", name: "y", value: 2, active: false, score: 0.0 },
          { id: "c", name: "z", value: 3, active: true, score: 0.0 },
        ]);

        await db.update(scratchTable).set({ name: "all-updated" });

        const rows = await db.select().from(scratchTable);
        expect(rows).toHaveLength(3);
        expect(rows.every((r) => r.name === "all-updated")).toBe(true);
      } finally {
        await dropTable(db, scratchName);
      }
    });

    it("does not error when WHERE matches no rows", async () => {
      const db = getDb();
      await expect(
        db.update(items).set({ name: "never" }).where(eq(items.id, "update-no-match-xyz")),
      ).resolves.not.toThrow();

      const rows = await db.select().from(items).where(eq(items.id, "update-no-match-xyz"));
      expect(rows).toEqual([]);
    });

    it("updates a column to NULL", async () => {
      const db = getDb();
      await db.insert(items).values({
        id: "update-to-null",
        name: "has-value",
        value: 5,
        active: true,
        score: 1.0,
      });

      await db.update(items).set({ name: null }).where(eq(items.id, "update-to-null"));

      const rows = await db
        .select()
        .from(items)
        .where(and(eq(items.id, "update-to-null"), isNull(items.name)));
      expect(rows).toHaveLength(1);
      expect(rows[0]!.name).toBeNull();
    });
  });

  // ---------- DELETE ----------

  describe("DELETE", () => {
    it("deletes a specific row and leaves others intact", async () => {
      const db = getDb();
      await db.insert(items).values([
        { id: "delete-keep", name: "keep", value: 1, active: true, score: 0.0 },
        { id: "delete-target", name: "target", value: 2, active: true, score: 0.0 },
      ]);

      await db.delete(items).where(eq(items.id, "delete-target"));

      const target = await db.select().from(items).where(eq(items.id, "delete-target"));
      const keep = await db.select().from(items).where(eq(items.id, "delete-keep"));

      expect(target).toEqual([]);
      expect(keep).toHaveLength(1);
    });

    it("deletes with complex WHERE using and()/gt()", async () => {
      const db = getDb();
      await db.insert(items).values([
        { id: "delete-complex-1", name: "low", value: 1, active: true, score: 0.0 },
        { id: "delete-complex-2", name: "high", value: 100, active: true, score: 0.0 },
        { id: "delete-complex-3", name: "high-inactive", value: 100, active: false, score: 0.0 },
      ]);

      await db.delete(items).where(and(gt(items.value, 50), eq(items.active, true)));

      const rows = await db.execute<{ id: string }>(
        sql`SELECT ${items.id} FROM ${items} WHERE ${items.id} LIKE ${"delete-complex-%"} ORDER BY ${items.id}`,
      );
      const remaining = rows.map((r) => r.id);
      expect(remaining).toContain("delete-complex-1");
      expect(remaining).toContain("delete-complex-3");
      expect(remaining).not.toContain("delete-complex-2");
    });

    it("does not error when WHERE matches no rows", async () => {
      const db = getDb();
      await expect(
        db.delete(items).where(eq(items.id, "delete-no-match-xyz")),
      ).resolves.not.toThrow();
    });

    it("deletes all rows when WHERE is omitted", async () => {
      const db = getDb();
      const scratchName = "qb_delete_all";
      const scratch = bt(scratchName);
      const scratchTable = databricksTable(scratchName, {
        id: string("id"),
        name: string("name"),
        value: int("value"),
        active: boolean("active"),
        score: double("score"),
      });
      try {
        await db.execute(
          sql.raw(
            `CREATE TABLE IF NOT EXISTS ${scratch} (
            id STRING, name STRING, value INT, active BOOLEAN, score DOUBLE
          ) USING DELTA`,
          ),
        );
        await db.insert(scratchTable).values([
          { id: "a", name: "x", value: 1, active: true, score: 0.0 },
          { id: "b", name: "y", value: 2, active: false, score: 0.0 },
        ]);

        await db.delete(scratchTable);

        const rows = await db.select().from(scratchTable);
        expect(rows).toEqual([]);
      } finally {
        await dropTable(db, scratchName);
      }
    });
  });

  // ---------- Lifecycle / consistency ----------

  describe("lifecycle", () => {
    it("insert then immediately select returns the inserted row", async () => {
      const db = getDb();
      await db.insert(items).values({
        id: "lifecycle-read",
        name: "fresh",
        value: 11,
        active: true,
        score: 1.1,
      });

      const rows = await db.select().from(items).where(eq(items.id, "lifecycle-read"));

      expect(rows).toHaveLength(1);
      expect(rows[0]!.name).toBe("fresh");
    });

    it("update then select reflects the change", async () => {
      const db = getDb();
      await db.insert(items).values({
        id: "lifecycle-update",
        name: "before",
        value: 1,
        active: true,
        score: 0.0,
      });

      await db.update(items).set({ name: "after" }).where(eq(items.id, "lifecycle-update"));

      const rows = await db.select().from(items).where(eq(items.id, "lifecycle-update"));
      expect(rows[0]!.name).toBe("after");
    });

    it("runs full insert -> update -> delete cycle", async () => {
      const db = getDb();
      const id = "lifecycle-cycle";

      await db.insert(items).values({
        id,
        name: "created",
        value: 1,
        active: true,
        score: 0.0,
      });

      let rows = await db.select().from(items).where(eq(items.id, id));
      expect(rows).toHaveLength(1);
      expect(rows[0]!.name).toBe("created");

      await db.update(items).set({ name: "updated", value: 2 }).where(eq(items.id, id));

      rows = await db.select().from(items).where(eq(items.id, id));
      expect(rows[0]).toMatchObject({ name: "updated", value: 2 });

      await db.delete(items).where(eq(items.id, id));

      rows = await db.select().from(items).where(eq(items.id, id));
      expect(rows).toEqual([]);
    });
  });
});
