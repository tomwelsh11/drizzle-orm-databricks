import { eq, gt, sql } from "drizzle-orm";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { boolean, databricksTable, double, int, string } from "../../src";
import { closeDb, dropTable, getDb, hasCredentials } from "./helpers";

const bt = (n: string) => "`" + n + "`";

const sourceName = "qb_insert_select_src";
const archiveName = "qb_insert_select_archive";

const sourceTable = databricksTable(sourceName, {
  id: string("id"),
  name: string("name"),
  age: int("age"),
  active: boolean("active"),
  score: double("score"),
});

const archiveTable = databricksTable(archiveName, {
  id: string("id"),
  name: string("name"),
  age: int("age"),
  active: boolean("active"),
  score: double("score"),
});

describe.skipIf(!hasCredentials())("INSERT...SELECT and WITH (CTE) on DML (e2e)", () => {
  beforeAll(async () => {
    const db = getDb();
    await dropTable(db, sourceName);
    await dropTable(db, archiveName);
    const cols = `(
      id STRING,
      name STRING,
      age INT,
      active BOOLEAN,
      score DOUBLE
    ) USING DELTA`;
    await db.execute(sql.raw(`CREATE TABLE IF NOT EXISTS ${bt(sourceName)} ${cols}`));
    await db.execute(sql.raw(`CREATE TABLE IF NOT EXISTS ${bt(archiveName)} ${cols}`));
    await db.insert(sourceTable).values([
      { id: "u1", name: "Alice", age: 30, active: true, score: 9.5 },
      { id: "u2", name: "Bob", age: 17, active: true, score: 7.0 },
      { id: "u3", name: "Carol", age: 65, active: false, score: 8.2 },
    ]);
  });

  afterAll(async () => {
    try {
      const db = getDb();
      await dropTable(db, sourceName);
      await dropTable(db, archiveName);
    } finally {
      await closeDb();
    }
  });

  it("INSERT...SELECT copies all rows from source to archive", async () => {
    const db = getDb();
    await db.execute(sql.raw(`DELETE FROM ${bt(archiveName)}`));

    await db.insert(archiveTable).select(db.select().from(sourceTable));

    const rows = await db.select().from(archiveTable);
    expect(rows).toHaveLength(3);
    expect(rows.map((r) => r.id).sort()).toEqual(["u1", "u2", "u3"]);
  });

  it("INSERT...SELECT with WHERE copies a subset", async () => {
    const db = getDb();
    await db.execute(sql.raw(`DELETE FROM ${bt(archiveName)}`));

    await db
      .insert(archiveTable)
      .select(db.select().from(sourceTable).where(gt(sourceTable.age, 17)));

    const rows = await db.select().from(archiveTable);
    expect(rows.map((r) => r.id).sort()).toEqual(["u1", "u3"]);
  });

  it("WITH (CTE) on INSERT...SELECT", async () => {
    const db = getDb();
    await db.execute(sql.raw(`DELETE FROM ${bt(archiveName)}`));

    const adults = db
      .$with("adults")
      .as(db.select().from(sourceTable).where(gt(sourceTable.age, 17)));

    await db.with(adults).insert(archiveTable).select(db.select().from(adults));

    const rows = await db.select().from(archiveTable);
    expect(rows.map((r) => r.id).sort()).toEqual(["u1", "u3"]);
  });

  it("WITH (CTE) on UPDATE", async () => {
    const db = getDb();
    await db.execute(sql.raw(`DELETE FROM ${bt(archiveName)}`));
    await db.insert(archiveTable).select(db.select().from(sourceTable));

    const seniors = db
      .$with("seniors")
      .as(db.select().from(archiveTable).where(gt(archiveTable.age, 60)));

    await db
      .with(seniors)
      .update(archiveTable)
      .set({ active: false })
      .where(eq(archiveTable.id, "u3"));

    const rows = await db.select().from(archiveTable).where(eq(archiveTable.id, "u3"));
    expect(rows[0]?.active).toBe(false);
  });

  it("WITH (CTE) on DELETE", async () => {
    const db = getDb();
    await db.execute(sql.raw(`DELETE FROM ${bt(archiveName)}`));
    await db.insert(archiveTable).select(db.select().from(sourceTable));

    const minors = db
      .$with("minors")
      .as(db.select().from(archiveTable).where(eq(archiveTable.id, "u2")));

    await db.with(minors).delete(archiveTable).where(eq(archiveTable.id, "u2"));

    const rows = await db.select().from(archiveTable);
    expect(rows.map((r) => r.id).sort()).toEqual(["u1", "u3"]);
  });
});
