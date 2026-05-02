import { sql } from "drizzle-orm";
import { DrizzleQueryError } from "drizzle-orm/errors";
import { afterAll, describe, expect, it } from "vitest";

import { DatabricksUnsupportedError } from "../../src/errors";
import { closeDb, dropTable, getDb, hasCredentials } from "./helpers";

describe.skipIf(!hasCredentials())("Error handling (e2e)", () => {
  const createdTables: string[] = [];

  afterAll(async () => {
    const db = getDb();
    for (const name of createdTables) {
      try {
        await dropTable(db, name);
      } catch {
        // best-effort
      }
    }
    await closeDb();
  });

  it("throws on invalid SQL syntax", async () => {
    const db = getDb();
    await expect(db.execute(sql.raw("SELECTTTT 1"))).rejects.toThrow();
  });

  it("wraps driver errors in DrizzleQueryError with SQL and params", async () => {
    const db = getDb();
    const err = await db.execute(sql.raw("SELECTTTT 1")).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(DrizzleQueryError);
    expect((err as DrizzleQueryError).query).toBe("SELECTTTT 1");
    expect((err as DrizzleQueryError).params).toEqual([]);
    expect((err as DrizzleQueryError).cause).toBeDefined();
  });

  it("throws when querying a non-existent table", async () => {
    const db = getDb();
    const missing = "e2e_does_not_exist";
    await expect(db.execute(sql.raw(`SELECT * FROM \`${missing}\``))).rejects.toThrow();
  });

  it("errors or coerces when inserting wrong-typed parameter", async () => {
    const db = getDb();
    const tableName = "e2e_typecheck";
    createdTables.push(tableName);

    await db.execute(sql.raw(`CREATE TABLE IF NOT EXISTS \`${tableName}\` (id INT) USING DELTA`));

    let threw = false;
    let inserted: unknown;
    try {
      await db.execute(sql.raw(`INSERT INTO \`${tableName}\` (id) VALUES ('not-an-int')`));
      const rows = (await db.execute(
        sql.raw(`SELECT id FROM \`${tableName}\``),
      )) as unknown as Array<{ id: unknown }>;
      inserted = rows[0]?.id;
    } catch {
      threw = true;
    }

    if (!threw) {
      expect(inserted === null || typeof inserted === "number").toBe(true);
    } else {
      expect(threw).toBe(true);
    }
  });

  it("throws DatabricksUnsupportedError for transactions", async () => {
    const db = getDb();
    await expect(
      (
        db as unknown as {
          session: { transaction: (fn: () => Promise<unknown>) => Promise<unknown> };
        }
      ).session.transaction(async () => undefined),
    ).rejects.toBeInstanceOf(DatabricksUnsupportedError);
  });

  it("throws on division by zero in ANSI mode", async () => {
    const db = getDb();
    await expect(db.execute(sql.raw("SELECT 1/0 AS result"))).rejects.toThrow(
      /DIVIDE_BY_ZERO|Division by zero/,
    );
  });

  it("throws on duplicate column name in CREATE TABLE", async () => {
    const db = getDb();
    const tableName = "e2e_dupcol";
    createdTables.push(tableName);
    await expect(
      db.execute(sql.raw(`CREATE TABLE \`${tableName}\` (id INT, id INT) USING DELTA`)),
    ).rejects.toThrow();
  });
});
