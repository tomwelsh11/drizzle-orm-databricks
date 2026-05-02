import { sql } from "drizzle-orm";
import { afterAll, describe, expect, it } from "vitest";

import { drizzle } from "../../src/driver";
import { closeDb, getDb, getConnectionConfig, hasCredentials } from "./helpers";

describe.skipIf(!hasCredentials())("connection lifecycle (e2e)", () => {
  afterAll(async () => {
    await closeDb();
  });

  it("runs a simple SELECT 1 query", async () => {
    const db = getDb();
    const rows = await db.execute<{ one: number }>(sql`SELECT 1 AS one`);
    expect(rows).toEqual([{ one: 1 }]);
  });

  it("returns the configured catalog from current_catalog()", async () => {
    const db = getDb();
    const rows = await db.execute<Record<string, string>>(sql`SELECT current_catalog() AS catalog`);
    expect(rows).toHaveLength(1);
    expect(typeof rows[0]!["catalog"]).toBe("string");
  });

  it("reuses the session across multiple queries", async () => {
    const db = getDb();
    const r1 = await db.execute<{ n: number }>(sql`SELECT 1 AS n`);
    const r2 = await db.execute<{ n: number }>(sql`SELECT 2 AS n`);
    const r3 = await db.execute<{ n: number }>(sql`SELECT 3 AS n`);
    expect(r1).toEqual([{ n: 1 }]);
    expect(r2).toEqual([{ n: 2 }]);
    expect(r3).toEqual([{ n: 3 }]);
  });

  it("can close and reconstruct a db instance", async () => {
    const config = getConnectionConfig();

    const first = drizzle(config);
    const r1 = await first.execute<{ n: number }>(sql`SELECT 1 AS n`);
    expect(r1).toEqual([{ n: 1 }]);
    await first.$close();

    const second = drizzle(config);
    try {
      const r2 = await second.execute<{ n: number }>(sql`SELECT 1 AS n`);
      expect(r2).toEqual([{ n: 1 }]);
    } finally {
      await second.$close();
    }
  });
});
