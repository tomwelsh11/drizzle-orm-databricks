import { sql } from "drizzle-orm";
import { afterAll, describe, expect, it } from "vitest";

import { drizzle } from "../../src/driver";
import { getConnectionConfig, hasCredentials } from "./helpers";

describe.skipIf(!hasCredentials())("connection pooling (e2e)", () => {
  const instances: { $close: () => Promise<void> }[] = [];

  function pooledDb(max: number) {
    const config = getConnectionConfig();
    const db = drizzle({ ...config, pool: { max } });
    instances.push(db);
    return db;
  }

  afterAll(async () => {
    for (const db of instances) {
      await db.$close();
    }
  });

  it("runs a simple query through a pooled connection", async () => {
    const db = pooledDb(2);
    const rows = await db.execute<{ n: number }>(sql`SELECT 1 AS n`);
    expect(rows).toEqual([{ n: 1 }]);
  });

  it("runs multiple sequential queries reusing pooled sessions", async () => {
    const db = pooledDb(2);
    for (let i = 1; i <= 5; i++) {
      const rows = await db.execute<{ n: number }>(sql`SELECT ${i} AS n`);
      expect(rows).toEqual([{ n: i }]);
    }
  });

  it("runs concurrent queries through the pool", async () => {
    const db = pooledDb(3);
    const promises = Array.from({ length: 6 }, (_, i) =>
      db.execute<{ n: number }>(sql`SELECT ${i + 1} AS n`),
    );
    const results = await Promise.all(promises);
    for (let i = 0; i < 6; i++) {
      expect(results[i]).toEqual([{ n: i + 1 }]);
    }
  });

  it("pool with max=1 serialises queries correctly", async () => {
    const db = pooledDb(1);
    const results: number[] = [];
    const promises = Array.from({ length: 3 }, (_, i) =>
      db.execute<{ n: number }>(sql`SELECT ${i + 1} AS n`).then((rows) => {
        results.push(rows[0]!.n);
      }),
    );
    await Promise.all(promises);
    expect(results.sort()).toEqual([1, 2, 3]);
  });

  it("closes cleanly after pooled queries", async () => {
    const config = getConnectionConfig();
    const db = drizzle({ ...config, pool: { max: 2 } });
    await db.execute(sql`SELECT 1 AS n`);
    await db.execute(sql`SELECT 2 AS n`);
    await db.$close();
    // no error = success
  });
});
