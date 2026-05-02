import { sql } from "drizzle-orm";
import { describe, expect, it, vi, beforeEach } from "vitest";

import { drizzle } from "../../src/driver";
import { migrate } from "../../src/migrator";
import { MockDBSQLClient } from "../mocks/databricks-sql";

function createDb(responses: Record<string, unknown>[][] = []) {
  const mockClient = new MockDBSQLClient();
  for (const r of responses) {
    mockClient.queueResponse(r);
  }
  const db = drizzle({ client: mockClient as never });
  return { db, mockClient };
}

describe("migrate()", () => {
  it("uses __drizzle_migrations as default table name", async () => {
    const { db, mockClient } = createDb([
      [], // CREATE TABLE
      [], // SELECT last migration (empty = none applied)
    ]);
    await migrate(db, { migrationsFolder: fixtureDir([]) });
    const createSql = mockClient.recorded[0]!.sql;
    expect(createSql).toContain("`__drizzle_migrations`");
  });

  it("uses custom migrationsTable name when provided", async () => {
    const { db, mockClient } = createDb([
      [], // CREATE TABLE
      [], // SELECT
    ]);
    await migrate(db, { migrationsFolder: fixtureDir([]), migrationsTable: "my_migrations" });
    const createSql = mockClient.recorded[0]!.sql;
    expect(createSql).toContain("`my_migrations`");
  });

  it("skips migrations that are already applied", async () => {
    const { db, mockClient } = createDb([
      [], // CREATE TABLE
      [{ hash: "abc", created_at: 1700000000100 }], // last applied = 1700000000100
    ]);
    const dir = fixtureDir([
      { tag: "0000_old", when: 1700000000100, sql: "CREATE TABLE old (id INT) USING DELTA;" },
      { tag: "0001_new", when: 1700000000200, sql: "CREATE TABLE new (id INT) USING DELTA;" },
    ]);
    await migrate(db, { migrationsFolder: dir });
    const sqls = mockClient.recorded.map((r) => r.sql);
    expect(sqls.some((s) => s.includes("CREATE TABLE old"))).toBe(false);
    expect(sqls.some((s) => s.includes("CREATE TABLE new"))).toBe(true);
  });

  it("applies all migrations when none have been applied", async () => {
    const { db, mockClient } = createDb([
      [], // CREATE TABLE
      [], // SELECT (no prior migrations)
      [], // migration 1 SQL
      [], // migration 1 INSERT
      [], // migration 2 SQL
      [], // migration 2 INSERT
    ]);
    const dir = fixtureDir([
      { tag: "0000_first", when: 1700000000100, sql: "CREATE TABLE first (id INT) USING DELTA;" },
      { tag: "0001_second", when: 1700000000200, sql: "CREATE TABLE second (id INT) USING DELTA;" },
    ]);
    await migrate(db, { migrationsFolder: dir });
    const sqls = mockClient.recorded.map((r) => r.sql);
    expect(sqls.some((s) => s.includes("CREATE TABLE first"))).toBe(true);
    expect(sqls.some((s) => s.includes("CREATE TABLE second"))).toBe(true);
  });

  it("records each applied migration in the tracking table", async () => {
    const { db, mockClient } = createDb([
      [], // CREATE TABLE
      [], // SELECT
      [], // migration SQL
      [], // INSERT record
    ]);
    const dir = fixtureDir([
      { tag: "0000_init", when: 1700000000100, sql: "CREATE TABLE x (id INT) USING DELTA;" },
    ]);
    await migrate(db, { migrationsFolder: dir });
    const insertCalls = mockClient.recorded.filter((r) => r.sql.includes("INSERT INTO"));
    expect(insertCalls).toHaveLength(1);
    expect(insertCalls[0]!.sql).toContain("__drizzle_migrations");
  });
});

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

type JournalEntry = {
  idx: number;
  version: string;
  when: number;
  tag: string;
  breakpoints: boolean;
};

function fixtureDir(entries: Array<{ tag: string; when: number; sql: string }>): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "drizzle-mig-unit-"));
  fs.mkdirSync(path.join(dir, "meta"));

  const journal: { version: string; dialect: string; entries: JournalEntry[] } = {
    version: "7",
    dialect: "postgresql",
    entries: entries.map((entry, idx) => ({
      idx,
      version: "7",
      when: entry.when,
      tag: entry.tag,
      breakpoints: true,
    })),
  };

  fs.writeFileSync(path.join(dir, "meta", "_journal.json"), JSON.stringify(journal, null, 2));

  for (const entry of entries) {
    fs.writeFileSync(path.join(dir, `${entry.tag}.sql`), entry.sql);
  }

  return dir;
}
