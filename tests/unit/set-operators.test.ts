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

const archived = databricksTable("archived", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
});

function createDb(rows: Record<string, unknown>[] = []) {
  const mockClient = new MockDBSQLClient();
  mockClient.queueResponse(rows);
  const db = drizzle({ client: mockClient as never });
  return { db, mockClient };
}

describe("Set operators (union / intersect / except)", () => {
  it(".union() generates UNION SQL", async () => {
    const { db, mockClient } = createDb();
    await db.select().from(users).union(db.select().from(archived));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("union");
    expect(s).not.toContain("union all");
    expect(s).toContain("`users`");
    expect(s).toContain("`archived`");
  });

  it(".unionAll() generates UNION ALL SQL", async () => {
    const { db, mockClient } = createDb();
    await db.select().from(users).unionAll(db.select().from(archived));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("union all");
  });

  it(".intersect() generates INTERSECT SQL", async () => {
    const { db, mockClient } = createDb();
    await db.select().from(users).intersect(db.select().from(archived));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("intersect");
    expect(s).not.toContain("intersect all");
  });

  it(".intersectAll() generates INTERSECT ALL SQL", async () => {
    const { db, mockClient } = createDb();
    await db.select().from(users).intersectAll(db.select().from(archived));
    expect(mockClient.recorded[0]!.sql).toContain("intersect all");
  });

  it(".except() generates EXCEPT SQL", async () => {
    const { db, mockClient } = createDb();
    await db.select().from(users).except(db.select().from(archived));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("except");
    expect(s).not.toContain("except all");
  });

  it(".exceptAll() generates EXCEPT ALL SQL", async () => {
    const { db, mockClient } = createDb();
    await db.select().from(users).exceptAll(db.select().from(archived));
    expect(mockClient.recorded[0]!.sql).toContain("except all");
  });

  it("standalone union() function generates correct SQL", async () => {
    const { union } = await import("../../src/query-builders/select");
    const { db, mockClient } = createDb();
    await union(db.select().from(users), db.select().from(archived));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("union");
    expect(s).toContain("`users`");
    expect(s).toContain("`archived`");
  });

  it("chaining multiple set operators composes them", async () => {
    const third = databricksTable("third", {
      id: string("id"),
      name: string("name"),
      age: int("age"),
    });
    const { db, mockClient } = createDb();
    await db.select().from(users).union(db.select().from(archived)).except(db.select().from(third));
    const s = mockClient.recorded[0]!.sql;
    expect(s).toContain("union");
    expect(s).toContain("except");
    expect(s).toContain("`third`");
  });

  it("set operator with mismatched fields throws", () => {
    const other = databricksTable("other", {
      x: string("x"),
    });
    const { db } = createDb();
    expect(() =>
      db
        .select()
        .from(users)
        .union(db.select().from(other) as any),
    ).toThrow(/Set operator error/);
  });
});
