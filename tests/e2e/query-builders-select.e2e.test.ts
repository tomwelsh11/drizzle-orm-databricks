import {
  and,
  asc,
  between,
  desc,
  eq,
  gt,
  gte,
  inArray,
  isNotNull,
  isNull,
  like,
  lt,
  lte,
  ne,
  or,
  sql,
} from "drizzle-orm";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { boolean, databricksTable, double, int, string } from "../../src";
import { closeDb, dropTable, getDb, hasCredentials } from "./helpers";

const usersName = "qb_select_users";

const users = databricksTable(usersName, {
  id: string("id"),
  name: string("name"),
  email: string("email"),
  age: int("age"),
  active: boolean("active"),
  score: double("score"),
});

const bt = (n: string) => "`" + n + "`";

describe.skipIf(!hasCredentials())("Query builder SELECT (e2e)", () => {
  beforeAll(async () => {
    const db = getDb();
    await dropTable(db, usersName);
    await db.execute(
      sql.raw(
        `CREATE TABLE IF NOT EXISTS ${bt(usersName)} (
          id STRING,
          name STRING,
          email STRING,
          age INT,
          active BOOLEAN,
          score DOUBLE
        ) USING DELTA`,
      ),
    );
    await db.execute(
      sql.raw(
        `INSERT INTO ${bt(usersName)} (id, name, email, age, active, score) VALUES
          ('u01', 'Alice',   'alice@example.com',   30, true,  92.5),
          ('u02', 'Bob',     'bob@example.com',     25, true,  78.0),
          ('u03', 'Carol',   'carol@example.com',   45, false, 64.5),
          ('u04', 'Dave',    'dave@example.com',    33, true,  88.0),
          ('u05', 'Eve',     'eve@example.com',     22, false, 55.5),
          ('u06', 'Frank',   'frank@example.com',   60, true,  70.0),
          ('u07', 'Grace',   'grace@example.com',   28, true,  78.0),
          ('u08', 'Heidi',   'heidi@example.com',   19, false, 40.0),
          ('u09', 'Ivan',    'ivan@example.com',    50, true,  95.5),
          ('u10', 'Judy',    NULL,                  37, false, 65.0)`,
      ),
    );
  });

  afterAll(async () => {
    try {
      await dropTable(getDb(), usersName);
    } finally {
      await closeDb();
    }
  });

  it("SELECT * returns all rows with correct types", async () => {
    const db = getDb();
    const rows = await db.select().from(users);
    expect(rows).toHaveLength(10);
    const alice = rows.find((r) => r.id === "u01");
    expect(alice).toBeDefined();
    expect(alice!.name).toBe("Alice");
    expect(alice!.email).toBe("alice@example.com");
    expect(typeof alice!.age).toBe("number");
    expect(alice!.age).toBe(30);
    expect(typeof alice!.active).toBe("boolean");
    expect(alice!.active).toBe(true);
    expect(typeof alice!.score).toBe("number");
    expect(alice!.score).toBeCloseTo(92.5);
  });

  it("partial SELECT returns only requested columns", async () => {
    const db = getDb();
    const rows = await db
      .select({ id: users.id, name: users.name })
      .from(users)
      .where(eq(users.id, "u01"));
    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual({ id: "u01", name: "Alice" });
    expect(Object.keys(rows[0]!).sort()).toEqual(["id", "name"]);
  });

  it("WHERE eq filters by exact match", async () => {
    const db = getDb();
    const rows = await db.select().from(users).where(eq(users.name, "Bob"));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.id).toBe("u02");
  });

  it("WHERE and() combines multiple conditions", async () => {
    const db = getDb();
    const rows = await db
      .select()
      .from(users)
      .where(and(gt(users.age, 25), eq(users.active, true)));
    expect(rows.length).toBeGreaterThanOrEqual(1);
    expect(rows.every((r) => r.age > 25 && r.active === true)).toBe(true);
  });

  it("WHERE or() matches any of multiple conditions", async () => {
    const db = getDb();
    const rows = await db
      .select()
      .from(users)
      .where(or(eq(users.id, "u01"), eq(users.id, "u02")));
    expect(rows).toHaveLength(2);
    const ids = rows.map((r) => r.id).sort();
    expect(ids).toEqual(["u01", "u02"]);
  });

  it("WHERE gt / lt / gte / lte filter by comparison", async () => {
    const db = getDb();
    const gtRows = await db
      .select({ id: users.id, age: users.age })
      .from(users)
      .where(gt(users.age, 45));
    expect(gtRows.every((r) => r.age > 45)).toBe(true);
    expect(gtRows.length).toBeGreaterThanOrEqual(2);

    const ltRows = await db
      .select({ id: users.id, age: users.age })
      .from(users)
      .where(lt(users.age, 25));
    expect(ltRows.every((r) => r.age < 25)).toBe(true);
    expect(ltRows.length).toBeGreaterThanOrEqual(2);

    const gteRows = await db
      .select({ id: users.id, age: users.age })
      .from(users)
      .where(gte(users.age, 45));
    expect(gteRows.every((r) => r.age >= 45)).toBe(true);
    expect(gteRows.length).toBeGreaterThanOrEqual(3);

    const lteRows = await db
      .select({ id: users.id, age: users.age })
      .from(users)
      .where(lte(users.age, 25));
    expect(lteRows.every((r) => r.age <= 25)).toBe(true);
    expect(lteRows.length).toBeGreaterThanOrEqual(3);
  });

  it("WHERE ne filters out exact match", async () => {
    const db = getDb();
    const rows = await db.select({ id: users.id }).from(users).where(ne(users.id, "u01"));
    expect(rows).toHaveLength(9);
    expect(rows.every((r) => r.id !== "u01")).toBe(true);
  });

  it("WHERE like matches a pattern", async () => {
    const db = getDb();
    const rows = await db.select({ name: users.name }).from(users).where(like(users.name, "A%"));
    expect(rows.length).toBeGreaterThanOrEqual(1);
    expect(rows.every((r) => r.name!.startsWith("A"))).toBe(true);
  });

  it("WHERE isNull / isNotNull filter null values", async () => {
    const db = getDb();
    const nullRows = await db.select({ id: users.id }).from(users).where(isNull(users.email));
    expect(nullRows).toHaveLength(1);
    expect(nullRows[0]!.id).toBe("u10");

    const notNullRows = await db.select({ id: users.id }).from(users).where(isNotNull(users.email));
    expect(notNullRows).toHaveLength(9);
    expect(notNullRows.every((r) => r.id !== "u10")).toBe(true);
  });

  it("WHERE inArray builds an IN clause", async () => {
    const db = getDb();
    const rows = await db
      .select({ id: users.id })
      .from(users)
      .where(inArray(users.id, ["u01", "u03", "u05"]));
    expect(rows).toHaveLength(3);
    const ids = rows.map((r) => r.id).sort();
    expect(ids).toEqual(["u01", "u03", "u05"]);
  });

  it("WHERE between filters an inclusive range", async () => {
    const db = getDb();
    const rows = await db
      .select({ id: users.id, age: users.age })
      .from(users)
      .where(between(users.age, 25, 35));
    expect(rows.length).toBeGreaterThanOrEqual(3);
    expect(rows.every((r) => r.age >= 25 && r.age <= 35)).toBe(true);
  });

  it("ORDER BY ascending sorts rows in ascending order", async () => {
    const db = getDb();
    const rows = await db
      .select({ id: users.id, age: users.age })
      .from(users)
      .orderBy(asc(users.age));
    expect(rows).toHaveLength(10);
    for (let i = 1; i < rows.length; i++) {
      expect(rows[i]!.age).toBeGreaterThanOrEqual(rows[i - 1]!.age);
    }
  });

  it("ORDER BY descending sorts rows in descending order", async () => {
    const db = getDb();
    const rows = await db
      .select({ id: users.id, age: users.age })
      .from(users)
      .orderBy(desc(users.age));
    expect(rows).toHaveLength(10);
    for (let i = 1; i < rows.length; i++) {
      expect(rows[i]!.age).toBeLessThanOrEqual(rows[i - 1]!.age);
    }
  });

  it("ORDER BY multiple columns sorts compound", async () => {
    const db = getDb();
    const rows = await db
      .select({ id: users.id, active: users.active, score: users.score })
      .from(users)
      .orderBy(asc(users.active), desc(users.score));
    expect(rows).toHaveLength(10);
    for (let i = 1; i < rows.length; i++) {
      const prev = rows[i - 1]!;
      const curr = rows[i]!;
      if (prev.active === curr.active) {
        expect(curr.score).toBeLessThanOrEqual(prev.score);
      } else {
        expect(Number(prev.active)).toBeLessThanOrEqual(Number(curr.active));
      }
    }
  });

  it("LIMIT restricts result size", async () => {
    const db = getDb();
    const rows = await db.select().from(users).orderBy(asc(users.id)).limit(3);
    expect(rows).toHaveLength(3);
    expect(rows.map((r) => r.id)).toEqual(["u01", "u02", "u03"]);
  });

  it("OFFSET with LIMIT paginates results", async () => {
    const db = getDb();
    const rows = await db
      .select({ id: users.id })
      .from(users)
      .orderBy(asc(users.id))
      .limit(3)
      .offset(3);
    expect(rows).toHaveLength(3);
    expect(rows.map((r) => r.id)).toEqual(["u04", "u05", "u06"]);
  });

  it("SELECT DISTINCT deduplicates results", async () => {
    const db = getDb();
    const rows = await db.selectDistinct({ active: users.active }).from(users);
    expect(rows.length).toBe(2);
    const values = rows.map((r) => r.active).sort();
    expect(values).toEqual([false, true]);
  });

  it("GROUP BY with count aggregates by group", async () => {
    const db = getDb();
    const rows = await db
      .select({
        active: users.active,
        cnt: sql<number>`count(*)`.as("cnt"),
      })
      .from(users)
      .groupBy(users.active)
      .orderBy(asc(users.active));
    expect(rows).toHaveLength(2);
    const totals = rows.map((r) => Number(r.cnt));
    expect(totals.reduce((a, b) => a + b, 0)).toBe(10);
    const trueRow = rows.find((r) => r.active === true);
    const falseRow = rows.find((r) => r.active === false);
    expect(Number(trueRow!.cnt)).toBe(6);
    expect(Number(falseRow!.cnt)).toBe(4);
  });

  it("returns empty array when WHERE matches nothing", async () => {
    const db = getDb();
    const rows = await db.select().from(users).where(eq(users.id, "does-not-exist"));
    expect(rows).toEqual([]);
  });

  it("chains where + orderBy + limit + offset together", async () => {
    const db = getDb();
    const rows = await db
      .select({ id: users.id, age: users.age, active: users.active })
      .from(users)
      .where(eq(users.active, true))
      .orderBy(desc(users.age))
      .limit(2)
      .offset(1);
    expect(rows).toHaveLength(2);
    expect(rows.every((r) => r.active === true)).toBe(true);
    for (let i = 1; i < rows.length; i++) {
      expect(rows[i]!.age).toBeLessThanOrEqual(rows[i - 1]!.age);
    }
  });
});
