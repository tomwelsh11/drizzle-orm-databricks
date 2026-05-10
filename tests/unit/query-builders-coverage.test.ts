import { eq, gt, sql, asc, desc } from "drizzle-orm";
import { entityKind } from "drizzle-orm/entity";
import { describe, expect, it } from "vitest";

import {
  databricksTable,
  string,
  int,
  boolean,
  DatabricksDialect,
  DatabricksUpdateBuilder,
  DatabricksDeleteBase,
  DatabricksQueryBuilder,
} from "../../src";

const users = databricksTable("users", {
  id: string("id"),
  name: string("name"),
  age: int("age"),
  active: boolean("active"),
});

// ---------------------------------------------------------------------------
// DatabricksDeleteBase — orderBy, limit, $dynamic, constructor branches
// ---------------------------------------------------------------------------

describe("DatabricksDeleteBase coverage", () => {
  function makeDelete() {
    const dialect = new DatabricksDialect();
    return new DatabricksDeleteBase(users, null as any, dialect);
  }

  it("orderBy() with column arguments", () => {
    const del = makeDelete();
    del.orderBy(asc(users.age));
    const compiled = del.toSQL();
    expect(compiled.sql.toLowerCase()).toContain("order by");
    expect(compiled.sql.toLowerCase()).toContain("asc");
  });

  it("orderBy() with multiple columns", () => {
    const del = makeDelete();
    del.orderBy(desc(users.age), asc(users.name));
    const compiled = del.toSQL();
    expect(compiled.sql.toLowerCase()).toContain("order by");
    expect(compiled.sql.toLowerCase()).toContain("desc");
    expect(compiled.sql.toLowerCase()).toContain("asc");
  });

  it("orderBy() with callback function returning array", () => {
    const del = makeDelete();
    del.orderBy((fields: any) => [desc(fields.age), asc(fields.name)]);
    const compiled = del.toSQL();
    expect(compiled.sql.toLowerCase()).toContain("order by");
  });

  it("orderBy() with callback function returning single column", () => {
    const del = makeDelete();
    del.orderBy((fields: any) => desc(fields.age));
    const compiled = del.toSQL();
    expect(compiled.sql.toLowerCase()).toContain("order by");
  });

  it("limit() sets limit on delete", () => {
    const del = makeDelete();
    del.where(eq(users.active, false));
    del.limit(10);
    const compiled = del.toSQL();
    expect(compiled.sql.toLowerCase()).toContain("limit");
    expect(compiled.params).toContain(10);
  });

  it("chaining where + orderBy + limit", () => {
    const del = makeDelete();
    del.where(eq(users.active, false)).orderBy(asc(users.age)).limit(5);
    const compiled = del.toSQL();
    expect(compiled.sql.toLowerCase()).toContain("where");
    expect(compiled.sql.toLowerCase()).toContain("order by");
    expect(compiled.sql.toLowerCase()).toContain("limit");
  });

  it("$dynamic() returns this", () => {
    const del = makeDelete();
    const result = del.$dynamic();
    expect(result).toBe(del);
  });

  it("toSQL() returns sql and params without typings", () => {
    const del = makeDelete();
    del.where(eq(users.id, "u1"));
    const result = del.toSQL();
    expect(result).toHaveProperty("sql");
    expect(result).toHaveProperty("params");
    expect(result).not.toHaveProperty("typings");
  });

  it("getSQL() returns SQL object", () => {
    const del = makeDelete();
    del.where(eq(users.id, "u1"));
    const sqlObj = del.getSQL();
    expect(sqlObj).toBeDefined();
    expect(typeof sqlObj.getSQL).toBe("function");
  });

  it("where(undefined) does not add WHERE clause", () => {
    const del = makeDelete();
    del.where(undefined);
    const compiled = del.toSQL();
    expect(compiled.sql.toLowerCase()).not.toContain("where");
  });

  it("limit() with sql expression", () => {
    const del = makeDelete();
    del.limit(sql`10`);
    const compiled = del.toSQL();
    expect(compiled.sql.toLowerCase()).toContain("limit");
  });
});

// ---------------------------------------------------------------------------
// DatabricksUpdateBase — orderBy, limit, $dynamic, constructor branches
// ---------------------------------------------------------------------------

describe("DatabricksUpdateBase coverage", () => {
  function makeUpdate() {
    const dialect = new DatabricksDialect();
    return new DatabricksUpdateBuilder(users, null as any, dialect);
  }

  it("orderBy() with column arguments", () => {
    const compiled = makeUpdate().set({ name: "X" }).orderBy(asc(users.age)).toSQL();
    expect(compiled.sql.toLowerCase()).toContain("order by");
    expect(compiled.sql.toLowerCase()).toContain("asc");
  });

  it("orderBy() with multiple columns", () => {
    const compiled = makeUpdate()
      .set({ name: "X" })
      .orderBy(desc(users.age), asc(users.name))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain("order by");
  });

  it("orderBy() with callback function returning array", () => {
    const compiled = makeUpdate()
      .set({ name: "X" })
      .orderBy((fields: any) => [desc(fields.age), asc(fields.name)])
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain("order by");
  });

  it("orderBy() with callback function returning single column", () => {
    const compiled = makeUpdate()
      .set({ name: "X" })
      .orderBy((fields: any) => desc(fields.age))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain("order by");
  });

  it("limit() sets limit on update", () => {
    const compiled = makeUpdate()
      .set({ active: false })
      .where(eq(users.active, true))
      .limit(10)
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain("limit");
    expect(compiled.params).toContain(10);
  });

  it("chaining set + where + orderBy + limit", () => {
    const compiled = makeUpdate()
      .set({ active: false })
      .where(gt(users.age, 65))
      .orderBy(asc(users.age))
      .limit(100)
      .toSQL();
    const lower = compiled.sql.toLowerCase();
    expect(lower).toContain("update");
    expect(lower).toContain("set");
    expect(lower).toContain("where");
    expect(lower).toContain("order by");
    expect(lower).toContain("limit");
  });

  it("$dynamic() returns this", () => {
    const upd = makeUpdate().set({ name: "X" });
    const result = upd.$dynamic();
    expect(result).toBe(upd);
  });

  it("toSQL() returns sql and params without typings", () => {
    const result = makeUpdate().set({ name: "X" }).toSQL();
    expect(result).toHaveProperty("sql");
    expect(result).toHaveProperty("params");
    expect(result).not.toHaveProperty("typings");
  });

  it("getSQL() returns SQL object", () => {
    const upd = makeUpdate().set({ name: "X" });
    const sqlObj = upd.getSQL();
    expect(sqlObj).toBeDefined();
    expect(typeof sqlObj.getSQL).toBe("function");
  });

  it("where(undefined) does not add WHERE clause", () => {
    const compiled = makeUpdate().set({ name: "X" }).where(undefined).toSQL();
    expect(compiled.sql.toLowerCase()).not.toContain("where");
  });

  it("limit() with sql expression", () => {
    const compiled = makeUpdate()
      .set({ name: "X" })
      .limit(sql`10`)
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain("limit");
  });

  it("set with raw sql expression value", () => {
    const compiled = makeUpdate()
      .set({ name: sql`UPPER('test')` })
      .where(eq(users.id, "u1"))
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain("upper('test')");
    expect(compiled.params).toEqual(["u1"]);
  });
});

// ---------------------------------------------------------------------------
// DatabricksQueryBuilder — $with, with, select, selectDistinct, constructor
// ---------------------------------------------------------------------------

describe("DatabricksQueryBuilder coverage", () => {
  it("creates with default dialect", () => {
    const qb = new DatabricksQueryBuilder();
    expect(qb).toBeDefined();
    expect((DatabricksQueryBuilder as any)[entityKind]).toBe("DatabricksQueryBuilder");
  });

  it("creates with explicit dialect", () => {
    const dialect = new DatabricksDialect();
    const qb = new DatabricksQueryBuilder(dialect);
    expect(qb).toBeDefined();
  });

  it("select() without fields returns all columns", () => {
    const qb = new DatabricksQueryBuilder();
    const compiled = qb.select().from(users).toSQL();
    expect(compiled.sql).toContain("`id`");
    expect(compiled.sql).toContain("`name`");
    expect(compiled.sql).toContain("`age`");
    expect(compiled.sql).toContain("`active`");
  });

  it("select() with fields returns specified columns", () => {
    const qb = new DatabricksQueryBuilder();
    const compiled = qb.select({ id: users.id, name: users.name }).from(users).toSQL();
    expect(compiled.sql).toContain("`id`");
    expect(compiled.sql).toContain("`name`");
    expect(compiled.sql).not.toContain("`age`");
  });

  it("selectDistinct() without fields", () => {
    const qb = new DatabricksQueryBuilder();
    const compiled = qb.selectDistinct().from(users).toSQL();
    expect(compiled.sql.toLowerCase()).toContain("select distinct");
  });

  it("selectDistinct() with fields", () => {
    const qb = new DatabricksQueryBuilder();
    const compiled = qb.selectDistinct({ name: users.name }).from(users).toSQL();
    expect(compiled.sql.toLowerCase()).toContain("select distinct");
    expect(compiled.sql).toContain("`name`");
    expect(compiled.sql).not.toContain("`age`");
  });

  it("select() chained with where, orderBy, limit", () => {
    const qb = new DatabricksQueryBuilder();
    const compiled = qb
      .select()
      .from(users)
      .where(eq(users.active, true))
      .orderBy(desc(users.age))
      .limit(5)
      .toSQL();
    expect(compiled.sql.toLowerCase()).toContain("where");
    expect(compiled.sql.toLowerCase()).toContain("order by");
    expect(compiled.sql.toLowerCase()).toContain("limit");
  });

  it("with().select() returns a select builder", () => {
    const qb = new DatabricksQueryBuilder();
    const cte = qb
      .$with("active_users")
      .as(qb.select({ id: users.id, name: users.name }).from(users).where(eq(users.active, true)));
    const result = qb.with(cte).select().from(users).toSQL();
    expect(result.sql).toBeDefined();
  });

  it("with().selectDistinct() returns a distinct select builder", () => {
    const qb = new DatabricksQueryBuilder();
    const cte = qb
      .$with("active_users")
      .as(qb.select({ id: users.id }).from(users).where(eq(users.active, true)));
    const result = qb.with(cte).selectDistinct().from(users).toSQL();
    expect(result.sql.toLowerCase()).toContain("select distinct");
  });

  it("with().select(fields) returns a select builder with fields", () => {
    const qb = new DatabricksQueryBuilder();
    const cte = qb
      .$with("active_users")
      .as(qb.select({ id: users.id }).from(users).where(eq(users.active, true)));
    const result = qb.with(cte).select({ id: users.id }).from(users).toSQL();
    expect(result.sql).toContain("`id`");
  });

  it("with().selectDistinct(fields) returns a distinct select builder with fields", () => {
    const qb = new DatabricksQueryBuilder();
    const cte = qb
      .$with("active_users")
      .as(qb.select({ id: users.id }).from(users).where(eq(users.active, true)));
    const result = qb.with(cte).selectDistinct({ name: users.name }).from(users).toSQL();
    expect(result.sql.toLowerCase()).toContain("select distinct");
    expect(result.sql).toContain("`name`");
  });

  it("$with().as() with function callback", () => {
    const qb = new DatabricksQueryBuilder();
    const cte = qb
      .$with("active_users")
      .as((innerQb) => innerQb.select({ id: users.id }).from(users).where(eq(users.active, true)));
    const result = qb.with(cte).select().from(users).toSQL();
    expect(result.sql).toBeDefined();
  });

  it("$with().as() with selection parameter", () => {
    const qb = new DatabricksQueryBuilder();
    const cte = qb
      .$with("active_users", { id: users.id })
      .as(qb.select({ id: users.id }).from(users).where(eq(users.active, true)));
    const result = qb.with(cte).select().from(users).toSQL();
    expect(result.sql).toBeDefined();
  });

  it("$with().as() with raw SQL (no getSelectedFields)", () => {
    const qb = new DatabricksQueryBuilder();
    const rawSql = sql`SELECT 1 AS val`;
    const cte = qb.$with("literal_cte").as(rawSql);
    const result = qb.with(cte).select().from(users).toSQL();
    expect(result.sql).toBeDefined();
  });
});
