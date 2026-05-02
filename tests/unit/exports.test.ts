import { describe, expect, it } from "vitest";

import * as index from "../../src/index";

describe("Public API exports (src/index.ts)", () => {
  it("exports drizzle factory and DatabricksDatabase", () => {
    expect(typeof index.drizzle).toBe("function");
    expect(index.DatabricksDatabase).toBeDefined();
  });

  it("exports migrate function", () => {
    expect(typeof index.migrate).toBe("function");
  });

  it("exports error classes", () => {
    expect(index.DatabricksUnsupportedError).toBeDefined();
    expect(index.DatabricksConnectionError).toBeDefined();
  });

  it("exports DatabricksDialect", () => {
    expect(index.DatabricksDialect).toBeDefined();
  });

  it("exports DatabricksSession and DatabricksPreparedQuery", () => {
    expect(index.DatabricksSession).toBeDefined();
    expect(index.DatabricksPreparedQuery).toBeDefined();
  });

  it("exports table builders", () => {
    expect(typeof index.databricksTable).toBe("function");
    expect(typeof index.databricksSchema).toBe("function");
    expect(index.DatabricksTable).toBeDefined();
  });

  it("exports query builder classes", () => {
    expect(index.DatabricksSelectBuilder).toBeDefined();
    expect(index.DatabricksSelectBase).toBeDefined();
    expect(index.DatabricksInsertBuilder).toBeDefined();
    expect(index.DatabricksInsertBase).toBeDefined();
    expect(index.DatabricksUpdateBuilder).toBeDefined();
    expect(index.DatabricksUpdateBase).toBeDefined();
    expect(index.DatabricksDeleteBase).toBeDefined();
  });

  it("exports set operator functions", () => {
    expect(typeof index.union).toBe("function");
    expect(typeof index.unionAll).toBe("function");
    expect(typeof index.intersect).toBe("function");
    expect(typeof index.intersectAll).toBe("function");
    expect(typeof index.except).toBe("function");
    expect(typeof index.exceptAll).toBe("function");
  });

  it("exports all column type builders", () => {
    expect(typeof index.string).toBe("function");
    expect(typeof index.int).toBe("function");
    expect(typeof index.bigint).toBe("function");
    expect(typeof index.boolean).toBe("function");
    expect(typeof index.double).toBe("function");
    expect(typeof index.float).toBe("function");
    expect(typeof index.decimal).toBe("function");
    expect(typeof index.varchar).toBe("function");
    expect(typeof index.char).toBe("function");
    expect(typeof index.timestamp).toBe("function");
    expect(typeof index.timestampNtz).toBe("function");
    expect(typeof index.date).toBe("function");
    expect(typeof index.binary).toBe("function");
    expect(typeof index.tinyint).toBe("function");
    expect(typeof index.smallint).toBe("function");
    expect(typeof index.variant).toBe("function");
  });

  it("exports type definitions (spot check)", () => {
    expect(index).toHaveProperty("DatabricksTable");
    expect(index).toHaveProperty("DatabricksDialect");
  });
});
