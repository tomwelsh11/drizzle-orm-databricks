import { sql } from "drizzle-orm";
import { NoopLogger } from "drizzle-orm/logger";
import { beforeEach, describe, expect, it } from "vitest";

import { SessionManager } from "../../src/connection";
import { DatabricksDialect } from "../../src/dialect";
import { DatabricksUnsupportedError } from "../../src/errors";
import { DatabricksPreparedQuery, DatabricksSession } from "../../src/session";
import { MockDBSQLClient } from "../mocks/databricks-sql";

describe("DatabricksSession", () => {
  let mockClient: MockDBSQLClient;
  let sessionManager: SessionManager;
  let dialect: DatabricksDialect;
  let session: DatabricksSession;

  beforeEach(() => {
    mockClient = new MockDBSQLClient();
    sessionManager = new SessionManager({ client: mockClient as never });
    dialect = new DatabricksDialect();
    session = new DatabricksSession(sessionManager, dialect);
  });

  it("execute() compiles SQL and sends it to the driver", async () => {
    mockClient.queueResponse([{ id: 1 }]);
    await session.execute(sql`SELECT * FROM t WHERE id = ${42}`);
    expect(mockClient.recorded).toHaveLength(1);
    expect(mockClient.recorded[0]!.sql).toBe("SELECT * FROM t WHERE id = ?");
    expect(mockClient.recorded[0]!.params).toEqual([42]);
  });

  it("all() returns rows from the driver", async () => {
    const rows = [
      { id: 1, name: "a" },
      { id: 2, name: "b" },
    ];
    mockClient.queueResponse(rows);
    const result = await session.all(sql`SELECT id, name FROM t`);
    expect(result).toEqual(rows);
  });

  it("execute() returns empty array when no rows queued", async () => {
    const result = await session.execute(sql`SELECT 1`);
    expect(result).toEqual([]);
  });

  it("transaction() throws DatabricksUnsupportedError", async () => {
    await expect(session.transaction(async () => "ok")).rejects.toThrow(DatabricksUnsupportedError);
  });
});

describe("DatabricksPreparedQuery", () => {
  let mockClient: MockDBSQLClient;
  let sessionManager: SessionManager;

  beforeEach(() => {
    mockClient = new MockDBSQLClient();
    sessionManager = new SessionManager({ client: mockClient as never });
  });

  it("execute() runs the query with params", async () => {
    mockClient.queueResponse([{ id: 7 }]);
    const prepared = new DatabricksPreparedQuery(
      sessionManager,
      "SELECT * FROM t WHERE id = ?",
      [7],
      new NoopLogger(),
      undefined,
    );
    const result = await prepared.execute();
    expect(mockClient.recorded).toHaveLength(1);
    expect(mockClient.recorded[0]!.sql).toBe("SELECT * FROM t WHERE id = ?");
    expect(mockClient.recorded[0]!.params).toEqual([7]);
    expect(result).toEqual({ rows: [{ id: 7 }] });
  });

  it("execute() applies a customResultMapper when provided", async () => {
    mockClient.queueResponse([{ a: 1 }, { a: 2 }] as never);
    const prepared = new DatabricksPreparedQuery(
      sessionManager,
      "SELECT a FROM t",
      [],
      new NoopLogger(),
      undefined,
      (rows) => (rows as Array<{ a: number }>).map((r) => r.a),
    );
    const result = await prepared.execute();
    expect(result).toEqual([1, 2]);
  });

  it("iterator() throws DatabricksUnsupportedError", () => {
    const prepared = new DatabricksPreparedQuery(
      sessionManager,
      "SELECT 1",
      [],
      new NoopLogger(),
      undefined,
    );
    expect(() => prepared.iterator()).toThrow(DatabricksUnsupportedError);
  });
});
