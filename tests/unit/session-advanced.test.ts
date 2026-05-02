import { sql } from "drizzle-orm";
import { NoopLogger } from "drizzle-orm/logger";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { SessionManager } from "../../src/connection";
import { DatabricksDialect } from "../../src/dialect";
import { DatabricksSession, DatabricksPreparedQuery } from "../../src/session";
import { MockDBSQLClient } from "../mocks/databricks-sql";

describe("Session logger integration", () => {
  let mockClient: MockDBSQLClient;
  let sessionManager: SessionManager;
  let dialect: DatabricksDialect;

  beforeEach(() => {
    mockClient = new MockDBSQLClient();
    sessionManager = new SessionManager({ client: mockClient as never });
    dialect = new DatabricksDialect();
  });

  it("execute() calls logger.logQuery with compiled SQL and params", async () => {
    const logQuery = vi.fn();
    const session = new DatabricksSession(sessionManager, dialect, {
      logger: { logQuery },
    });
    mockClient.queueResponse([{ n: 1 }]);
    await session.execute(sql`SELECT ${42} AS n`);
    expect(logQuery).toHaveBeenCalledTimes(1);
    expect(logQuery).toHaveBeenCalledWith("SELECT ? AS n", [42]);
  });

  it("prepareQuery + execute calls logger.logQuery", async () => {
    const logQuery = vi.fn();
    const prepared = new DatabricksPreparedQuery(
      sessionManager,
      "SELECT ?",
      [99],
      { logQuery },
      undefined,
    );
    mockClient.queueResponse([{ n: 99 }]);
    await prepared.execute();
    expect(logQuery).toHaveBeenCalledWith("SELECT ?", [99]);
  });

  it("NoopLogger is used by default (no error)", async () => {
    const session = new DatabricksSession(sessionManager, dialect);
    mockClient.queueResponse([]);
    await expect(session.execute(sql`SELECT 1`)).resolves.toBeDefined();
  });
});

describe("DatabricksPreparedQuery edge cases", () => {
  let mockClient: MockDBSQLClient;
  let sessionManager: SessionManager;

  beforeEach(() => {
    mockClient = new MockDBSQLClient();
    sessionManager = new SessionManager({ client: mockClient as never });
  });

  it("execute() with no fields and no customResultMapper returns raw result", async () => {
    mockClient.queueResponse([{ id: 1 }, { id: 2 }]);
    const prepared = new DatabricksPreparedQuery(
      sessionManager,
      "SELECT * FROM t",
      [],
      new NoopLogger(),
      undefined,
    );
    const result = await prepared.execute();
    expect(result).toEqual({ rows: [{ id: 1 }, { id: 2 }] });
  });

  it("execute() with empty params still works", async () => {
    mockClient.queueResponse([]);
    const prepared = new DatabricksPreparedQuery(
      sessionManager,
      "SELECT 1",
      [],
      new NoopLogger(),
      undefined,
    );
    const result = await prepared.execute();
    expect(result).toEqual({ rows: [] });
  });
});
