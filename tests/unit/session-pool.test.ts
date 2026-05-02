import { beforeEach, describe, expect, it, vi } from "vitest";

import { MockDBSQLClient } from "../mocks/databricks-sql";

describe("SessionPool", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.doUnmock("@databricks/sql");
  });

  it("uses a provided client without owning it and pools sessions", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionPool } = await import("../../src/session-pool");
    const pool = new SessionPool({ client: mockClient as never }, { max: 2 });

    const s1 = await pool.acquire();
    const s2 = await pool.acquire();

    expect(mockClient.openSessionCalls).toHaveLength(2);
    expect(mockClient.sessions[0]).toBe(s1);
    expect(mockClient.sessions[1]).toBe(s2);
    expect(pool.inUse).toBe(2);

    await pool.release(s1);
    await pool.release(s2);
    expect(pool.available).toBe(2);

    // Reused — no new sessions opened.
    const s3 = await pool.acquire();
    expect(mockClient.openSessionCalls).toHaveLength(2);
    expect([s1, s2]).toContain(s3);

    await pool.drain();
    expect(mockClient.closed).toBe(false);
  });

  it("shares a single DBSQLClient across pooled sessions", async () => {
    const mockClient = new MockDBSQLClient();
    const ctorSpy = vi.fn(() => mockClient);
    vi.doMock("@databricks/sql", () => ({
      DBSQLClient: class {
        constructor() {
          return ctorSpy();
        }
      },
    }));

    const { SessionPool } = await import("../../src/session-pool");
    const pool = new SessionPool({ host: "h", path: "/p", token: "t" }, { max: 3 });

    await Promise.all([pool.acquire(), pool.acquire(), pool.acquire()]);

    expect(ctorSpy).toHaveBeenCalledTimes(1);
    expect(mockClient.openSessionCalls).toHaveLength(3);

    await pool.drain();
    expect(mockClient.closed).toBe(true);
  });

  it("connects with token credentials lazily on first acquire", async () => {
    const mockClient = new MockDBSQLClient();
    const connectSpy = vi.spyOn(mockClient, "connect");
    vi.doMock("@databricks/sql", () => ({
      DBSQLClient: class {
        constructor() {
          return mockClient;
        }
      },
    }));

    const { SessionPool } = await import("../../src/session-pool");
    const pool = new SessionPool({
      host: "h.databricks.com",
      path: "/sql/1.0/warehouses/x",
      token: "tok",
      catalog: "cat",
      schema: "sch",
    });

    expect(connectSpy).not.toHaveBeenCalled();

    await pool.acquire();

    expect(connectSpy).toHaveBeenCalledWith({
      host: "h.databricks.com",
      path: "/sql/1.0/warehouses/x",
      token: "tok",
    });
    expect(mockClient.openSessionCalls[0]).toEqual({
      initialCatalog: "cat",
      initialSchema: "sch",
    });

    await pool.drain();
  });

  it("connects with OAuth M2M credentials", async () => {
    const mockClient = new MockDBSQLClient();
    const connectSpy = vi.spyOn(mockClient, "connect");
    vi.doMock("@databricks/sql", () => ({
      DBSQLClient: class {
        constructor() {
          return mockClient;
        }
      },
    }));

    const { SessionPool } = await import("../../src/session-pool");
    const pool = new SessionPool({
      host: "h",
      path: "/p",
      clientId: "id",
      clientSecret: "sec",
    });

    await pool.acquire();

    expect(connectSpy).toHaveBeenCalledWith({
      host: "h",
      path: "/p",
      authType: "databricks-oauth",
      oauthClientId: "id",
      oauthClientSecret: "sec",
    });

    await pool.drain();
  });

  it("evicts sessions older than sessionMaxAgeMs on acquire", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionPool } = await import("../../src/session-pool");
    const pool = new SessionPool({ client: mockClient as never }, { max: 1, sessionMaxAgeMs: 100 });

    const s1 = await pool.acquire();
    await pool.release(s1);

    // Age the session past the limit.
    const realNow = Date.now;
    Date.now = () => realNow() + 200;
    try {
      const s2 = await pool.acquire();
      expect(s2).not.toBe(s1);
      expect((mockClient.sessions[0] as unknown as { closed: boolean }).closed).toBe(true);
    } finally {
      Date.now = realNow;
    }

    await pool.drain();
  });

  it("retries stale-session errors with a fresh session via runWithRetry", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionPool } = await import("../../src/session-pool");
    const pool = new SessionPool({ client: mockClient as never });

    let calls = 0;
    const result = await pool.runWithRetry(async () => {
      calls += 1;
      if (calls === 1) throw new Error("session is closed");
      return "ok";
    });

    expect(result).toBe("ok");
    expect(calls).toBe(2);
    expect(mockClient.openSessionCalls).toHaveLength(2);

    await pool.drain();
  });

  it("does not retry non-stale errors", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionPool } = await import("../../src/session-pool");
    const pool = new SessionPool({ client: mockClient as never });

    let calls = 0;
    await expect(
      pool.runWithRetry(async () => {
        calls += 1;
        throw new Error("syntax error");
      }),
    ).rejects.toThrow("syntax error");

    expect(calls).toBe(1);
    expect(mockClient.openSessionCalls).toHaveLength(1);
    // Session should still be released — available again for reuse.
    expect(pool.available).toBe(1);

    await pool.drain();
  });

  it("releases sessions after runWithRetry success", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionPool } = await import("../../src/session-pool");
    const pool = new SessionPool({ client: mockClient as never });

    await pool.runWithRetry(async () => "ok");

    expect(pool.inUse).toBe(0);
    expect(pool.available).toBe(1);

    await pool.drain();
  });

  it("drain closes all sessions and the owned client", async () => {
    const mockClient = new MockDBSQLClient();
    vi.doMock("@databricks/sql", () => ({
      DBSQLClient: class {
        constructor() {
          return mockClient;
        }
      },
    }));

    const { SessionPool } = await import("../../src/session-pool");
    const pool = new SessionPool({ host: "h", path: "/p", token: "t" }, { max: 2 });

    const s1 = await pool.acquire();
    await pool.acquire();
    await pool.release(s1);

    await pool.drain();

    expect(mockClient.sessions[0]!.closed).toBe(true);
    expect(mockClient.sessions[1]!.closed).toBe(true);
    expect(mockClient.closed).toBe(true);
  });

  it("drain closes sessions but not a non-owned client", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionPool } = await import("../../src/session-pool");
    const pool = new SessionPool({ client: mockClient as never });

    await pool.acquire();
    await pool.drain();

    expect(mockClient.sessions[0]!.closed).toBe(true);
    expect(mockClient.closed).toBe(false);
  });

  it("propagates connection errors as DatabricksConnectionError", async () => {
    const mockClient = new MockDBSQLClient();
    vi.spyOn(mockClient, "connect").mockRejectedValueOnce(new Error("boom"));
    vi.doMock("@databricks/sql", () => ({
      DBSQLClient: class {
        constructor() {
          return mockClient;
        }
      },
    }));

    const { SessionPool } = await import("../../src/session-pool");
    const { DatabricksConnectionError } = await import("../../src/errors");
    const pool = new SessionPool({ host: "h", path: "/p", token: "t" });

    await expect(pool.acquire()).rejects.toBeInstanceOf(DatabricksConnectionError);
  });
});
