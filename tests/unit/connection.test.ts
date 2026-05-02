import { beforeEach, describe, expect, it, vi } from "vitest";

import { MockDBSQLClient } from "../mocks/databricks-sql";

describe("SessionManager", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.doUnmock("@databricks/sql");
  });

  it("uses a provided client without owning it", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionManager } = await import("../../src/connection");
    const manager = new SessionManager({ client: mockClient as never });

    const session = await manager.getSession();
    expect(session).toBe(mockClient.sessions[0]);
    expect(mockClient.openSessionCalls).toHaveLength(1);

    await manager.close();
    expect(mockClient.sessions[0]!.closed).toBe(true);
    expect(mockClient.closed).toBe(false);
  });

  it("constructs a client lazily from connection config", async () => {
    const mockClient = new MockDBSQLClient();
    const connectSpy = vi.spyOn(mockClient, "connect");

    vi.doMock("@databricks/sql", () => ({
      DBSQLClient: class {
        constructor() {
          return mockClient;
        }
      },
    }));

    const { SessionManager } = await import("../../src/connection");
    const manager = new SessionManager({
      host: "h.databricks.com",
      path: "/sql/1.0/warehouses/x",
      token: "tok",
      catalog: "cat",
      schema: "sch",
    });

    expect(connectSpy).not.toHaveBeenCalled();

    await manager.getSession();

    expect(connectSpy).toHaveBeenCalledWith({
      host: "h.databricks.com",
      path: "/sql/1.0/warehouses/x",
      token: "tok",
    });
    expect(mockClient.openSessionCalls[0]).toEqual({
      initialCatalog: "cat",
      initialSchema: "sch",
    });
  });

  it("opens a session lazily on first getSession()", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionManager } = await import("../../src/connection");
    const manager = new SessionManager({ client: mockClient as never });

    expect(mockClient.openSessionCalls).toHaveLength(0);

    const a = await manager.getSession();
    const b = await manager.getSession();

    expect(mockClient.openSessionCalls).toHaveLength(1);
    expect(a).toBe(b);
  });

  it("runWithRetry() retries on stale session errors", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionManager } = await import("../../src/connection");
    const manager = new SessionManager({ client: mockClient as never });

    let calls = 0;
    const result = await manager.runWithRetry(async () => {
      calls += 1;
      if (calls === 1) {
        throw new Error("session is closed");
      }
      return "ok";
    });

    expect(result).toBe("ok");
    expect(calls).toBe(2);
    // A fresh session was opened after the stale failure.
    expect(mockClient.openSessionCalls).toHaveLength(2);
    expect(mockClient.sessions).toHaveLength(2);
  });

  it("runWithRetry() does not retry non-stale errors", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionManager } = await import("../../src/connection");
    const manager = new SessionManager({ client: mockClient as never });

    let calls = 0;
    await expect(
      manager.runWithRetry(async () => {
        calls += 1;
        throw new Error("syntax error near FROM");
      }),
    ).rejects.toThrow("syntax error near FROM");

    expect(calls).toBe(1);
    expect(mockClient.openSessionCalls).toHaveLength(1);
  });

  it("close() closes session and owned client", async () => {
    const mockClient = new MockDBSQLClient();

    vi.doMock("@databricks/sql", () => ({
      DBSQLClient: class {
        constructor() {
          return mockClient;
        }
      },
    }));

    const { SessionManager } = await import("../../src/connection");
    const manager = new SessionManager({
      host: "h",
      path: "/p",
      token: "t",
    });

    await manager.getSession();
    await manager.close();

    expect(mockClient.sessions[0]!.closed).toBe(true);
    expect(mockClient.closed).toBe(true);
  });

  it("close() does not close a non-owned client", async () => {
    const mockClient = new MockDBSQLClient();
    const { SessionManager } = await import("../../src/connection");
    const manager = new SessionManager({ client: mockClient as never });

    await manager.getSession();
    await manager.close();

    expect(mockClient.sessions[0]!.closed).toBe(true);
    expect(mockClient.closed).toBe(false);
  });

  it("connects with OAuth M2M credentials (service principal)", async () => {
    const mockClient = new MockDBSQLClient();
    const connectSpy = vi.spyOn(mockClient, "connect");

    vi.doMock("@databricks/sql", () => ({
      DBSQLClient: class {
        constructor() {
          return mockClient;
        }
      },
    }));

    const { SessionManager } = await import("../../src/connection");
    const manager = new SessionManager({
      host: "h.databricks.com",
      path: "/sql/1.0/warehouses/x",
      clientId: "sp-client-id",
      clientSecret: "sp-client-secret",
      catalog: "cat",
      schema: "sch",
    });

    await manager.getSession();

    expect(connectSpy).toHaveBeenCalledWith({
      host: "h.databricks.com",
      path: "/sql/1.0/warehouses/x",
      authType: "databricks-oauth",
      oauthClientId: "sp-client-id",
      oauthClientSecret: "sp-client-secret",
    });
    expect(mockClient.openSessionCalls[0]).toEqual({
      initialCatalog: "cat",
      initialSchema: "sch",
    });
  });
});
