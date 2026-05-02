import { DBSQLClient } from "@databricks/sql";
import type { ConnectionOptions } from "@databricks/sql/dist/contracts/IDBSQLClient";
import type IDBSQLSession from "@databricks/sql/dist/contracts/IDBSQLSession";
import type { Logger } from "drizzle-orm/logger";

import { DatabricksConnectionError } from "./errors";
import { Pool, type PoolOptions } from "./pool";
import {
  isClientConfig,
  isOAuthConfig,
  type DatabricksConfig,
  type DatabricksConnectionConfig,
} from "./types";

export const DEFAULT_SESSION_MAX_AGE_MS = 30 * 60 * 1000;

export interface SessionPoolOptions {
  max?: number;
  acquireTimeoutMs?: number;
  sessionMaxAgeMs?: number;
  logger?: Logger;
}

interface PooledSessionEntry {
  session: IDBSQLSession;
  createdAt: number;
}

interface ResolvedSessionOptions {
  initialCatalog?: string;
  initialSchema?: string;
}

/**
 * Pools `IDBSQLSession` instances backed by a single shared `DBSQLClient`.
 * Sessions are validated by age (older than `sessionMaxAgeMs` are evicted).
 */
export class SessionPool {
  private readonly pool: Pool<PooledSessionEntry>;
  private readonly sessionOptions: ResolvedSessionOptions;
  private readonly sessionMaxAgeMs: number;
  private readonly ownsClient: boolean;
  private readonly entryBySession = new Map<IDBSQLSession, PooledSessionEntry>();

  private client: DBSQLClient | undefined;
  private connecting: Promise<DBSQLClient> | undefined;
  private readonly connectArgs: ConnectionOptions | undefined;

  constructor(config: DatabricksConfig, options: SessionPoolOptions = {}) {
    this.sessionOptions = {
      initialCatalog: config.catalog,
      initialSchema: config.schema,
    };
    this.sessionMaxAgeMs = options.sessionMaxAgeMs ?? DEFAULT_SESSION_MAX_AGE_MS;

    if (isClientConfig(config)) {
      this.client = config.client;
      this.ownsClient = false;
      this.connectArgs = undefined;
    } else {
      this.ownsClient = true;
      this.connectArgs = buildConnectArgs(config);
    }

    const poolOptions: PoolOptions<PooledSessionEntry> = {
      create: () => this.createEntry(),
      destroy: (entry) => this.destroyEntry(entry),
      validate: (entry) => this.validateEntry(entry),
      maxSize: options.max,
      acquireTimeoutMs: options.acquireTimeoutMs,
      logger: options.logger,
    };
    this.pool = new Pool(poolOptions);
  }

  get size(): number {
    return this.pool.size;
  }

  get available(): number {
    return this.pool.available;
  }

  get inUse(): number {
    return this.pool.inUse;
  }

  async acquire(): Promise<IDBSQLSession> {
    const entry = await this.pool.acquire();
    return entry.session;
  }

  async release(session: IDBSQLSession): Promise<void> {
    const entry = this.entryBySession.get(session);
    if (!entry) return;
    await this.pool.release(entry);
  }

  async invalidate(session: IDBSQLSession): Promise<void> {
    const entry = this.entryBySession.get(session);
    if (!entry) return;
    entry.createdAt = 0;
    await this.pool.release(entry);
  }

  async withSession<T>(fn: (session: IDBSQLSession) => Promise<T>): Promise<T> {
    return this.runWithRetry(fn);
  }

  async runWithRetry<T>(fn: (session: IDBSQLSession) => Promise<T>): Promise<T> {
    const session = await this.acquire();
    let stale = false;
    try {
      return await fn(session);
    } catch (err) {
      if (isStaleSessionError(err)) {
        stale = true;
        await this.invalidate(session);
        const fresh = await this.acquire();
        try {
          return await fn(fresh);
        } finally {
          await this.release(fresh);
        }
      }
      throw err;
    } finally {
      if (!stale) await this.release(session);
    }
  }

  async drain(): Promise<void> {
    await this.pool.drain();
    if (this.ownsClient && this.client) {
      const client = this.client;
      this.client = undefined;
      try {
        await client.close();
      } catch {
        // best-effort
      }
    }
  }

  private async createEntry(): Promise<PooledSessionEntry> {
    const client = await this.ensureClient();
    try {
      const session = await client.openSession(this.sessionOptions);
      const entry: PooledSessionEntry = { session, createdAt: Date.now() };
      this.entryBySession.set(session, entry);
      return entry;
    } catch (err) {
      throw new DatabricksConnectionError("Failed to open Databricks session.", err);
    }
  }

  private async destroyEntry(entry: PooledSessionEntry): Promise<void> {
    this.entryBySession.delete(entry.session);
    try {
      await entry.session.close();
    } catch {
      // best-effort
    }
  }

  private async validateEntry(entry: PooledSessionEntry): Promise<boolean> {
    if (entry.createdAt === 0) return false;
    if (Date.now() - entry.createdAt >= this.sessionMaxAgeMs) return false;
    const maybe = entry.session as unknown as { isOpen?: () => boolean | Promise<boolean> };
    if (typeof maybe.isOpen === "function") {
      try {
        return await maybe.isOpen();
      } catch {
        return false;
      }
    }
    return true;
  }

  private async ensureClient(): Promise<DBSQLClient> {
    if (this.client) return this.client;
    if (this.connecting) return this.connecting;

    this.connecting = (async () => {
      try {
        const client = new DBSQLClient();
        await client.connect(this.connectArgs!);
        this.client = client;
        return client;
      } catch (err) {
        throw new DatabricksConnectionError(
          "Failed to connect to Databricks SQL warehouse. Check host, path, and credentials.",
          err,
        );
      } finally {
        this.connecting = undefined;
      }
    })();

    return this.connecting;
  }
}

function buildConnectArgs(config: DatabricksConnectionConfig): ConnectionOptions {
  if (isOAuthConfig(config)) {
    return {
      host: config.host,
      path: config.path,
      authType: "databricks-oauth",
      oauthClientId: config.clientId,
      oauthClientSecret: config.clientSecret,
    };
  }
  return { host: config.host, path: config.path, token: config.token };
}

function isStaleSessionError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const msg = String((err as { message?: unknown }).message ?? "").toLowerCase();
  return (
    msg.includes("session") &&
    (msg.includes("closed") ||
      msg.includes("expired") ||
      msg.includes("invalid") ||
      msg.includes("not found"))
  );
}
