import { DBSQLClient } from '@databricks/sql';
import type IDBSQLSession from '@databricks/sql/dist/contracts/IDBSQLSession';
import { DatabricksConnectionError } from './errors';
import { isClientConfig, type DatabricksConfig } from './types';

interface ResolvedSessionOptions {
  initialCatalog?: string;
  initialSchema?: string;
}

export class SessionManager {
  private client: DBSQLClient | undefined;
  private session: IDBSQLSession | undefined;
  private connecting: Promise<void> | undefined;
  private readonly ownsClient: boolean;
  private readonly sessionOptions: ResolvedSessionOptions;
  private readonly connectArgs: { host: string; path: string; token: string } | undefined;

  constructor(config: DatabricksConfig) {
    this.sessionOptions = {
      initialCatalog: config.catalog,
      initialSchema: config.schema,
    };

    if (isClientConfig(config)) {
      this.client = config.client;
      this.ownsClient = false;
      this.connectArgs = undefined;
    } else {
      this.ownsClient = true;
      this.connectArgs = { host: config.host, path: config.path, token: config.token };
    }
  }

  async getSession(): Promise<IDBSQLSession> {
    if (this.session && (await this.isSessionAlive(this.session))) {
      return this.session;
    }
    await this.ensureClient();
    this.session = await this.openSession();
    return this.session;
  }

  async runWithRetry<T>(fn: (session: IDBSQLSession) => Promise<T>): Promise<T> {
    const session = await this.getSession();
    try {
      return await fn(session);
    } catch (err) {
      if (!isStaleSessionError(err)) throw err;
      this.session = undefined;
      const fresh = await this.getSession();
      return await fn(fresh);
    }
  }

  async close(): Promise<void> {
    const session = this.session;
    this.session = undefined;
    if (session) {
      try {
        await session.close();
      } catch {
        // best-effort
      }
    }
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

  private async ensureClient(): Promise<void> {
    if (this.client) return;
    if (this.connecting) return this.connecting;

    this.connecting = (async () => {
      try {
        const client = new DBSQLClient();
        await client.connect(this.connectArgs!);
        this.client = client;
      } catch (err) {
        throw new DatabricksConnectionError(
          'Failed to connect to Databricks SQL warehouse. Check host, path, and token.',
          err,
        );
      } finally {
        this.connecting = undefined;
      }
    })();

    return this.connecting;
  }

  private async openSession(): Promise<IDBSQLSession> {
    if (!this.client) {
      throw new DatabricksConnectionError('Databricks client is not initialized.');
    }
    try {
      return await this.client.openSession(this.sessionOptions);
    } catch (err) {
      throw new DatabricksConnectionError('Failed to open Databricks session.', err);
    }
  }

  private async isSessionAlive(session: IDBSQLSession): Promise<boolean> {
    const maybe = session as unknown as { isOpen?: () => boolean | Promise<boolean> };
    if (typeof maybe.isOpen === 'function') {
      try {
        return await maybe.isOpen();
      } catch {
        return false;
      }
    }
    return true;
  }
}

function isStaleSessionError(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const msg = String((err as { message?: unknown }).message ?? '').toLowerCase();
  return (
    msg.includes('session') &&
    (msg.includes('closed') ||
      msg.includes('expired') ||
      msg.includes('invalid') ||
      msg.includes('not found'))
  );
}
