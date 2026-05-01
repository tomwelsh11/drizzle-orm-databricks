import { vi } from 'vitest';

export interface RecordedStatement {
  sql: string;
  params: unknown[];
}

export interface QueuedResponse {
  rows?: Array<Record<string, unknown>>;
  error?: Error;
}

export class MockOperation {
  closed = false;

  constructor(private readonly response: QueuedResponse) {}

  async fetchAll(): Promise<Array<Record<string, unknown>>> {
    if (this.response.error) throw this.response.error;
    return this.response.rows ?? [];
  }

  async close(): Promise<void> {
    this.closed = true;
  }
}

export class MockSession {
  closed = false;
  readonly recorded: RecordedStatement[];
  readonly responseQueue: QueuedResponse[];
  readonly openSessionConfig: Record<string, unknown> | undefined;

  constructor(
    recorded: RecordedStatement[],
    responseQueue: QueuedResponse[],
    openConfig?: Record<string, unknown>,
  ) {
    this.recorded = recorded;
    this.responseQueue = responseQueue;
    this.openSessionConfig = openConfig;
  }

  async executeStatement(
    sql: string,
    options: { ordinalParameters?: unknown[] } = {},
  ): Promise<MockOperation> {
    this.recorded.push({ sql, params: options.ordinalParameters ?? [] });
    const next = this.responseQueue.shift() ?? { rows: [] };
    return new MockOperation(next);
  }

  async close(): Promise<void> {
    this.closed = true;
  }
}

export class MockDBSQLClient {
  recorded: RecordedStatement[] = [];
  responseQueue: QueuedResponse[] = [];
  sessions: MockSession[] = [];
  openSessionCalls: Array<Record<string, unknown> | undefined> = [];
  openSessionError: Error | undefined;
  closed = false;

  async connect(_opts: unknown): Promise<this> {
    return this;
  }

  async openSession(
    config?: Record<string, unknown>,
  ): Promise<MockSession> {
    this.openSessionCalls.push(config);
    if (this.openSessionError) throw this.openSessionError;
    const session = new MockSession(
      this.recorded,
      this.responseQueue,
      config,
    );
    this.sessions.push(session);
    return session;
  }

  async close(): Promise<void> {
    this.closed = true;
  }

  queueResponse(rows: Array<Record<string, unknown>>): void {
    this.responseQueue.push({ rows });
  }

  queueError(err: Error): void {
    this.responseQueue.push({ error: err });
  }
}

/**
 * Install a vi.mock for `@databricks/sql` that returns the given client
 * instance whenever `new DBSQLClient()` is called. Call before importing
 * the module under test.
 */
export function installDatabricksMock(client: MockDBSQLClient): void {
  vi.doMock('@databricks/sql', () => ({
    DBSQLClient: vi.fn(() => client),
  }));
}
