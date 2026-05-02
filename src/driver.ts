import type { DrizzleConfig } from 'drizzle-orm';
import { entityKind } from 'drizzle-orm/entity';
import { DefaultLogger } from 'drizzle-orm/logger';
import type { SQL, SQLWrapper } from 'drizzle-orm/sql';

import { SessionManager } from './connection';
import { DatabricksDialect } from './dialect';
import { DatabricksSession, type DatabricksRawQueryResult } from './session';
import type { DatabricksClientConfig, DatabricksConfig, DatabricksConnectionConfig, DatabricksOAuthConnectionConfig, DatabricksTokenConnectionConfig } from './types';

export class DatabricksDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>,
> {
  static readonly [entityKind]: string = 'DatabricksDatabase';

  constructor(
    /** @internal */
    readonly dialect: DatabricksDialect,
    /** @internal */
    readonly session: DatabricksSession,
  ) {}

  async execute<T extends Record<string, unknown> = Record<string, unknown>>(
    query: SQLWrapper,
  ): Promise<T[]> {
    const sql: SQL = query.getSQL();
    return this.session.execute<T[]>(sql);
  }

  async run(query: SQLWrapper): Promise<DatabricksRawQueryResult> {
    const sql: SQL = query.getSQL();
    return this.session.execute<DatabricksRawQueryResult>(sql);
  }

  async all<T = unknown>(query: SQLWrapper): Promise<T[]> {
    const sql: SQL = query.getSQL();
    return this.session.all<T>(sql);
  }

  /** Closes the underlying Databricks session and (if owned) client. */
  $close!: () => Promise<void>;
}

export function drizzle<TSchema extends Record<string, unknown> = Record<string, never>>(
  config: DatabricksTokenConnectionConfig,
  drizzleConfig?: DrizzleConfig<TSchema>,
): DatabricksDatabase<TSchema>;
export function drizzle<TSchema extends Record<string, unknown> = Record<string, never>>(
  config: DatabricksOAuthConnectionConfig,
  drizzleConfig?: DrizzleConfig<TSchema>,
): DatabricksDatabase<TSchema>;
export function drizzle<TSchema extends Record<string, unknown> = Record<string, never>>(
  config: DatabricksClientConfig,
  drizzleConfig?: DrizzleConfig<TSchema>,
): DatabricksDatabase<TSchema>;
export function drizzle<TSchema extends Record<string, unknown> = Record<string, never>>(
  config: DatabricksConfig,
  drizzleConfig: DrizzleConfig<TSchema> = {},
): DatabricksDatabase<TSchema> {
  const sessionManager = new SessionManager(config);
  const dialect = new DatabricksDialect({ casing: drizzleConfig.casing });

  let logger;
  if (drizzleConfig.logger === true) {
    logger = new DefaultLogger();
  } else if (drizzleConfig.logger !== false) {
    logger = drizzleConfig.logger;
  }

  const session = new DatabricksSession(sessionManager, dialect, { logger });
  const db = new DatabricksDatabase<TSchema>(dialect, session);

  db.$close = () => sessionManager.close();
  return db;
}
