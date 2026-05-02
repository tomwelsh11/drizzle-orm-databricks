import { Column } from "drizzle-orm/column";
import { entityKind, is } from "drizzle-orm/entity";
import type { Logger } from "drizzle-orm/logger";
import { NoopLogger } from "drizzle-orm/logger";
import { fillPlaceholders, SQL, type Query } from "drizzle-orm/sql";

import type { SessionManager } from "./connection";
import type { DatabricksDialect } from "./dialect";
import { DatabricksUnsupportedError } from "./errors";

type FieldMapping = {
  path: string[];
  field: Column | SQL | SQL.Aliased;
}[];

const mapResultRow = (
  columns: FieldMapping,
  row: unknown[],
  joinsNotNullableMap?: Record<string, boolean>,
): Record<string, unknown> => {
  const result: Record<string, unknown> = {};
  for (const [columnIndex, { path, field }] of columns.entries()) {
    let decoder: { mapFromDriverValue(value: unknown): unknown };
    if (is(field, Column)) {
      decoder = field;
    } else if (is(field, SQL)) {
      decoder = (field as any).decoder;
    } else {
      decoder = (field as any).sql.decoder;
    }
    let node: Record<string, unknown> = result;
    for (const [pathChunkIndex, pathChunk] of path.entries()) {
      if (pathChunkIndex < path.length - 1) {
        if (!(pathChunk in node)) {
          node[pathChunk] = {};
        }
        node = node[pathChunk] as Record<string, unknown>;
      } else {
        const rawValue = row[columnIndex];
        node[pathChunk] = rawValue === null ? null : decoder.mapFromDriverValue(rawValue);
      }
    }
  }
  if (joinsNotNullableMap) {
    for (const [key, isNullable] of Object.entries(joinsNotNullableMap)) {
      if (!isNullable && result[key] === null) {
        result[key] = null;
      }
    }
  }
  return result;
};

export interface DatabricksRawQueryResult {
  rows: unknown[];
}

export interface DatabricksSessionOptions {
  logger?: Logger;
}

export class DatabricksPreparedQuery {
  static readonly [entityKind]: string = "DatabricksPreparedQuery";

  constructor(
    private connection: SessionManager,
    private queryString: string,
    private params: unknown[],
    private logger: Logger,
    private fields: FieldMapping | undefined,
    private customResultMapper?: (rows: unknown[][]) => unknown,
  ) {}

  async execute(placeholderValues: Record<string, unknown> | undefined = {}): Promise<unknown> {
    const params = fillPlaceholders(this.params, placeholderValues);
    const { fields, queryString, logger, customResultMapper } = this;

    logger.logQuery(queryString, params);

    const rows = await this.runStatement(queryString, params);

    if (!fields && !customResultMapper) {
      return { rows } as DatabricksRawQueryResult;
    }

    if (customResultMapper) {
      return customResultMapper(rows as unknown[][]);
    }

    return (rows as unknown[][]).map((row) => mapResultRow(fields!, row));
  }

  iterator(_placeholderValues?: Record<string, unknown>): AsyncGenerator<unknown> {
    throw new DatabricksUnsupportedError(
      "Streaming iteration",
      "Use execute() to fetch results as a single batch.",
    );
  }

  private async runStatement(sqlText: string, params: unknown[]): Promise<unknown[]> {
    return this.connection.runWithRetry(async (session) => {
      const op = await session.executeStatement(sqlText, {
        ordinalParameters: params as never,
      });
      try {
        const result = await op.fetchAll();
        return (result ?? []) as unknown[];
      } finally {
        try {
          await op.close();
        } catch {
          // best-effort
        }
      }
    });
  }
}

export class DatabricksSession {
  static readonly [entityKind]: string = "DatabricksSession";

  private logger: Logger;

  constructor(
    private connection: SessionManager,
    private dialect: DatabricksDialect,
    options: DatabricksSessionOptions = {},
  ) {
    this.logger = options.logger ?? new NoopLogger();
  }

  prepareQuery(
    query: Query,
    fields?: FieldMapping,
    customResultMapper?: (rows: unknown[][]) => unknown,
  ): DatabricksPreparedQuery {
    return new DatabricksPreparedQuery(
      this.connection,
      query.sql,
      query.params,
      this.logger,
      fields,
      customResultMapper,
    );
  }

  async execute<T = unknown>(query: SQL): Promise<T> {
    const compiled = this.dialect.sqlToQuery(query);
    this.logger.logQuery(compiled.sql, compiled.params);
    const rows = await this.connection.runWithRetry(async (session) => {
      const op = await session.executeStatement(compiled.sql, {
        ordinalParameters: compiled.params as never,
      });
      try {
        return (await op.fetchAll()) ?? [];
      } finally {
        try {
          await op.close();
        } catch {
          // best-effort
        }
      }
    });
    return rows as T;
  }

  async all<T = unknown>(query: SQL): Promise<T[]> {
    return this.execute<T[]>(query);
  }

  async transaction<T>(_transaction: () => Promise<T>): Promise<T> {
    throw new DatabricksUnsupportedError(
      "Transactions",
      "Databricks does not support multi-statement transactions in this adapter. Use single statements or MERGE for atomic multi-row writes.",
    );
  }
}
