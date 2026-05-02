import type { DrizzleConfig } from "drizzle-orm";
import { entityKind } from "drizzle-orm/entity";
import { DefaultLogger } from "drizzle-orm/logger";
import type { SQL, SQLWrapper } from "drizzle-orm/sql";
import { SelectionProxyHandler } from "drizzle-orm/selection-proxy";
import { Subquery, WithSubquery } from "drizzle-orm/subquery";

import { SessionManager } from "./connection";
import { DatabricksDialect } from "./dialect";
import {
  DatabricksDeleteBase,
  DatabricksInsertBuilder,
  DatabricksSelectBuilder,
  DatabricksUpdateBuilder,
} from "./query-builders";
import { DatabricksSession, type DatabricksRawQueryResult } from "./session";
import type { DatabricksTable } from "./table";
import type {
  DatabricksClientConfig,
  DatabricksConfig,
  DatabricksConnectionConfig,
  DatabricksOAuthConnectionConfig,
  DatabricksTokenConnectionConfig,
} from "./types";

export class DatabricksDatabase<TSchema extends Record<string, unknown> = Record<string, never>> {
  static readonly [entityKind]: string = "DatabricksDatabase";

  constructor(
    /** @internal */
    readonly dialect: DatabricksDialect,
    /** @internal */
    readonly session: DatabricksSession,
  ) {}

  select(): DatabricksSelectBuilder<undefined>;
  select<TSelection extends Record<string, unknown>>(
    fields: TSelection,
  ): DatabricksSelectBuilder<TSelection>;
  select(fields?: Record<string, unknown>) {
    return new DatabricksSelectBuilder({
      fields: fields ?? undefined,
      session: this.session,
      dialect: this.dialect,
    });
  }

  selectDistinct(): DatabricksSelectBuilder<undefined>;
  selectDistinct<TSelection extends Record<string, unknown>>(
    fields: TSelection,
  ): DatabricksSelectBuilder<TSelection>;
  selectDistinct(fields?: Record<string, unknown>) {
    return new DatabricksSelectBuilder({
      fields: fields ?? undefined,
      session: this.session,
      dialect: this.dialect,
      distinct: true,
    });
  }

  insert<TTable extends DatabricksTable<any>>(table: TTable): DatabricksInsertBuilder<TTable> {
    return new DatabricksInsertBuilder(table, this.session, this.dialect);
  }

  update<TTable extends DatabricksTable<any>>(table: TTable): DatabricksUpdateBuilder<TTable> {
    return new DatabricksUpdateBuilder(table, this.session, this.dialect);
  }

  delete<TTable extends DatabricksTable<any>>(table: TTable): DatabricksDeleteBase<TTable> {
    return new DatabricksDeleteBase(table, this.session, this.dialect);
  }

  $with = <TAlias extends string>(alias: TAlias, selection?: Record<string, unknown>) => {
    const self = this;
    const as = (
      qb:
        | ((qb: DatabricksDatabase<TSchema>) => SQLWrapper & { getSelectedFields?: () => any })
        | (SQLWrapper & { getSelectedFields?: () => any }),
    ) => {
      const resolved = typeof qb === "function" ? qb(self) : qb;
      return new Proxy(
        new WithSubquery(
          resolved.getSQL(),
          selection ??
            ("getSelectedFields" in resolved && typeof resolved.getSelectedFields === "function"
              ? (resolved.getSelectedFields() ?? {})
              : {}),
          alias,
          true,
        ),
        new SelectionProxyHandler({ alias, sqlAliasedBehavior: "alias", sqlBehavior: "error" }),
      ) as unknown as Subquery;
    };
    return { as };
  };

  with(...queries: Subquery[]) {
    const self = this;
    function select(): DatabricksSelectBuilder<undefined>;
    function select<TSelection extends Record<string, unknown>>(
      fields: TSelection,
    ): DatabricksSelectBuilder<TSelection>;
    function select(fields?: Record<string, unknown>) {
      return new DatabricksSelectBuilder({
        fields: fields ?? undefined,
        session: self.session,
        dialect: self.dialect,
        withList: queries,
      });
    }
    function selectDistinct(): DatabricksSelectBuilder<undefined>;
    function selectDistinct<TSelection extends Record<string, unknown>>(
      fields: TSelection,
    ): DatabricksSelectBuilder<TSelection>;
    function selectDistinct(fields?: Record<string, unknown>) {
      return new DatabricksSelectBuilder({
        fields: fields ?? undefined,
        session: self.session,
        dialect: self.dialect,
        withList: queries,
        distinct: true,
      });
    }
    function update<TTable extends DatabricksTable<any>>(table: TTable) {
      return new DatabricksUpdateBuilder(table, self.session, self.dialect, queries);
    }
    function insert<TTable extends DatabricksTable<any>>(table: TTable) {
      return new DatabricksInsertBuilder(table, self.session, self.dialect, queries);
    }
    function delete_<TTable extends DatabricksTable<any>>(table: TTable) {
      return new DatabricksDeleteBase(table, self.session, self.dialect, queries);
    }
    return { select, selectDistinct, update, insert, delete: delete_ };
  }

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
