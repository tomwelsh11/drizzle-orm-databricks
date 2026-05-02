import { entityKind } from "drizzle-orm/entity";
import { QueryPromise } from "drizzle-orm/query-promise";
import { SelectionProxyHandler } from "drizzle-orm/selection-proxy";
import type { SQL } from "drizzle-orm/sql";
import { Subquery } from "drizzle-orm/subquery";
import { Table } from "drizzle-orm/table";
import type { Column } from "drizzle-orm/column";

import type { DatabricksDialect } from "../dialect";
import type { DatabricksSession } from "../session";
import type { DatabricksTable, NamespaceOverride } from "../table";

const TableSymbol = (Table as any).Symbol as { Columns: symbol };

const { mapUpdateSet } = require("drizzle-orm/utils") as {
  mapUpdateSet: (table: any, values: any) => Record<string, unknown>;
};

export class DatabricksUpdateBuilder<TTable extends DatabricksTable<any>> {
  static readonly [entityKind]: string = "DatabricksUpdateBuilder";

  constructor(
    private table: TTable,
    private session: DatabricksSession,
    private dialect: DatabricksDialect,
    private withList?: Subquery[],
    private namespaceOverride?: NamespaceOverride,
  ) {}

  set(values: Record<string, unknown>) {
    return new DatabricksUpdateBase(
      this.table,
      mapUpdateSet(this.table, values),
      this.session,
      this.dialect,
      this.withList,
      this.namespaceOverride,
    );
  }
}

export class DatabricksUpdateBase<TTable extends DatabricksTable<any>> extends QueryPromise<void> {
  static override readonly [entityKind]: string = "DatabricksUpdate";

  config: {
    set: Record<string, unknown>;
    table: TTable;
    where?: SQL;
    withList?: Subquery[];
    orderBy?: (SQL | Column)[];
    limit?: number | SQL;
    namespaceOverride?: NamespaceOverride;
  };

  constructor(
    table: TTable,
    set: Record<string, unknown>,
    private session: DatabricksSession,
    private dialect: DatabricksDialect,
    withList?: Subquery[],
    namespaceOverride?: NamespaceOverride,
  ) {
    super();
    this.config = { set, table, withList, namespaceOverride };
  }

  where(where: SQL | undefined) {
    this.config.where = where;
    return this;
  }

  orderBy(...columns: (SQL | Column)[] | [(fields: any) => (SQL | Column)[] | SQL | Column]) {
    if (typeof columns[0] === "function") {
      const orderBy = (columns[0] as Function)(
        new Proxy(
          (this.config.table as any)[TableSymbol.Columns],
          new SelectionProxyHandler({ sqlAliasedBehavior: "alias", sqlBehavior: "sql" }),
        ),
      );
      const orderByArray = Array.isArray(orderBy) ? orderBy : [orderBy];
      this.config.orderBy = orderByArray;
    } else {
      this.config.orderBy = columns as (SQL | Column)[];
    }
    return this;
  }

  limit(limit: number | SQL) {
    this.config.limit = limit;
    return this;
  }

  /** @internal */
  getSQL(): SQL {
    return this.dialect.buildUpdateQuery(this.config as any);
  }

  toSQL() {
    const { typings: _typings, ...rest } = this.dialect.sqlToQuery(this.getSQL());
    return rest;
  }

  prepare() {
    return this.session.prepareQuery(this.dialect.sqlToQuery(this.getSQL()));
  }

  override execute = (placeholderValues?: Record<string, unknown>): Promise<void> => {
    return this.prepare().execute(placeholderValues) as Promise<void>;
  };

  $dynamic() {
    return this;
  }
}
