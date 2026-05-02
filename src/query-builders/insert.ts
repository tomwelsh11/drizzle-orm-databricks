import { entityKind, is } from "drizzle-orm/entity";
import { QueryPromise } from "drizzle-orm/query-promise";
import { Param, SQL } from "drizzle-orm/sql";
import { Subquery } from "drizzle-orm/subquery";
import { Table } from "drizzle-orm/table";
import { haveSameKeys } from "drizzle-orm/utils";

import { DatabricksQueryBuilder } from "./query-builder";

import type { DatabricksDialect } from "../dialect";
import type { DatabricksSession } from "../session";
import type { DatabricksTable } from "../table";

const TableSymbol = (Table as any).Symbol as { Columns: symbol };

export class DatabricksInsertBuilder<TTable extends DatabricksTable<any>> {
  static readonly [entityKind]: string = "DatabricksInsertBuilder";

  constructor(
    private table: TTable,
    private session: DatabricksSession,
    private dialect: DatabricksDialect,
    private withList?: Subquery[],
  ) {}

  values(values: Record<string, unknown> | Record<string, unknown>[]) {
    values = Array.isArray(values) ? values : [values];
    if (values.length === 0) {
      throw new Error("values() must be called with at least one value");
    }

    const mappedValues = values.map((entry) => {
      const result: Record<string, SQL | Param> = {};
      const cols: Record<string, any> = (this.table as any)[TableSymbol.Columns];
      for (const colKey of Object.keys(entry)) {
        const colValue = entry[colKey];
        result[colKey] = is(colValue, SQL) ? colValue : new Param(colValue, cols[colKey]);
      }
      return result;
    });

    return new DatabricksInsertBase(
      this.table,
      mappedValues,
      this.session,
      this.dialect,
      false,
      this.withList,
    );
  }

  select(
    selectQuery:
      | ((qb: DatabricksQueryBuilder) => SQL | { _: { selectedFields: Record<string, unknown> } })
      | SQL
      | { _: { selectedFields: Record<string, unknown> } },
  ) {
    const select =
      typeof selectQuery === "function"
        ? selectQuery(new DatabricksQueryBuilder(this.dialect))
        : selectQuery;
    if (
      !is(select, SQL) &&
      !haveSameKeys((this.table as any)[TableSymbol.Columns], (select as any)._.selectedFields)
    ) {
      throw new Error(
        "Insert select error: selected fields are not the same or are in a different order compared to the table definition",
      );
    }
    return new DatabricksInsertBase(
      this.table,
      select as any,
      this.session,
      this.dialect,
      true,
      this.withList,
    );
  }
}

export class DatabricksInsertBase<TTable extends DatabricksTable<any>> extends QueryPromise<void> {
  static override readonly [entityKind]: string = "DatabricksInsert";

  config: {
    table: TTable;
    values: Record<string, SQL | Param>[] | SQL;
    select?: boolean;
    onConflict?: SQL;
    withList?: Subquery[];
  };

  constructor(
    table: TTable,
    values: Record<string, SQL | Param>[] | SQL,
    private session: DatabricksSession,
    private dialect: DatabricksDialect,
    select?: boolean,
    withList?: Subquery[],
  ) {
    super();
    this.config = { table, values, select, withList };
  }

  /** @internal */
  getSQL(): SQL {
    return this.dialect.buildInsertQuery(this.config).sql;
  }

  toSQL() {
    const { typings: _typings, ...rest } = this.dialect.sqlToQuery(this.getSQL());
    return rest;
  }

  prepare() {
    const { sql } = this.dialect.buildInsertQuery(this.config);
    return this.session.prepareQuery(this.dialect.sqlToQuery(sql));
  }

  override execute = (placeholderValues?: Record<string, unknown>): Promise<void> => {
    return this.prepare().execute(placeholderValues) as Promise<void>;
  };

  $dynamic() {
    return this;
  }
}
