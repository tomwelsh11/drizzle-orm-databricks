import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

type TimestampSqlName = "TIMESTAMP" | "TIMESTAMP_NTZ";

export class DatabricksTimestampBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"date", "DatabricksTimestamp">,
  { sqlName: TimestampSqlName }
> {
  static {
    (this as any)[entityKind] = "DatabricksTimestampBuilder";
  }

  constructor(name: string, sqlName: TimestampSqlName) {
    super(name, "date", "DatabricksTimestamp");
    (this.config as any).sqlName = sqlName;
  }

  override build(table: Table): DatabricksTimestamp {
    return new DatabricksTimestamp(table, this.config as any);
  }
}

export class DatabricksTimestamp extends DatabricksColumn<
  ColumnBaseConfig<"date", "DatabricksTimestamp">,
  { sqlName: TimestampSqlName }
> {
  static {
    (this as any)[entityKind] = "DatabricksTimestamp";
  }

  readonly sqlName: TimestampSqlName = (this.config as any).sqlName;

  getSQLType(): string {
    return this.sqlName;
  }

  override mapFromDriverValue(value: string | number | Date): Date {
    if (value instanceof Date) return value;
    return new Date(value);
  }
}

export function timestamp(): DatabricksTimestampBuilder;
export function timestamp(name: string): DatabricksTimestampBuilder;
export function timestamp(name?: string): DatabricksTimestampBuilder {
  return new DatabricksTimestampBuilder(name ?? "", "TIMESTAMP");
}

export function timestampNtz(): DatabricksTimestampBuilder;
export function timestampNtz(name: string): DatabricksTimestampBuilder;
export function timestampNtz(name?: string): DatabricksTimestampBuilder {
  return new DatabricksTimestampBuilder(name ?? "", "TIMESTAMP_NTZ");
}
