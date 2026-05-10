import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksBooleanBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"boolean", "DatabricksBoolean">
> {
  static {
    (this as any)[entityKind] = "DatabricksBooleanBuilder";
  }

  constructor(name: string) {
    super(name, "boolean", "DatabricksBoolean");
  }

  override build(table: Table): DatabricksBoolean {
    return new DatabricksBoolean(table, this.config as any);
  }
}

export class DatabricksBoolean extends DatabricksColumn<
  ColumnBaseConfig<"boolean", "DatabricksBoolean">
> {
  static {
    (this as any)[entityKind] = "DatabricksBoolean";
  }

  getSQLType(): string {
    return "BOOLEAN";
  }

  override mapFromDriverValue(value: boolean | number | string): boolean {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") return value === 1;
    return value === "true" || value === "1";
  }
}

export function boolean(): DatabricksBooleanBuilder;
export function boolean(name: string): DatabricksBooleanBuilder;
export function boolean(name?: string): DatabricksBooleanBuilder {
  return new DatabricksBooleanBuilder(name ?? "");
}
