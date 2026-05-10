import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksDoubleBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"number", "DatabricksDouble">
> {
  static { (this as any)[entityKind] = "DatabricksDoubleBuilder"; }

  constructor(name: string) {
    super(name, "number", "DatabricksDouble");
  }

  override build(table: Table): DatabricksDouble {
    return new DatabricksDouble(table, this.config as any);
  }
}

export class DatabricksDouble extends DatabricksColumn<
  ColumnBaseConfig<"number", "DatabricksDouble">
> {
  static { (this as any)[entityKind] = "DatabricksDouble"; }

  getSQLType(): string {
    return "DOUBLE";
  }
}

export function double(): DatabricksDoubleBuilder;
export function double(name: string): DatabricksDoubleBuilder;
export function double(name?: string): DatabricksDoubleBuilder {
  return new DatabricksDoubleBuilder(name ?? "");
}
