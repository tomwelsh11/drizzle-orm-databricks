import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksFloatBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"number", "DatabricksFloat">
> {
  static { (this as any)[entityKind] = "DatabricksFloatBuilder"; }

  constructor(name: string) {
    super(name, "number", "DatabricksFloat");
  }

  override build(table: Table): DatabricksFloat {
    return new DatabricksFloat(table, this.config as any);
  }
}

export class DatabricksFloat extends DatabricksColumn<
  ColumnBaseConfig<"number", "DatabricksFloat">
> {
  static { (this as any)[entityKind] = "DatabricksFloat"; }

  getSQLType(): string {
    return "FLOAT";
  }
}

export function float(): DatabricksFloatBuilder;
export function float(name: string): DatabricksFloatBuilder;
export function float(name?: string): DatabricksFloatBuilder {
  return new DatabricksFloatBuilder(name ?? "");
}
