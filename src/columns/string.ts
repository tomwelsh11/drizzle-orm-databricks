import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksStringBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"string", "DatabricksString">
> {
  static override readonly [entityKind] = "DatabricksStringBuilder";

  constructor(name: string) {
    super(name, "string", "DatabricksString");
  }

  override build(table: Table): DatabricksString {
    return new DatabricksString(table, this.config as any);
  }
}

export class DatabricksString extends DatabricksColumn<
  ColumnBaseConfig<"string", "DatabricksString">
> {
  static override readonly [entityKind] = "DatabricksString";

  getSQLType(): string {
    return "STRING";
  }
}

export function string(): DatabricksStringBuilder;
export function string(name: string): DatabricksStringBuilder;
export function string(name?: string): DatabricksStringBuilder {
  return new DatabricksStringBuilder(name ?? "");
}
