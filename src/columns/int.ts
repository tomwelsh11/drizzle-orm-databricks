import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksIntBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"number", "DatabricksInt">
> {
  static override readonly [entityKind] = "DatabricksIntBuilder";

  constructor(name: string) {
    super(name, "number", "DatabricksInt");
  }

  override build(table: Table): DatabricksInt {
    return new DatabricksInt(table, this.config as any);
  }
}

export class DatabricksInt extends DatabricksColumn<ColumnBaseConfig<"number", "DatabricksInt">> {
  static override readonly [entityKind] = "DatabricksInt";

  getSQLType(): string {
    return "INT";
  }
}

export function int(): DatabricksIntBuilder;
export function int(name: string): DatabricksIntBuilder;
export function int(name?: string): DatabricksIntBuilder {
  return new DatabricksIntBuilder(name ?? "");
}
