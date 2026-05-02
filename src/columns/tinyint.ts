import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksTinyIntBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"number", "DatabricksTinyInt">
> {
  static override readonly [entityKind] = "DatabricksTinyIntBuilder";

  constructor(name: string) {
    super(name, "number", "DatabricksTinyInt");
  }

  override build(table: Table): DatabricksTinyInt {
    return new DatabricksTinyInt(table, this.config as any);
  }
}

export class DatabricksTinyInt extends DatabricksColumn<
  ColumnBaseConfig<"number", "DatabricksTinyInt">
> {
  static override readonly [entityKind] = "DatabricksTinyInt";

  getSQLType(): string {
    return "TINYINT";
  }
}

export function tinyint(): DatabricksTinyIntBuilder;
export function tinyint(name: string): DatabricksTinyIntBuilder;
export function tinyint(name?: string): DatabricksTinyIntBuilder {
  return new DatabricksTinyIntBuilder(name ?? "");
}
