import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksDateBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"date", "DatabricksDate">
> {
  static override readonly [entityKind] = "DatabricksDateBuilder";

  constructor(name: string) {
    super(name, "date", "DatabricksDate");
  }

  override build(table: Table): DatabricksDate {
    return new DatabricksDate(table, this.config as any);
  }
}

export class DatabricksDate extends DatabricksColumn<ColumnBaseConfig<"date", "DatabricksDate">> {
  static override readonly [entityKind] = "DatabricksDate";

  getSQLType(): string {
    return "DATE";
  }
}

export function date(): DatabricksDateBuilder;
export function date(name: string): DatabricksDateBuilder;
export function date(name?: string): DatabricksDateBuilder {
  return new DatabricksDateBuilder(name ?? "");
}
