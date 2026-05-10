import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksVarCharBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"string", "DatabricksVarChar">,
  { length: number; enum?: string[] }
> {
  static {
    (this as any)[entityKind] = "DatabricksVarCharBuilder";
  }

  constructor(name: string, config: { length: number; enum?: string[] }) {
    super(name, "string", "DatabricksVarChar");
    this.config.length = config.length;
    this.config.enum = config.enum;
  }

  override build(table: Table): DatabricksVarChar {
    return new DatabricksVarChar(table, this.config as any);
  }
}

export class DatabricksVarChar extends DatabricksColumn<
  ColumnBaseConfig<"string", "DatabricksVarChar">,
  { length: number; enum?: string[] }
> {
  static {
    (this as any)[entityKind] = "DatabricksVarChar";
  }

  readonly length: number | undefined = (this.config as any).length;
  override readonly enumValues: string[] | undefined = (this.config as any).enum;

  override getSQLType(): string {
    return this.length ? `VARCHAR(${this.length})` : "VARCHAR";
  }
}

export function varchar(config: { length: number; enum?: string[] }): DatabricksVarCharBuilder;
export function varchar(
  name: string,
  config: { length: number; enum?: string[] },
): DatabricksVarCharBuilder;
export function varchar(
  a: string | { length: number; enum?: string[] },
  b?: { length: number; enum?: string[] },
): DatabricksVarCharBuilder {
  const name = typeof a === "string" ? a : "";
  const config = typeof a === "object" ? a : b!;
  return new DatabricksVarCharBuilder(name, config);
}
