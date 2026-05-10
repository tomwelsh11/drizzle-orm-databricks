import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksCharBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"string", "DatabricksChar">,
  { length: number; enum?: string[] }
> {
  static {
    (this as any)[entityKind] = "DatabricksCharBuilder";
  }

  constructor(name: string, config: { length: number; enum?: string[] }) {
    super(name, "string", "DatabricksChar");
    this.config.length = config.length;
    this.config.enum = config.enum;
  }

  override build(table: Table): DatabricksChar {
    return new DatabricksChar(table, this.config as any);
  }
}

export class DatabricksChar extends DatabricksColumn<
  ColumnBaseConfig<"string", "DatabricksChar">,
  { length: number; enum?: string[] }
> {
  static {
    (this as any)[entityKind] = "DatabricksChar";
  }

  readonly length: number | undefined = (this.config as any).length;
  override readonly enumValues: string[] | undefined = (this.config as any).enum;

  override getSQLType(): string {
    return this.length ? `CHAR(${this.length})` : "CHAR";
  }
}

export function char(config: { length: number; enum?: string[] }): DatabricksCharBuilder;
export function char(
  name: string,
  config: { length: number; enum?: string[] },
): DatabricksCharBuilder;
export function char(
  a: string | { length: number; enum?: string[] },
  b?: { length: number; enum?: string[] },
): DatabricksCharBuilder {
  const name = typeof a === "string" ? a : "";
  const config = typeof a === "object" ? a : b!;
  return new DatabricksCharBuilder(name, config);
}
