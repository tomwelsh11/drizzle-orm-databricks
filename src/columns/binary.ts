import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksBinaryBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"buffer", "DatabricksBinary">
> {
  static override readonly [entityKind] = "DatabricksBinaryBuilder";

  constructor(name: string) {
    super(name, "buffer", "DatabricksBinary");
  }

  override build(table: Table): DatabricksBinary {
    return new DatabricksBinary(table, this.config as any);
  }
}

export class DatabricksBinary extends DatabricksColumn<
  ColumnBaseConfig<"buffer", "DatabricksBinary">
> {
  static override readonly [entityKind] = "DatabricksBinary";

  getSQLType(): string {
    return "BINARY";
  }
}

export function binary(): DatabricksBinaryBuilder;
export function binary(name: string): DatabricksBinaryBuilder;
export function binary(name?: string): DatabricksBinaryBuilder {
  return new DatabricksBinaryBuilder(name ?? "");
}
