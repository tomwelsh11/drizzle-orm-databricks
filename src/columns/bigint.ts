import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksBigIntBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"bigint", "DatabricksBigInt">
> {
  static override readonly [entityKind] = "DatabricksBigIntBuilder";

  constructor(name: string) {
    super(name, "bigint", "DatabricksBigInt");
  }

  override build(table: Table): DatabricksBigInt {
    return new DatabricksBigInt(table, this.config as any);
  }
}

export class DatabricksBigInt extends DatabricksColumn<
  ColumnBaseConfig<"bigint", "DatabricksBigInt">
> {
  static override readonly [entityKind] = "DatabricksBigInt";

  getSQLType(): string {
    return "BIGINT";
  }

  override mapFromDriverValue(value: string | number | bigint): bigint {
    return BigInt(value as never);
  }

  override mapToDriverValue(value: bigint): string {
    return value.toString();
  }
}

export function bigint(): DatabricksBigIntBuilder;
export function bigint(name: string): DatabricksBigIntBuilder;
export function bigint(name?: string): DatabricksBigIntBuilder {
  return new DatabricksBigIntBuilder(name ?? "");
}
