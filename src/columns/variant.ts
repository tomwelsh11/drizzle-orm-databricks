import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export class DatabricksVariantBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"json", "DatabricksVariant">
> {
  static {
    (this as any)[entityKind] = "DatabricksVariantBuilder";
  }

  constructor(name: string) {
    super(name, "json", "DatabricksVariant");
  }

  override build(table: Table): DatabricksVariant {
    return new DatabricksVariant(table, this.config as any);
  }
}

export class DatabricksVariant extends DatabricksColumn<
  ColumnBaseConfig<"json", "DatabricksVariant">
> {
  static {
    (this as any)[entityKind] = "DatabricksVariant";
  }

  getSQLType(): string {
    return "VARIANT";
  }

  override mapFromDriverValue(value: unknown): unknown {
    if (typeof value === "string") {
      try {
        return JSON.parse(value);
      } catch {
        return value;
      }
    }
    return value;
  }

  override mapToDriverValue(value: unknown): string {
    return typeof value === "string" ? value : JSON.stringify(value);
  }
}

export function variant(): DatabricksVariantBuilder;
export function variant(name: string): DatabricksVariantBuilder;
export function variant(name?: string): DatabricksVariantBuilder {
  return new DatabricksVariantBuilder(name ?? "");
}
