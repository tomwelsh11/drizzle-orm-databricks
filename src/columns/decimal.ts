import type { ColumnBaseConfig } from "drizzle-orm/column";
import type { ColumnBuilderBaseConfig } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import type { Table } from "drizzle-orm/table";
import { DatabricksColumn, DatabricksColumnBuilder } from "./common";

export interface DatabricksDecimalConfig {
  precision?: number;
  scale?: number;
}

export class DatabricksDecimalBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<"string", "DatabricksDecimal">
> {
  static override readonly [entityKind] = "DatabricksDecimalBuilder";

  readonly precision: number;
  readonly scale: number;

  constructor(name: string, config?: DatabricksDecimalConfig) {
    super(name, "string", "DatabricksDecimal");
    this.precision = config?.precision ?? 10;
    this.scale = config?.scale ?? 0;
    (this.config as any).precision = this.precision;
    (this.config as any).scale = this.scale;
  }

  override build(table: Table): DatabricksDecimal {
    return new DatabricksDecimal(table, this.config as any);
  }
}

export class DatabricksDecimal extends DatabricksColumn<
  ColumnBaseConfig<"string", "DatabricksDecimal">
> {
  static override readonly [entityKind] = "DatabricksDecimal";

  readonly precision: number = (this.config as any).precision ?? 10;
  readonly scale: number = (this.config as any).scale ?? 0;

  getSQLType(): string {
    return `DECIMAL(${this.precision}, ${this.scale})`;
  }
}

export function decimal(config?: DatabricksDecimalConfig): DatabricksDecimalBuilder;
export function decimal(name: string, config?: DatabricksDecimalConfig): DatabricksDecimalBuilder;
export function decimal(
  nameOrConfig?: string | DatabricksDecimalConfig,
  config?: DatabricksDecimalConfig,
): DatabricksDecimalBuilder {
  if (typeof nameOrConfig === "string") {
    return new DatabricksDecimalBuilder(nameOrConfig, config);
  }
  return new DatabricksDecimalBuilder("", nameOrConfig);
}
