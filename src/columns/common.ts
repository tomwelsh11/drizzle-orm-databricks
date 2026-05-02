import type { ColumnBaseConfig } from 'drizzle-orm/column';
import { Column } from 'drizzle-orm/column';
import type { ColumnBuilderBaseConfig, ColumnBuilderExtraConfig, ColumnDataType, GeneratedColumnConfig } from 'drizzle-orm/column-builder';
import { ColumnBuilder } from 'drizzle-orm/column-builder';
import { entityKind } from 'drizzle-orm/entity';
import type { SQL } from 'drizzle-orm/sql';
import type { Table } from 'drizzle-orm/table';

const TableNameSymbol = Symbol.for('drizzle:Name');

function uniqueKeyName(table: Table, columns: string[]): string {
  const tableName = (table as unknown as Record<symbol, string>)[TableNameSymbol] ?? 'unknown';
  return `${tableName}_${columns.join('_')}_unique`;
}

export abstract class DatabricksColumnBuilder<
  T extends ColumnBuilderBaseConfig<ColumnDataType, string> = ColumnBuilderBaseConfig<ColumnDataType, string>,
  TRuntimeConfig extends object = object,
  TTypeConfig extends object = object,
  TExtraConfig extends ColumnBuilderExtraConfig = ColumnBuilderExtraConfig,
> extends ColumnBuilder<T, TRuntimeConfig, TTypeConfig, TExtraConfig> {
  static override readonly [entityKind]: string = 'DatabricksColumnBuilder';

  unique(name?: string): this {
    this.config.isUnique = true;
    this.config.uniqueName = name;
    return this;
  }

  override generatedAlwaysAs(
    as: SQL | T['data'] | (() => SQL),
    config?: Partial<GeneratedColumnConfig<unknown>>,
  ): any {
    this.config.generated = {
      as,
      type: 'always',
      mode: config?.mode ?? 'stored',
    };
    return this as any;
  }

  abstract build(table: Table): DatabricksColumn;
}

export abstract class DatabricksColumn<
  T extends ColumnBaseConfig<ColumnDataType, string> = ColumnBaseConfig<ColumnDataType, string>,
  TRuntimeConfig extends object = object,
  TTypeConfig extends object = object,
> extends Column<T, TRuntimeConfig, TTypeConfig> {
  static override readonly [entityKind]: string = 'DatabricksColumn';

  constructor(override readonly table: Table, config: any) {
    if (!config.uniqueName) {
      config.uniqueName = uniqueKeyName(table, [config.name]);
    }
    super(table, config);
  }
}

export type AnyDatabricksColumn = DatabricksColumn<ColumnBaseConfig<ColumnDataType, string>>;
