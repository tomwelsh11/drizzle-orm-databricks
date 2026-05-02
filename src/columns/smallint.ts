import type { ColumnBaseConfig } from 'drizzle-orm/column';
import type { ColumnBuilderBaseConfig } from 'drizzle-orm/column-builder';
import { entityKind } from 'drizzle-orm/entity';
import type { Table } from 'drizzle-orm/table';
import { DatabricksColumn, DatabricksColumnBuilder } from './common';

export class DatabricksSmallIntBuilder extends DatabricksColumnBuilder<
  ColumnBuilderBaseConfig<'number', 'DatabricksSmallInt'>
> {
  static override readonly [entityKind] = 'DatabricksSmallIntBuilder';

  constructor(name: string) {
    super(name, 'number', 'DatabricksSmallInt');
  }

  override build(table: Table): DatabricksSmallInt {
    return new DatabricksSmallInt(table, this.config as any);
  }
}

export class DatabricksSmallInt extends DatabricksColumn<
  ColumnBaseConfig<'number', 'DatabricksSmallInt'>
> {
  static override readonly [entityKind] = 'DatabricksSmallInt';

  getSQLType(): string {
    return 'SMALLINT';
  }
}

export function smallint(): DatabricksSmallIntBuilder;
export function smallint(name: string): DatabricksSmallIntBuilder;
export function smallint(name?: string): DatabricksSmallIntBuilder {
  return new DatabricksSmallIntBuilder(name ?? '');
}
