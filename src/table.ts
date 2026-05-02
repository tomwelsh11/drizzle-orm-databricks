import type { BuildColumns } from "drizzle-orm/column-builder";
import { entityKind } from "drizzle-orm/entity";
import { Table, type TableConfig } from "drizzle-orm/table";
import type { DatabricksColumn, DatabricksColumnBuilder } from "./columns/common";

const ColumnsSymbol = Symbol.for("drizzle:Columns");
const ExtraConfigColumnsSymbol = Symbol.for("drizzle:ExtraConfigColumns");

export type DatabricksTableConfig = TableConfig<DatabricksColumn>;

export class DatabricksTable<T extends TableConfig = TableConfig> extends Table<T> {
  static override readonly [entityKind]: string = "DatabricksTable";

  declare protected $columns: T["columns"];
}

export type DatabricksTableWithColumns<T extends TableConfig> = DatabricksTable<T> & {
  [Key in keyof T["columns"]]: T["columns"][Key];
};

export function databricksTable<
  TTableName extends string,
  TColumnsMap extends Record<string, DatabricksColumnBuilder>,
>(
  name: TTableName,
  columns: TColumnsMap,
): DatabricksTableWithColumns<{
  name: TTableName;
  schema: undefined;
  columns: BuildColumns<TTableName, TColumnsMap, "common">;
  dialect: "common";
}> {
  const rawTable = new DatabricksTable(name, undefined, name);
  const builtColumns = Object.fromEntries(
    Object.entries(columns).map(([key, colBuilder]) => {
      (colBuilder as any).setName(key);
      const column = colBuilder.build(rawTable);
      return [key, column];
    }),
  );
  const table = Object.assign(rawTable, builtColumns);
  (table as any)[ColumnsSymbol] = builtColumns;
  (table as any)[ExtraConfigColumnsSymbol] = builtColumns;
  return table as any;
}

export interface DatabricksSchema<TSchemaName extends string> {
  schemaName: TSchemaName;
  table: <TTableName extends string, TColumnsMap extends Record<string, DatabricksColumnBuilder>>(
    name: TTableName,
    columns: TColumnsMap,
  ) => DatabricksTableWithColumns<{
    name: TTableName;
    schema: TSchemaName;
    columns: BuildColumns<TTableName, TColumnsMap, "common">;
    dialect: "common";
  }>;
}

export function databricksSchema<TSchemaName extends string>(
  schemaName: TSchemaName,
): DatabricksSchema<TSchemaName> {
  return {
    schemaName,
    table: (name, columns) => {
      const rawTable = new DatabricksTable(name, schemaName, name);
      const builtColumns = Object.fromEntries(
        Object.entries(columns).map(([key, colBuilder]) => {
          (colBuilder as any).setName(key);
          const column = colBuilder.build(rawTable);
          return [key, column];
        }),
      );
      const table = Object.assign(rawTable, builtColumns);
      (table as any)[ColumnsSymbol] = builtColumns;
      (table as any)[ExtraConfigColumnsSymbol] = builtColumns;
      return table as any;
    },
  };
}
