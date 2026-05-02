import { Column } from 'drizzle-orm/column';
import { CasingCache } from 'drizzle-orm/casing';
import { entityKind, is } from 'drizzle-orm/entity';
import type { QueryWithTypings } from 'drizzle-orm/sql';
import { Param, SQL, sql, View } from 'drizzle-orm/sql';
import { Subquery } from 'drizzle-orm/subquery';
import { getTableName, Table } from 'drizzle-orm/table';
import type { Casing } from 'drizzle-orm/utils';
import { ViewBaseConfig } from 'drizzle-orm/view-common';

import { DatabricksColumn } from './columns/common';
import { DatabricksTable } from './table';

// Drizzle internal symbols not typed for external use
const TableSymbol = (Table as any).Symbol as {
  Name: symbol;
  Schema: symbol;
  OriginalName: symbol;
  Columns: symbol;
  BaseName: symbol;
  IsAlias: symbol;
};

export interface DatabricksDialectConfig {
  casing?: Casing;
}

type FieldMapping = {
  path: string[];
  field: Column | SQL | SQL.Aliased;
}[];

export class DatabricksDialect {
  static readonly [entityKind]: string = 'DatabricksDialect';

  /** @internal */
  casing: CasingCache;

  constructor(config?: DatabricksDialectConfig) {
    this.casing = new CasingCache(config?.casing);
  }

  escapeName(name: string): string {
    return `\`${name.replace(/`/g, '``')}\``;
  }

  escapeParam(_num: number, _value: unknown): string {
    return '?';
  }

  escapeString(str: string): string {
    return `'${str.replace(/'/g, "''")}'`;
  }

  sqlToQuery(sql: SQL, invokeSource?: 'indexes'): QueryWithTypings {
    return sql.toQuery({
      casing: this.casing,
      escapeName: this.escapeName,
      escapeParam: this.escapeParam,
      escapeString: this.escapeString,
      invokeSource,
    });
  }

  buildWithCTE(queries: Subquery[] | undefined): SQL | undefined {
    if (!queries?.length) return undefined;
    const withSqlChunks: SQL[] = [sql`with `];
    for (const [i, w] of queries.entries()) {
      withSqlChunks.push(sql`${sql.identifier((w as any)._.alias)} as (${(w as any)._.sql})`);
      if (i < queries.length - 1) {
        withSqlChunks.push(sql`, `);
      }
    }
    withSqlChunks.push(sql` `);
    return sql.join(withSqlChunks);
  }

  buildSelection(
    fields: FieldMapping,
    { isSingleTable = false }: { isSingleTable?: boolean } = {},
  ): SQL {
    const columnsLen = fields.length;
    const chunks: unknown[] = fields.flatMap(({ field }, i) => {
      const chunk: unknown[] = [];

      if (is(field, SQL.Aliased) && (field as any).isSelectionField) {
        chunk.push(sql.identifier(field.fieldAlias));
      } else if (is(field, SQL.Aliased) || is(field, SQL)) {
        const query: SQL = is(field, SQL.Aliased) ? field.sql : field;
        if (isSingleTable) {
          chunk.push(
            new SQL(
              (query.queryChunks as unknown[]).map((c: unknown) => {
                if (is(c, DatabricksColumn)) {
                  return sql.identifier(this.casing.getColumnCasing(c));
                }
                return c;
              }) as any,
            ),
          );
        } else {
          chunk.push(query);
        }
        if (is(field, SQL.Aliased)) {
          chunk.push(sql` as ${sql.identifier(field.fieldAlias)}`);
        }
      } else if (is(field, Column)) {
        if (isSingleTable) {
          chunk.push(sql.identifier(this.casing.getColumnCasing(field)));
        } else {
          chunk.push(field as unknown as SQL);
        }
      }

      if (i < columnsLen - 1) {
        chunk.push(sql`, `);
      }
      return chunk;
    });
    return sql.join(chunks as SQL[]);
  }

  buildLimit(limit: number | SQL | undefined): SQL | undefined {
    return typeof limit === 'object' || (typeof limit === 'number' && limit >= 0)
      ? sql` limit ${limit}`
      : undefined;
  }

  buildOrderBy(orderBy: (SQL | Column)[] | undefined): SQL | undefined {
    return orderBy && orderBy.length > 0
      ? sql` order by ${sql.join(orderBy, sql`, `)}`
      : undefined;
  }

  buildUpdateSet(
    table: Table,
    set: Record<string, unknown>,
  ): SQL {
    const tableColumns: Record<string, Column> = (table as any)[TableSymbol.Columns];
    const columnNames = Object.keys(tableColumns).filter(
      (colName) => set[colName] !== undefined || (tableColumns[colName] as any)?.onUpdateFn !== undefined,
    );
    const setSize = columnNames.length;
    return sql.join(
      columnNames.flatMap((colName, i) => {
        const col = tableColumns[colName]!;
        const value = set[colName] ?? sql.param((col as any).onUpdateFn(), col);
        const res = sql`${sql.identifier(this.casing.getColumnCasing(col))} = ${value}`;
        if (i < setSize - 1) {
          return [res, sql.raw(', ')];
        }
        return [res];
      }),
    );
  }

  buildSelectQuery({
    withList,
    fields,
    fieldsFlat,
    where,
    having,
    table,
    joins,
    orderBy,
    groupBy,
    limit,
    offset,
    distinct,
    setOperators,
  }: {
    withList?: Subquery[];
    fields: Record<string, unknown>;
    fieldsFlat?: FieldMapping;
    where?: SQL;
    having?: SQL;
    table: Table | Subquery | SQL | View;
    joins?: Array<{ on: SQL; table: Table | Subquery | SQL | View; joinType: string; alias?: string; lateral?: boolean }>;
    orderBy?: (SQL | Column)[];
    groupBy?: (SQL | Column)[];
    limit?: number | SQL;
    offset?: number | SQL;
    distinct?: boolean;
    setOperators: Array<{ type: string; isAll: boolean; rightSelect: any; limit?: number | SQL; orderBy?: (SQL | Column)[]; offset?: number | SQL }>;
  }): SQL {
    const { orderSelectedFields } = require('drizzle-orm/utils') as { orderSelectedFields: (fields: any) => FieldMapping };
    const fieldsList: FieldMapping = fieldsFlat ?? orderSelectedFields(fields);

    for (const f of fieldsList) {
      if (
        is(f.field, Column) &&
        getTableName(f.field.table) !==
          (is(table, Subquery)
            ? (table as any)._.alias
            : is(table, View)
              ? (table as any)[ViewBaseConfig].name
              : is(table, SQL)
                ? undefined
                : getTableName(table as Table)) &&
        !((table2: Table) =>
          joins?.some(
            ({ alias }) =>
              alias ===
              ((table2 as any)[TableSymbol.IsAlias]
                ? getTableName(table2)
                : (table2 as any)[TableSymbol.BaseName]),
          ))(f.field.table)
      ) {
        const tableName = getTableName(f.field.table);
        throw new Error(
          `Your "${f.path.join('->') }" field references a column "${tableName}"."${f.field.name}", but the table "${tableName}" is not part of the query! Did you forget to join it?`,
        );
      }
    }

    const isSingleTable = !joins || joins.length === 0;

    const withSql = this.buildWithCTE(withList);
    const distinctSql = distinct ? sql` distinct` : undefined;
    const selection = this.buildSelection(fieldsList, { isSingleTable });

    const tableSql = (() => {
      if (
        is(table, Table) &&
        (table as any)[TableSymbol.OriginalName] !== (table as any)[TableSymbol.Name]
      ) {
        return sql`${sql.identifier((table as any)[TableSymbol.OriginalName])} ${sql.identifier((table as any)[TableSymbol.Name])}`;
      }
      return table as unknown as SQL;
    })();

    const joinsArray: SQL[] = [];
    if (joins) {
      for (const [index, joinMeta] of joins.entries()) {
        if (index === 0) {
          joinsArray.push(sql` `);
        }
        const table2 = joinMeta.table;
        const lateralSql = joinMeta.lateral ? sql` lateral` : undefined;

        if (is(table2, Table)) {
          const tableName = (table2 as any)[TableSymbol.Name] as string;
          const tableSchema = (table2 as any)[TableSymbol.Schema] as string | undefined;
          const origTableName = (table2 as any)[TableSymbol.OriginalName] as string;
          const alias = tableName === origTableName ? undefined : joinMeta.alias;
          joinsArray.push(
            sql`${sql.raw(joinMeta.joinType)} join${lateralSql} ${tableSchema ? sql`${sql.identifier(tableSchema)}.` : undefined}${sql.identifier(origTableName)}${alias && sql` ${sql.identifier(alias)}`} on ${joinMeta.on}`,
          );
        } else if (is(table2, View)) {
          const viewName = (table2 as any)[ViewBaseConfig].name as string;
          const viewSchema = (table2 as any)[ViewBaseConfig].schema as string | undefined;
          const origViewName = (table2 as any)[ViewBaseConfig].originalName as string;
          const alias = viewName === origViewName ? undefined : joinMeta.alias;
          joinsArray.push(
            sql`${sql.raw(joinMeta.joinType)} join${lateralSql} ${viewSchema ? sql`${sql.identifier(viewSchema)}.` : undefined}${sql.identifier(origViewName)}${alias && sql` ${sql.identifier(alias)}`} on ${joinMeta.on}`,
          );
        } else {
          joinsArray.push(
            sql`${sql.raw(joinMeta.joinType)} join${lateralSql} ${table2 as unknown as SQL} on ${joinMeta.on}`,
          );
        }

        if (index < joins.length - 1) {
          joinsArray.push(sql` `);
        }
      }
    }

    const joinsSql = sql.join(joinsArray);
    const whereSql = where ? sql` where ${where}` : undefined;
    const havingSql = having ? sql` having ${having}` : undefined;
    const orderBySql = this.buildOrderBy(orderBy);
    const groupBySql =
      groupBy && groupBy.length > 0 ? sql` group by ${sql.join(groupBy, sql`, `)}` : undefined;
    const limitSql = this.buildLimit(limit);
    const offsetSql = offset ? sql` offset ${offset}` : undefined;

    const finalQuery = sql`${withSql}select${distinctSql} ${selection} from ${tableSql}${joinsSql}${whereSql}${groupBySql}${havingSql}${orderBySql}${limitSql}${offsetSql}`;

    if (setOperators.length > 0) {
      return this.buildSetOperations(finalQuery, setOperators);
    }
    return finalQuery;
  }

  buildSetOperations(
    leftSelect: SQL,
    setOperators: Array<{ type: string; isAll: boolean; rightSelect: any; limit?: number | SQL; orderBy?: (SQL | Column)[]; offset?: number | SQL }>,
  ): SQL {
    const [setOperator, ...rest] = setOperators;
    if (!setOperator) {
      throw new Error('Cannot pass undefined values to any set operator');
    }
    if (rest.length === 0) {
      return this.buildSetOperationQuery({ leftSelect, setOperator });
    }
    return this.buildSetOperations(
      this.buildSetOperationQuery({ leftSelect, setOperator }),
      rest,
    );
  }

  buildSetOperationQuery({
    leftSelect,
    setOperator: { type, isAll, rightSelect, limit, orderBy, offset },
  }: {
    leftSelect: SQL;
    setOperator: { type: string; isAll: boolean; rightSelect: any; limit?: number | SQL; orderBy?: (SQL | Column)[]; offset?: number | SQL };
  }): SQL {
    const leftChunk = sql`(${leftSelect}) `;
    const rightChunk = sql`(${rightSelect.getSQL()})`;

    let orderBySql: SQL | undefined;
    if (orderBy && orderBy.length > 0) {
      const orderByValues: unknown[] = [];
      for (const orderByUnit of orderBy) {
        if (is(orderByUnit, DatabricksColumn)) {
          orderByValues.push(sql.identifier(this.casing.getColumnCasing(orderByUnit)));
        } else if (is(orderByUnit, SQL)) {
          for (let i = 0; i < orderByUnit.queryChunks.length; i++) {
            const chunk = orderByUnit.queryChunks[i];
            if (is(chunk, DatabricksColumn)) {
              orderByUnit.queryChunks[i] = sql.identifier(this.casing.getColumnCasing(chunk as any)) as any;
            }
          }
          orderByValues.push(sql`${orderByUnit}`);
        } else {
          orderByValues.push(sql`${orderByUnit}`);
        }
      }
      orderBySql = sql` order by ${sql.join(orderByValues as SQL[], sql`, `)} `;
    }

    const limitSql =
      typeof limit === 'object' || (typeof limit === 'number' && limit >= 0)
        ? sql` limit ${limit}`
        : undefined;
    const operatorChunk = sql.raw(`${type} ${isAll ? 'all ' : ''}`);
    const offsetSql = offset ? sql` offset ${offset}` : undefined;

    return sql`${leftChunk}${operatorChunk}${rightChunk}${orderBySql}${limitSql}${offsetSql}`;
  }

  buildInsertQuery({
    table,
    values: valuesOrSelect,
    onConflict,
    select,
  }: {
    table: Table;
    values: Record<string, SQL | Param>[] | SQL;
    onConflict?: SQL;
    select?: boolean;
  }): { sql: SQL; generatedIds: Record<string, unknown>[] } {
    const valuesSqlList: (SQL | SQL[])[] = [];
    const columns: Record<string, Column> = (table as any)[TableSymbol.Columns];
    const colEntries: [string, Column][] = Object.entries(columns).filter(
      ([, col]) => !(col as any).shouldDisableInsert(),
    );
    const insertOrder = sql.join(
      colEntries.map(([, column]) => sql.identifier(this.casing.getColumnCasing(column))),
      sql.raw(', '),
    );
    const generatedIdsResponse: Record<string, unknown>[] = [];

    if (select) {
      const select2 = valuesOrSelect;
      if (is(select2, SQL)) {
        valuesSqlList.push(select2);
      } else {
        valuesSqlList.push((select2 as any).getSQL());
      }
    } else {
      const values = valuesOrSelect as Record<string, SQL | Param>[];
      valuesSqlList.push(sql.raw('values '));
      for (const [valueIndex, value] of values.entries()) {
        const generatedIds: Record<string, unknown> = {};
        const valueList: (SQL | Param)[] = [];
        for (const [fieldName, col] of colEntries) {
          const colValue = value[fieldName];
          if (colValue === undefined || (is(colValue, Param) && colValue.value === undefined)) {
            if ((col as any).defaultFn !== undefined) {
              const defaultFnResult = (col as any).defaultFn();
              generatedIds[fieldName] = defaultFnResult;
              const defaultValue = is(defaultFnResult, SQL)
                ? defaultFnResult
                : sql.param(defaultFnResult, col);
              valueList.push(defaultValue);
            } else if (!(col as any).default && (col as any).onUpdateFn !== undefined) {
              const onUpdateFnResult = (col as any).onUpdateFn();
              const newValue = is(onUpdateFnResult, SQL)
                ? onUpdateFnResult
                : sql.param(onUpdateFnResult, col);
              valueList.push(newValue);
            } else {
              valueList.push(sql`default`);
            }
          } else {
            if ((col as any).defaultFn && is(colValue, Param)) {
              generatedIds[fieldName] = colValue.value;
            }
            valueList.push(colValue);
          }
        }
        generatedIdsResponse.push(generatedIds);
        valuesSqlList.push(valueList as unknown as SQL[]);
        if (valueIndex < values.length - 1) {
          valuesSqlList.push(sql`, `);
        }
      }
    }

    const valuesSql = sql.join(valuesSqlList);
    const onConflictSql = onConflict ? sql` on duplicate key ${onConflict}` : undefined;

    return {
      sql: sql`insert into ${table} (${insertOrder}) ${valuesSql}${onConflictSql}`,
      generatedIds: generatedIdsResponse,
    };
  }

  buildUpdateQuery({
    table,
    set,
    where,
    withList,
    limit,
    orderBy,
  }: {
    table: Table;
    set: Record<string, unknown>;
    where?: SQL;
    withList?: Subquery[];
    limit?: number | SQL;
    orderBy?: (SQL | Column)[];
  }): SQL {
    const withSql = this.buildWithCTE(withList);
    const setSql = this.buildUpdateSet(table, set);
    const whereSql = where ? sql` where ${where}` : undefined;
    const orderBySql = this.buildOrderBy(orderBy);
    const limitSql = this.buildLimit(limit);
    return sql`${withSql}update ${table} set ${setSql}${whereSql}${orderBySql}${limitSql}`;
  }

  buildDeleteQuery({
    table,
    where,
    withList,
    limit,
    orderBy,
  }: {
    table: Table;
    where?: SQL;
    withList?: Subquery[];
    limit?: number | SQL;
    orderBy?: (SQL | Column)[];
  }): SQL {
    const withSql = this.buildWithCTE(withList);
    const whereSql = where ? sql` where ${where}` : undefined;
    const orderBySql = this.buildOrderBy(orderBy);
    const limitSql = this.buildLimit(limit);
    return sql`${withSql}delete from ${table}${whereSql}${orderBySql}${limitSql}`;
  }
}
