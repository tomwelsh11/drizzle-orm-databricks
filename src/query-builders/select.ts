import { Column } from 'drizzle-orm/column';
import { entityKind, is } from 'drizzle-orm/entity';
import { TypedQueryBuilder } from 'drizzle-orm/query-builders/query-builder';
import { QueryPromise } from 'drizzle-orm/query-promise';
import { SelectionProxyHandler } from 'drizzle-orm/selection-proxy';
import { SQL, View } from 'drizzle-orm/sql';
import { Subquery } from 'drizzle-orm/subquery';
import { Table } from 'drizzle-orm/table';
import { getTableColumns, haveSameKeys } from 'drizzle-orm/utils';
import { ViewBaseConfig } from 'drizzle-orm/view-common';

import type { DatabricksDialect } from '../dialect';
import type { DatabricksSession } from '../session';
import type { DatabricksTable } from '../table';

// Runtime-only drizzle-orm utilities not in type declarations
const {
  applyMixins,
  getTableLikeName,
  orderSelectedFields,
} = require('drizzle-orm/utils') as {
  applyMixins: (baseClass: any, extendedClasses: any[]) => void;
  getTableLikeName: (table: any) => string | undefined;
  orderSelectedFields: (fields: any) => FieldMapping;
};

type FieldMapping = {
  path: string[];
  field: Column | SQL | SQL.Aliased;
}[];

const TableSymbol = (Table as any).Symbol as {
  Columns: symbol;
};

export class DatabricksSelectBuilder<
  TSelection extends Record<string, unknown> | undefined,
> {
  static readonly [entityKind]: string = 'DatabricksSelectBuilder';

  private fields: TSelection;
  private session: DatabricksSession;
  private dialect: DatabricksDialect;
  private withList: Subquery[] = [];
  private distinct: boolean | undefined;

  constructor(config: {
    fields: TSelection;
    session: DatabricksSession;
    dialect: DatabricksDialect;
    withList?: Subquery[];
    distinct?: boolean;
  }) {
    this.fields = config.fields;
    this.session = config.session;
    this.dialect = config.dialect;
    if (config.withList) {
      this.withList = config.withList;
    }
    this.distinct = config.distinct;
  }

  from(source: DatabricksTable<any> | Subquery | SQL | View): DatabricksSelectBase<any, any, any> {
    const isPartialSelect = !!this.fields;
    let fields: Record<string, unknown>;
    if (this.fields) {
      fields = this.fields as Record<string, unknown>;
    } else if (is(source, Subquery)) {
      fields = Object.fromEntries(
        Object.keys((source as any)._.selectedFields).map((key) => [key, (source as any)[key]]),
      );
    } else if (is(source, View)) {
      fields = (source as any)[ViewBaseConfig].selectedFields;
    } else if (is(source, SQL)) {
      fields = {};
    } else {
      fields = getTableColumns(source as any);
    }

    return new DatabricksSelectBase({
      table: source,
      fields,
      isPartialSelect,
      session: this.session,
      dialect: this.dialect,
      withList: this.withList,
      distinct: this.distinct,
    });
  }
}

export class DatabricksSelectQueryBuilderBase<
  TSelection extends Record<string, unknown>,
  TResult = unknown,
  TSelectHKT extends Record<string, unknown> = Record<string, unknown>,
> extends TypedQueryBuilder<TSelection, TResult> {
  static override readonly [entityKind]: string = 'DatabricksSelectQueryBuilder';

  declare _: { selectedFields: TSelection; result: TResult };
  config: any;
  joinsNotNullableMap: Record<string, boolean>;
  tableName: string | undefined;
  isPartialSelect: boolean;
  session: DatabricksSession;
  dialect: DatabricksDialect;

  constructor({
    table,
    fields,
    isPartialSelect,
    session,
    dialect,
    withList,
    distinct,
  }: {
    table: Table | Subquery | SQL | View;
    fields: Record<string, unknown>;
    isPartialSelect: boolean;
    session: DatabricksSession;
    dialect: DatabricksDialect;
    withList?: Subquery[];
    distinct?: boolean;
  }) {
    super();
    this.config = {
      withList,
      table,
      fields: { ...fields },
      distinct,
      setOperators: [],
    };
    this.isPartialSelect = isPartialSelect;
    this.session = session;
    this.dialect = dialect;
    this._ = {
      selectedFields: fields as TSelection,
      result: undefined as unknown as TResult,
    };
    this.tableName = getTableLikeName(table);
    this.joinsNotNullableMap =
      typeof this.tableName === 'string' ? { [this.tableName]: true } : {};
  }

  private createJoin(joinType: string) {
    return (table: DatabricksTable<any> | Subquery | SQL, on: SQL | ((fields: any) => SQL)) => {
      const baseTableName = this.tableName;
      const tableName = getTableLikeName(table);

      if (
        typeof tableName === 'string' &&
        this.config.joins?.some((join: any) => join.alias === tableName)
      ) {
        throw new Error(`Alias "${tableName}" is already used in this query`);
      }

      if (!this.isPartialSelect) {
        if (
          Object.keys(this.joinsNotNullableMap).length === 1 &&
          typeof baseTableName === 'string'
        ) {
          this.config.fields = {
            [baseTableName]: this.config.fields,
          };
        }
        if (typeof tableName === 'string' && !is(table, SQL)) {
          const selection = is(table, Subquery)
            ? (table as any)._.selectedFields
            : is(table, View)
              ? (table as any)[ViewBaseConfig].selectedFields
              : (table as any)[TableSymbol.Columns];
          this.config.fields[tableName] = selection;
        }
      }

      if (typeof on === 'function') {
        on = on(
          new Proxy(
            this.config.fields,
            new SelectionProxyHandler({
              sqlAliasedBehavior: 'sql',
              sqlBehavior: 'sql',
            }),
          ),
        );
      }

      if (!this.config.joins) {
        this.config.joins = [];
      }
      this.config.joins.push({ on, table, joinType, alias: tableName });

      if (typeof tableName === 'string') {
        switch (joinType) {
          case 'left':
            this.joinsNotNullableMap[tableName] = false;
            break;
          case 'right':
            this.joinsNotNullableMap = Object.fromEntries(
              Object.entries(this.joinsNotNullableMap).map(([key]) => [key, false]),
            );
            this.joinsNotNullableMap[tableName] = true;
            break;
          case 'inner':
            this.joinsNotNullableMap[tableName] = true;
            break;
          case 'full':
            this.joinsNotNullableMap = Object.fromEntries(
              Object.entries(this.joinsNotNullableMap).map(([key]) => [key, false]),
            );
            this.joinsNotNullableMap[tableName] = false;
            break;
        }
      }
      return this;
    };
  }

  leftJoin = this.createJoin('left');
  rightJoin = this.createJoin('right');
  innerJoin = this.createJoin('inner');
  fullJoin = this.createJoin('full');

  private createSetOperator(type: string, isAll: boolean) {
    return (rightSelection: any) => {
      const rightSelect =
        typeof rightSelection === 'function'
          ? rightSelection(getDatabricksSetOperators())
          : rightSelection;
      if (!haveSameKeys(this.getSelectedFields(), rightSelect.getSelectedFields())) {
        throw new Error(
          'Set operator error (union / intersect / except): selected fields are not the same or are in a different order',
        );
      }
      this.config.setOperators.push({ type, isAll, rightSelect });
      return this;
    };
  }

  union = this.createSetOperator('union', false);
  unionAll = this.createSetOperator('union', true);
  intersect = this.createSetOperator('intersect', false);
  intersectAll = this.createSetOperator('intersect', true);
  except = this.createSetOperator('except', false);
  exceptAll = this.createSetOperator('except', true);

  /** @internal */
  addSetOperators(setOperators: any[]) {
    this.config.setOperators.push(...setOperators);
    return this;
  }

  where(where: SQL | ((fields: any) => SQL | undefined) | undefined) {
    if (typeof where === 'function') {
      where = where(
        new Proxy(this.config.fields, new SelectionProxyHandler({ sqlAliasedBehavior: 'sql', sqlBehavior: 'sql' })),
      );
    }
    this.config.where = where;
    return this;
  }

  having(having: SQL | ((fields: any) => SQL | undefined) | undefined) {
    if (typeof having === 'function') {
      having = having(
        new Proxy(this.config.fields, new SelectionProxyHandler({ sqlAliasedBehavior: 'sql', sqlBehavior: 'sql' })),
      );
    }
    this.config.having = having;
    return this;
  }

  groupBy(...columns: (SQL | Column)[] | [((fields: any) => (SQL | Column)[] | SQL | Column)]) {
    if (typeof columns[0] === 'function') {
      const groupBy = (columns[0] as Function)(
        new Proxy(this.config.fields, new SelectionProxyHandler({ sqlAliasedBehavior: 'alias', sqlBehavior: 'sql' })),
      );
      this.config.groupBy = Array.isArray(groupBy) ? groupBy : [groupBy];
    } else {
      this.config.groupBy = columns;
    }
    return this;
  }

  orderBy(...columns: (SQL | Column)[] | [((fields: any) => (SQL | Column)[] | SQL | Column)]) {
    if (typeof columns[0] === 'function') {
      const orderBy = (columns[0] as Function)(
        new Proxy(this.config.fields, new SelectionProxyHandler({ sqlAliasedBehavior: 'alias', sqlBehavior: 'sql' })),
      );
      const orderByArray = Array.isArray(orderBy) ? orderBy : [orderBy];
      if (this.config.setOperators.length > 0) {
        this.config.setOperators.at(-1).orderBy = orderByArray;
      } else {
        this.config.orderBy = orderByArray;
      }
    } else {
      const orderByArray = columns;
      if (this.config.setOperators.length > 0) {
        this.config.setOperators.at(-1).orderBy = orderByArray;
      } else {
        this.config.orderBy = orderByArray;
      }
    }
    return this;
  }

  limit(limit: number | SQL) {
    if (this.config.setOperators.length > 0) {
      this.config.setOperators.at(-1).limit = limit;
    } else {
      this.config.limit = limit;
    }
    return this;
  }

  offset(offset: number | SQL) {
    if (this.config.setOperators.length > 0) {
      this.config.setOperators.at(-1).offset = offset;
    } else {
      this.config.offset = offset;
    }
    return this;
  }

  /** @internal */
  getSQL(): SQL {
    return this.dialect.buildSelectQuery(this.config);
  }

  toSQL() {
    const { typings: _typings, ...rest } = this.dialect.sqlToQuery(this.getSQL());
    return rest;
  }

  as(alias: string): Subquery {
    return new Proxy(
      new Subquery(this.getSQL(), this.config.fields, alias),
      new SelectionProxyHandler({ alias, sqlAliasedBehavior: 'alias', sqlBehavior: 'error' }),
    ) as unknown as Subquery;
  }

  /** @internal */
  getSelectedFields() {
    return new Proxy(
      this.config.fields,
      new SelectionProxyHandler({
        alias: this.tableName,
        sqlAliasedBehavior: 'alias',
        sqlBehavior: 'error',
      }),
    ) as any;
  }

  $dynamic() {
    return this;
  }
}

export interface DatabricksSelectBase<
  TSelection extends Record<string, unknown>,
  TResult,
  TSelectHKT extends Record<string, unknown>,
> extends DatabricksSelectQueryBuilderBase<TSelection, TResult, TSelectHKT>,
    QueryPromise<TResult> {}

export class DatabricksSelectBase<
  TSelection extends Record<string, unknown>,
  TResult = unknown,
  TSelectHKT extends Record<string, unknown> = Record<string, unknown>,
> extends DatabricksSelectQueryBuilderBase<TSelection, TResult, TSelectHKT> {
  static override readonly [entityKind]: string = 'DatabricksSelect';

  prepare() {
    if (!this.session) {
      throw new Error(
        'Cannot execute a query on a query builder. Please use a database instance instead.',
      );
    }
    const fieldsList: FieldMapping = orderSelectedFields(this.config.fields);
    const isSingleTable = !this.config.joins || this.config.joins.length === 0;

    const customResultMapper = (rows: unknown[]) => {
      return (rows as Record<string, unknown>[]).map((row) =>
        mapObjectResultRow(fieldsList, row, this.joinsNotNullableMap, isSingleTable, this.dialect),
      );
    };

    const query = this.session.prepareQuery(
      this.dialect.sqlToQuery(this.getSQL()),
      undefined,
      customResultMapper as (rows: unknown[][]) => unknown,
    );
    (query as any).joinsNotNullableMap = this.joinsNotNullableMap;
    return query;
  }

  execute = (placeholderValues?: Record<string, unknown>): Promise<TResult> => {
    return this.prepare().execute(placeholderValues) as Promise<TResult>;
  };

  createIterator = () => {
    const self = this;
    return async function* (placeholderValues?: Record<string, unknown>) {
      yield* self.prepare().iterator(placeholderValues);
    };
  };

  iterator = this.createIterator();
}

applyMixins(DatabricksSelectBase, [QueryPromise]);

function mapObjectResultRow(
  columns: FieldMapping,
  row: Record<string, unknown>,
  joinsNotNullableMap: Record<string, boolean> | undefined,
  isSingleTable: boolean,
  dialect: DatabricksDialect,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const { path, field } of columns) {
    let decoder: { mapFromDriverValue(value: unknown): unknown };
    let key: string;

    if (is(field, Column)) {
      decoder = field;
      key = isSingleTable
        ? dialect.casing.getColumnCasing(field)
        : field.name;
    } else if (is(field, SQL.Aliased)) {
      decoder = (field as any).sql?.decoder ?? { mapFromDriverValue: (v: unknown) => v };
      key = field.fieldAlias;
    } else {
      decoder = (field as any).decoder ?? { mapFromDriverValue: (v: unknown) => v };
      key = path[path.length - 1]!;
    }

    let node: Record<string, unknown> = result;
    for (const [pathChunkIndex, pathChunk] of path.entries()) {
      if (pathChunkIndex < path.length - 1) {
        if (!(pathChunk in node)) {
          node[pathChunk] = {};
        }
        node = node[pathChunk] as Record<string, unknown>;
      } else {
        const rawValue = row[key];
        node[pathChunk] = rawValue === null || rawValue === undefined
          ? null
          : decoder.mapFromDriverValue(rawValue);
      }
    }
  }

  if (joinsNotNullableMap) {
    for (const [key, isNotNullable] of Object.entries(joinsNotNullableMap)) {
      if (!isNotNullable) {
        const nested = result[key] as Record<string, unknown> | null;
        if (nested && typeof nested === 'object' && Object.values(nested).every((v) => v === null)) {
          result[key] = null;
        }
      }
    }
  }

  return result;
}

function createSetOperator(type: string, isAll: boolean) {
  return (leftSelect: any, rightSelect: any, ...restSelects: any[]) => {
    const setOperators = [rightSelect, ...restSelects].map((select) => ({
      type,
      isAll,
      rightSelect: select,
    }));
    for (const setOperator of setOperators) {
      if (!haveSameKeys(leftSelect.getSelectedFields(), setOperator.rightSelect.getSelectedFields())) {
        throw new Error(
          'Set operator error (union / intersect / except): selected fields are not the same or are in a different order',
        );
      }
    }
    return leftSelect.addSetOperators(setOperators);
  };
}

const getDatabricksSetOperators = () => ({
  union,
  unionAll,
  intersect,
  intersectAll,
  except,
  exceptAll,
});

export const union = createSetOperator('union', false);
export const unionAll = createSetOperator('union', true);
export const intersect = createSetOperator('intersect', false);
export const intersectAll = createSetOperator('intersect', true);
export const except = createSetOperator('except', false);
export const exceptAll = createSetOperator('except', true);
