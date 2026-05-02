import { CasingCache } from 'drizzle-orm/casing';
import { entityKind } from 'drizzle-orm/entity';
import type { QueryWithTypings } from 'drizzle-orm/sql';
import { SQL } from 'drizzle-orm/sql';
import type { Casing } from 'drizzle-orm/utils';

export interface DatabricksDialectConfig {
  casing?: Casing;
}

export class DatabricksDialect {
  static readonly [entityKind]: string = 'DatabricksDialect';

  private casing: CasingCache;

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
}
