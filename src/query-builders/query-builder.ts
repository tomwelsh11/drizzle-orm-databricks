import { entityKind } from "drizzle-orm/entity";
import { SelectionProxyHandler } from "drizzle-orm/selection-proxy";
import { Subquery, WithSubquery } from "drizzle-orm/subquery";
import type { SQLWrapper } from "drizzle-orm/sql";

import { DatabricksDialect } from "../dialect";
import { DatabricksSelectBuilder } from "./select";

export class DatabricksQueryBuilder {
  static { (this as any)[entityKind] = "DatabricksQueryBuilder"; }

  private dialect: DatabricksDialect;

  constructor(dialect?: DatabricksDialect) {
    this.dialect = dialect ?? new DatabricksDialect();
  }

  $with = <TAlias extends string>(alias: TAlias, selection?: Record<string, unknown>) => {
    const queryBuilder = this;
    const as = (
      qb:
        | ((qb: DatabricksQueryBuilder) => SQLWrapper & { getSelectedFields?: () => any })
        | (SQLWrapper & { getSelectedFields?: () => any }),
    ) => {
      const resolved = typeof qb === "function" ? qb(queryBuilder) : qb;
      return new Proxy(
        new WithSubquery(
          resolved.getSQL(),
          selection ??
            ("getSelectedFields" in resolved && typeof resolved.getSelectedFields === "function"
              ? (resolved.getSelectedFields() ?? {})
              : {}),
          alias,
          true,
        ),
        new SelectionProxyHandler({ alias, sqlAliasedBehavior: "alias", sqlBehavior: "error" }),
      ) as unknown as Subquery;
    };
    return { as };
  };

  with(...queries: Subquery[]) {
    const self = this;
    function select(): DatabricksSelectBuilder<undefined>;
    function select<TSelection extends Record<string, unknown>>(
      fields: TSelection,
    ): DatabricksSelectBuilder<TSelection>;
    function select(fields?: Record<string, unknown>) {
      return new DatabricksSelectBuilder({
        fields: fields ?? undefined,
        session: undefined as never,
        dialect: self.dialect,
        withList: queries,
      });
    }
    function selectDistinct(): DatabricksSelectBuilder<undefined>;
    function selectDistinct<TSelection extends Record<string, unknown>>(
      fields: TSelection,
    ): DatabricksSelectBuilder<TSelection>;
    function selectDistinct(fields?: Record<string, unknown>) {
      return new DatabricksSelectBuilder({
        fields: fields ?? undefined,
        session: undefined as never,
        dialect: self.dialect,
        withList: queries,
        distinct: true,
      });
    }
    return { select, selectDistinct };
  }

  select(): DatabricksSelectBuilder<undefined>;
  select<TSelection extends Record<string, unknown>>(
    fields: TSelection,
  ): DatabricksSelectBuilder<TSelection>;
  select(fields?: Record<string, unknown>) {
    return new DatabricksSelectBuilder({
      fields: fields ?? undefined,
      session: undefined as never,
      dialect: this.dialect,
    });
  }

  selectDistinct(): DatabricksSelectBuilder<undefined>;
  selectDistinct<TSelection extends Record<string, unknown>>(
    fields: TSelection,
  ): DatabricksSelectBuilder<TSelection>;
  selectDistinct(fields?: Record<string, unknown>) {
    return new DatabricksSelectBuilder({
      fields: fields ?? undefined,
      session: undefined as never,
      dialect: this.dialect,
      distinct: true,
    });
  }
}
