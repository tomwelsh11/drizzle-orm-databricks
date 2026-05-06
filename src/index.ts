export { drizzle, DatabricksDatabase } from "./driver";
export type { DatabricksDriverOptions } from "./driver";
export { migrate } from "./migrator";
export { DatabricksUnsupportedError, DatabricksConnectionError } from "./errors";
export { DatabricksDialect } from "./dialect";
export type { IdentifierQuoteStyle } from "./dialect";
export { DatabricksSession, DatabricksPreparedQuery } from "./session";
export type { SessionExecutor } from "./session";
export { Pool, PoolError, DEFAULT_POOL_MAX_SIZE, DEFAULT_POOL_ACQUIRE_TIMEOUT_MS } from "./pool";
export type { PoolHooks, PoolOptions } from "./pool";
export { SessionPool, DEFAULT_SESSION_MAX_AGE_MS } from "./session-pool";
export type { SessionPoolOptions } from "./session-pool";
export type {
  DatabricksConfig,
  DatabricksConnectionConfig,
  DatabricksTokenConnectionConfig,
  DatabricksOAuthConnectionConfig,
  DatabricksClientConfig,
} from "./types";

export { databricksTable, databricksSchema, databricksCatalog, DatabricksTable } from "./table";
export type { DatabricksCatalog, NamespaceOverride } from "./table";

export {
  DatabricksSelectBuilder,
  DatabricksSelectBase,
  DatabricksInsertBuilder,
  DatabricksInsertBase,
  DatabricksUpdateBuilder,
  DatabricksUpdateBase,
  DatabricksDeleteBase,
  DatabricksQueryBuilder,
  union,
  unionAll,
  intersect,
  intersectAll,
  except,
  exceptAll,
} from "./query-builders";

export * from "./columns";
