import type { DBSQLClient } from "@databricks/sql";

export interface DatabricksTokenConnectionConfig {
  host: string;
  path: string;
  token: string;
  catalog?: string;
  schema?: string;
}

export interface DatabricksOAuthConnectionConfig {
  host: string;
  path: string;
  clientId: string;
  clientSecret: string;
  catalog?: string;
  schema?: string;
}

export type DatabricksConnectionConfig =
  | DatabricksTokenConnectionConfig
  | DatabricksOAuthConnectionConfig;

export interface DatabricksClientConfig {
  client: DBSQLClient;
  catalog?: string;
  schema?: string;
}

export type DatabricksConfig = DatabricksConnectionConfig | DatabricksClientConfig;

export function isClientConfig(config: DatabricksConfig): config is DatabricksClientConfig {
  return "client" in config;
}

export function isOAuthConfig(
  config: DatabricksConnectionConfig,
): config is DatabricksOAuthConnectionConfig {
  return "clientId" in config;
}
