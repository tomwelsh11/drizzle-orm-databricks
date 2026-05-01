import type { DBSQLClient } from '@databricks/sql';

export interface DatabricksConnectionConfig {
  host: string;
  path: string;
  token: string;
  catalog?: string;
  schema?: string;
}

export interface DatabricksClientConfig {
  client: DBSQLClient;
  catalog?: string;
  schema?: string;
}

export type DatabricksConfig = DatabricksConnectionConfig | DatabricksClientConfig;

export function isClientConfig(config: DatabricksConfig): config is DatabricksClientConfig {
  return (config as DatabricksClientConfig).client !== undefined;
}
