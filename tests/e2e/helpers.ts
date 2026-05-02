import { sql } from 'drizzle-orm';
import { drizzle, type DatabricksDatabase } from '../../src/driver';
import type { DatabricksConnectionConfig } from '../../src/types';

const env = {
  host: process.env['DATABRICKS_HOST'],
  path: process.env['DATABRICKS_SQL_PATH'],
  token: process.env['DATABRICKS_TOKEN'],
  clientId: process.env['DATABRICKS_CLIENT_ID'],
  clientSecret: process.env['DATABRICKS_CLIENT_SECRET'],
  catalog: process.env['DATABRICKS_CATALOG'],
  schema: process.env['DATABRICKS_SCHEMA'],
};

export function hasCredentials(): boolean {
  return !!(env.host && env.path && (env.token || (env.clientId && env.clientSecret)));
}

export function getConnectionConfig(): DatabricksConnectionConfig {
  if (!hasCredentials()) {
    throw new Error('Missing DATABRICKS_HOST / DATABRICKS_SQL_PATH and either DATABRICKS_TOKEN or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET');
  }
  if (env.token) {
    return { host: env.host!, path: env.path!, token: env.token, catalog: env.catalog, schema: env.schema };
  }
  return { host: env.host!, path: env.path!, clientId: env.clientId!, clientSecret: env.clientSecret!, catalog: env.catalog, schema: env.schema };
}

let cachedDb: DatabricksDatabase | undefined;

export function getDb(): DatabricksDatabase {
  if (cachedDb) return cachedDb;
  cachedDb = drizzle(getConnectionConfig());
  return cachedDb;
}

export async function closeDb(): Promise<void> {
  if (cachedDb) {
    await cachedDb.$close();
    cachedDb = undefined;
  }
}

export async function dropTable(db: DatabricksDatabase, name: string): Promise<void> {
  await db.execute(sql.raw(`DROP TABLE IF EXISTS ${backtick(name)}`));
}

function backtick(name: string): string {
  return '`' + name.replace(/`/g, '``') + '`';
}
