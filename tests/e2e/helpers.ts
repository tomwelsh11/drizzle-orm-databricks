import { sql } from 'drizzle-orm';
import { drizzle, type DatabricksDatabase } from '../../src/driver';

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

const suffix = Math.random().toString(36).slice(2, 8);

export function uniqueName(base: string): string {
  return `drizzle_e2e_${base}_${suffix}`;
}

let cachedDb: DatabricksDatabase | undefined;

export function getDb(): DatabricksDatabase {
  if (cachedDb) return cachedDb;
  if (!hasCredentials()) {
    throw new Error('Missing DATABRICKS_HOST / DATABRICKS_SQL_PATH and either DATABRICKS_TOKEN or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET');
  }
  const base = { host: env.host!, path: env.path!, catalog: env.catalog, schema: env.schema };
  cachedDb = env.token
    ? drizzle({ ...base, token: env.token })
    : drizzle({ ...base, clientId: env.clientId!, clientSecret: env.clientSecret! });
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
