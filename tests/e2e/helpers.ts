import { sql } from 'drizzle-orm';
import { drizzle, type DatabricksDatabase } from '../../src/driver';

const env = {
  host: process.env['DATABRICKS_HOST'],
  path: process.env['DATABRICKS_SQL_PATH'],
  token: process.env['DATABRICKS_TOKEN'],
  catalog: process.env['DATABRICKS_CATALOG'],
  schema: process.env['DATABRICKS_SCHEMA'],
};

export function hasCredentials(): boolean {
  return !!(env.host && env.path && env.token);
}

const suffix = Math.random().toString(36).slice(2, 8);

export function uniqueName(base: string): string {
  return `drizzle_e2e_${base}_${suffix}`;
}

let cachedDb: DatabricksDatabase | undefined;

export function getDb(): DatabricksDatabase {
  if (cachedDb) return cachedDb;
  if (!hasCredentials()) {
    throw new Error('Missing DATABRICKS_HOST / DATABRICKS_SQL_PATH / DATABRICKS_TOKEN');
  }
  cachedDb = drizzle({
    host: env.host!,
    path: env.path!,
    token: env.token!,
    catalog: env.catalog,
    schema: env.schema,
  });
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
