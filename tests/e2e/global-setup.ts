import { sql } from 'drizzle-orm';
import { drizzle } from '../../src/driver';
import type { DatabricksConnectionConfig } from '../../src/types';

function getBootstrapConfig(): DatabricksConnectionConfig | undefined {
  const host = process.env['DATABRICKS_HOST'];
  const path = process.env['DATABRICKS_SQL_PATH'];
  const token = process.env['DATABRICKS_TOKEN'];
  const clientId = process.env['DATABRICKS_CLIENT_ID'];
  const clientSecret = process.env['DATABRICKS_CLIENT_SECRET'];

  if (!host || !path) return undefined;
  if (token) return { host, path, token };
  if (clientId && clientSecret) return { host, path, clientId, clientSecret };
  return undefined;
}

export async function setup(): Promise<void> {
  const config = getBootstrapConfig();
  if (!config) return;

  const catalog = process.env['DATABRICKS_CATALOG'];
  const schema = process.env['DATABRICKS_SCHEMA'];
  if (!schema) return;

  const db = drizzle(config);
  try {
    if (catalog) {
      await db.execute(sql`CREATE SCHEMA IF NOT EXISTS ${sql.identifier(catalog)}.${sql.identifier(schema)}`);
    } else {
      await db.execute(sql`CREATE SCHEMA IF NOT EXISTS ${sql.identifier(schema)}`);
    }
  } catch {
    // Schema may already exist or the principal may lack CREATE SCHEMA — not fatal.
    // Tests will fail with a clear error if the schema is truly missing.
  } finally {
    await db.$close();
  }
}
