export class DatabricksUnsupportedError extends Error {
  constructor(feature: string, alternative?: string) {
    const tail = alternative ? ` ${alternative}` : "";
    super(`${feature} is not supported by the Databricks adapter for Drizzle ORM.${tail}`);
    this.name = "DatabricksUnsupportedError";
  }
}

export class DatabricksConnectionError extends Error {
  override readonly cause?: unknown;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = "DatabricksConnectionError";
    this.cause = cause;
  }
}
