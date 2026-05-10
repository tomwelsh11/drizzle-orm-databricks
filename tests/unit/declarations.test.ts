import { describe, expect, it, beforeAll } from "vitest";
import { execSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

const DIST_DIR = join(import.meta.dirname, "../../dist");

describe("Declaration file output", () => {
  beforeAll(() => {
    execSync("npm run build", { cwd: join(import.meta.dirname, "../.."), stdio: "pipe" });
  });

  it("does not expose entityKind in any declaration file", () => {
    const dtsFiles = readdirSync(DIST_DIR).filter(
      (f) => f.endsWith(".d.mts") || f.endsWith(".d.cts"),
    );

    expect(dtsFiles.length).toBeGreaterThan(0);

    for (const file of dtsFiles) {
      const content = readFileSync(join(DIST_DIR, file), "utf-8");
      expect(content, `${file} should not reference entityKind`).not.toContain("entityKind");
    }
  });

  it("does not import from drizzle-orm/entity in declaration files", () => {
    const dtsFiles = readdirSync(DIST_DIR).filter(
      (f) => f.endsWith(".d.mts") || f.endsWith(".d.cts"),
    );

    for (const file of dtsFiles) {
      const content = readFileSync(join(DIST_DIR, file), "utf-8");
      expect(content, `${file} should not import from drizzle-orm/entity`).not.toContain(
        "drizzle-orm/entity",
      );
    }
  });

  it("does not import from @databricks/sql internal paths", () => {
    const dtsFiles = readdirSync(DIST_DIR).filter(
      (f) => f.endsWith(".d.mts") || f.endsWith(".d.cts"),
    );

    for (const file of dtsFiles) {
      const content = readFileSync(join(DIST_DIR, file), "utf-8");
      expect(content, `${file} should not import from @databricks/sql/dist`).not.toContain(
        "@databricks/sql/dist",
      );
      expect(content, `${file} should not reference IDBSQLSession`).not.toContain("IDBSQLSession");
    }
  });
});
