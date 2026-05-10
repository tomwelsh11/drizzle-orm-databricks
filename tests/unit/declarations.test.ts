import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

const SRC_DIR = join(import.meta.dirname, "../../src");

function readAllSourceFiles(): { file: string; content: string }[] {
  const results: { file: string; content: string }[] = [];
  function walk(dir: string) {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) walk(path);
      else if (entry.name.endsWith(".ts") && !entry.name.endsWith(".test.ts")) {
        results.push({
          file: path.replace(SRC_DIR + "/", ""),
          content: readFileSync(path, "utf-8"),
        });
      }
    }
  }
  walk(SRC_DIR);
  return results;
}

describe("Public API surface guards", () => {
  const sources = readAllSourceFiles();

  it("does not use entityKind as a static property declaration", () => {
    const pattern = /static\s+(override\s+)?readonly\s+\[entityKind\]/;
    for (const { file, content } of sources) {
      expect(
        pattern.test(content),
        `${file} uses entityKind as a static property — use a static block instead`,
      ).toBe(false);
    }
  });

  it("does not import from @databricks/sql internal paths in public modules", () => {
    const publicModules = sources.filter(
      ({ file }) => !file.startsWith("connection") && !file.startsWith("session-pool"),
    );
    for (const { file, content } of publicModules) {
      expect(content, `${file} should not import from @databricks/sql/dist`).not.toContain(
        "@databricks/sql/dist",
      );
    }
  });

  it("SessionExecutor does not reference IDBSQLSession", () => {
    const sessionFile = sources.find(({ file }) => file === "session.ts");
    expect(sessionFile).toBeDefined();
    expect(sessionFile!.content).not.toContain("IDBSQLSession");
  });
});
