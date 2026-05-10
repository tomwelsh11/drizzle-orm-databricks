# drizzle-orm-databricks

Databricks SQL adapter for Drizzle ORM.

## Commands

- `npm run build` — Build with vite-plus (`vp pack`)
- `npm run check` — Lint and format check (`vp check`); use `--fix` to auto-fix
- `npm test` — Run unit tests
- `npm run test:types` — Type check with `tsc --noEmit`
- `npm run test:e2e` — Run end-to-end tests (requires Databricks credentials)

## Workflow

Before committing, always run:

```sh
npm run check --fix && npm test && npm run test:types
```
