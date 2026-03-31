# Development Conventions

## Branching

- `main` — production-ready code only; direct pushes prohibited
- `feat/<ticket>-<slug>` — new features (e.g. `feat/GRO-7-google-ads-sync`)
- `fix/<ticket>-<slug>` — bug fixes
- `chore/<slug>` — maintenance (dependency bumps, config changes)

All PRs require at least one approval and a passing CI run before merge.

## Commits

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(pipelines): add Google Ads campaign sync
fix(dashboard): correct date range filter off-by-one
chore: bump ruff to 0.5
```

## Python (packages/pipelines)

- Formatter: **black** (line length 100)
- Linter: **ruff** — `E`, `F`, `I`, `UP`, `B` rule sets
- Type checker: **mypy** strict mode
- Tests: **pytest** with `asyncio_mode = "auto"`
- All public functions must have type annotations.

## TypeScript (packages/dashboard, packages/shared)

- Formatter: **prettier** (singleQuote, semi)
- Linter: **eslint** with typescript-eslint
- No `any` types unless absolutely unavoidable; prefer `unknown`.
- React components: functional only, no class components.

## Environment Variables

- Store secrets in `.env` (gitignored) locally; in CI/prod use repository secrets.
- Copy `.env.example` for required keys — keep it up to date.
- Required pipeline vars: `DATABASE_URL`, `GOOGLE_ADS_DEVELOPER_TOKEN`, `META_ACCESS_TOKEN`
- Required dashboard vars: `DATABASE_URL`, `NEXTAUTH_SECRET`

## Database Migrations

- Plain SQL files in `db/migrations/`, zero-padded 3-digit prefix (e.g. `003_add_budgets.sql`).
- Migrations are append-only — never modify an existing migration that has been merged.
- Run locally: `psql $DATABASE_URL -f db/migrations/<file>.sql`
