# Performance Marketing Platform

Infrastructure for managing, tracking, and reporting on client ad campaigns across Google Ads and Meta.

## Repo Structure

```
packages/
  pipelines/    # Python — data ingestion, campaign sync, budget pacing
  dashboard/    # TypeScript/Next.js — client reporting dashboard
  shared/       # TypeScript — shared types and utilities
db/
  migrations/   # SQL migrations (postgres)
docs/
  architecture.md
  conventions.md
.github/
  workflows/    # CI/CD (GitHub Actions)
```

## Quick Start

### Pipelines (Python ≥ 3.11)

```bash
cd packages/pipelines
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

### Dashboard (Node ≥ 20)

```bash
cd packages/dashboard
npm install
npm run dev
```

## Tech Stack

| Layer | Choice | Rationale |
|---|---|---|
| Data pipelines | Python 3.11+ | Rich ad-platform SDK ecosystem |
| Dashboard | Next.js 14 (TypeScript) | Fast SSR, good charting support |
| Database | PostgreSQL 16 | JSONB for flexible campaign metadata |
| CI/CD | GitHub Actions | Native GitHub integration |
| Formatting | black + ruff (Python), prettier + eslint (TS) | Consistent style across the repo |
