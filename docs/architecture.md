# Architecture Overview

## System Diagram

```
Ad Platforms (Google Ads, Meta)
        │
        ▼
┌────────────────────┐
│  pipelines/        │  Python — fetch, normalise, upsert
│  src/integrations  │
│  src/transforms    │
│  src/loaders       │
└────────┬───────────┘
         │ psycopg3
         ▼
┌────────────────────┐
│  PostgreSQL 16     │  campaign_metrics, ad_accounts, clients
└────────┬───────────┘
         │ HTTP API (Next.js API routes)
         ▼
┌────────────────────┐
│  dashboard/        │  Next.js + Recharts — client reporting UI
└────────────────────┘
```

## Data Flow

1. **Ingestion** — scheduled Python jobs call ad-platform APIs via `AdPlatformClient` subclasses, normalise responses to `CampaignMetrics`, then upsert into Postgres with `ON CONFLICT DO UPDATE`.
2. **API layer** — Next.js API routes query Postgres and return typed JSON.
3. **Dashboard** — React pages fetch via `@tanstack/react-query` and render Recharts visualisations.

## Key Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Pipeline language | Python | google-ads and facebook-business SDKs are Python-first |
| Normalised types | `packages/shared` TypeScript | Single source of truth consumed by both dashboard and any future TS services |
| DB access | psycopg3 (Python) + Postgres JS (TS) | Async-native drivers for both stacks |
| Monorepo tooling | Turborepo | Incremental builds across TS packages; Python handled separately |
| Reporting UI | Next.js + Recharts | SSR-friendly, no heavy BI dependency, easy to white-label |
