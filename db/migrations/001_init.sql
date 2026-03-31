-- Initial schema for performance marketing platform
-- Run via: psql $DATABASE_URL -f db/migrations/001_init.sql

CREATE TABLE IF NOT EXISTS clients (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ad_accounts (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id    UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    platform     TEXT NOT NULL CHECK (platform IN ('google_ads', 'meta')),
    external_id  TEXT NOT NULL,
    label        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (platform, external_id)
);

CREATE TABLE IF NOT EXISTS campaign_metrics (
    id             BIGSERIAL PRIMARY KEY,
    date           DATE NOT NULL,
    platform       TEXT NOT NULL,
    account_id     UUID NOT NULL REFERENCES ad_accounts(id),
    campaign_id    TEXT NOT NULL,
    campaign_name  TEXT NOT NULL,
    impressions    BIGINT NOT NULL DEFAULT 0,
    clicks         BIGINT NOT NULL DEFAULT 0,
    spend_usd      NUMERIC(12, 4) NOT NULL DEFAULT 0,
    conversions    BIGINT NOT NULL DEFAULT 0,
    revenue_usd    NUMERIC(12, 4) NOT NULL DEFAULT 0,
    synced_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (date, platform, account_id, campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_campaign_metrics_date ON campaign_metrics (date DESC);
CREATE INDEX IF NOT EXISTS idx_campaign_metrics_account ON campaign_metrics (account_id, date DESC);
