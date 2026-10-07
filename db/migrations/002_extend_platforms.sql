-- Migration 002: extend platform support and add native ad upload tracking
-- Run via: psql $DATABASE_URL -f db/migrations/002_extend_platforms.sql

-- Extend platform constraint to include native ad networks.
-- Drop the old constraint and replace it with a broader one.
ALTER TABLE ad_accounts
    DROP CONSTRAINT IF EXISTS ad_accounts_platform_check;

ALTER TABLE ad_accounts
    ADD CONSTRAINT ad_accounts_platform_check
        CHECK (platform IN ('google_ads', 'meta', 'outbrain', 'taboola'));

-- Track which NATIVE_MS packages have been uploaded, preventing duplicate uploads.
CREATE TABLE IF NOT EXISTS native_ad_uploads (
    id              BIGSERIAL PRIMARY KEY,
    package_name    TEXT NOT NULL,          -- e.g. NATIVE_MS_2173_STATIC_SHEKO
    platform        TEXT NOT NULL CHECK (platform IN ('outbrain', 'taboola')),
    campaign_id     TEXT,                   -- ID returned by the ad platform
    images_count    INT NOT NULL DEFAULT 0,
    landing_url     TEXT,
    uploaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (package_name, platform)         -- idempotent: skip if already uploaded
);

CREATE INDEX IF NOT EXISTS idx_native_ad_uploads_package ON native_ad_uploads (package_name);
CREATE INDEX IF NOT EXISTS idx_native_ad_uploads_uploaded ON native_ad_uploads (uploaded_at DESC);

-- Seed a default client if the table is empty (single-client installations).
-- Production multi-client installs should insert clients explicitly instead.
INSERT INTO clients (name)
SELECT 'Default Client'
WHERE NOT EXISTS (SELECT 1 FROM clients)
ON CONFLICT DO NOTHING;
