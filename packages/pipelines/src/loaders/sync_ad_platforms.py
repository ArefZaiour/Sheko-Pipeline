"""Sync campaign metrics from Google Ads and Meta to Postgres.

Usage:
    # Sync both platforms for yesterday
    python -m loaders.sync_ad_platforms

    # Sync specific date range
    python -m loaders.sync_ad_platforms --start 2026-10-01 --end 2026-10-06

    # Sync only one platform
    python -m loaders.sync_ad_platforms --platform google_ads
    python -m loaders.sync_ad_platforms --platform meta

Required env vars:
    DATABASE_URL
    GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET,
    GOOGLE_ADS_REFRESH_TOKEN
    META_ACCESS_TOKEN

    Per-account mappings are loaded from SYNC_ACCOUNTS (JSON):
        '[{"platform":"google_ads","external_id":"123-456-7890"},
          {"platform":"meta","external_id":"act_987654321"}]'
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import date, timedelta
from typing import Any

import psycopg
import structlog

from integrations.google_ads import GoogleAdsClient, build_from_env as build_google_ads
from integrations.meta import MetaAdsClient, build_from_env as build_meta

log = structlog.get_logger(__name__)

_UPSERT_SQL = """
    INSERT INTO campaign_metrics
        (date, platform, account_id, campaign_id, campaign_name,
         impressions, clicks, spend_usd, conversions, revenue_usd)
    VALUES
        (%(date)s, %(platform)s, %(account_id)s, %(campaign_id)s, %(campaign_name)s,
         %(impressions)s, %(clicks)s, %(spend_usd)s, %(conversions)s, %(revenue_usd)s)
    ON CONFLICT (date, platform, account_id, campaign_id)
    DO UPDATE SET
        campaign_name = EXCLUDED.campaign_name,
        impressions   = EXCLUDED.impressions,
        clicks        = EXCLUDED.clicks,
        spend_usd     = EXCLUDED.spend_usd,
        conversions   = EXCLUDED.conversions,
        revenue_usd   = EXCLUDED.revenue_usd,
        synced_at     = now()
"""


def _resolve_account_uuid(conn: Any, platform: str, external_id: str) -> str:
    """Return the ad_accounts.id UUID for (platform, external_id), creating if absent."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM ad_accounts WHERE platform = %s AND external_id = %s",
            (platform, external_id),
        )
        row = cur.fetchone()
        if row:
            return str(row[0])

        # Auto-create the account record when first seen.
        cur.execute(
            """
            INSERT INTO ad_accounts (client_id, platform, external_id, label)
            VALUES (
                (SELECT id FROM clients LIMIT 1),
                %s, %s, %s
            )
            RETURNING id
            """,
            (platform, external_id, external_id),
        )
        return str(cur.fetchone()[0])  # type: ignore[index]


async def _sync_google_ads(
    client: GoogleAdsClient,
    account_external_id: str,
    account_uuid: str,
    start: date,
    end: date,
    conn: Any,
) -> int:
    rows = await client.fetch_campaign_metrics(account_external_id, start, end)
    upserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                _UPSERT_SQL,
                {
                    "date": row["date"],
                    "platform": "google_ads",
                    "account_id": account_uuid,
                    "campaign_id": row["campaign_id"],
                    "campaign_name": row["campaign_name"],
                    "impressions": row["impressions"],
                    "clicks": row["clicks"],
                    "spend_usd": row["spend_usd"],
                    "conversions": row["conversions"],
                    "revenue_usd": row["revenue_usd"],
                },
            )
            upserted += 1
    log.info("sync.google_ads.done", account=account_external_id, rows=upserted)
    return upserted


async def _sync_meta(
    client: MetaAdsClient,
    account_external_id: str,
    account_uuid: str,
    start: date,
    end: date,
    conn: Any,
) -> int:
    rows = await client.fetch_campaign_metrics(account_external_id, start, end)
    upserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                _UPSERT_SQL,
                {
                    "date": row.get("date_start") or row.get("date_stop") or start.isoformat(),
                    "platform": "meta",
                    "account_id": account_uuid,
                    "campaign_id": row["campaign_id"],
                    "campaign_name": row["campaign_name"],
                    "impressions": row["impressions"],
                    "clicks": row["clicks"],
                    "spend_usd": row["spend_usd"],
                    "conversions": row["conversions"],
                    "revenue_usd": row["revenue_usd"],
                },
            )
            upserted += 1
    log.info("sync.meta.done", account=account_external_id, rows=upserted)
    return upserted


async def run_sync(
    platforms: list[str],
    start: date,
    end: date,
    database_url: str,
    accounts: list[dict[str, str]],
) -> dict[str, int]:
    """Run the sync for all configured accounts and return per-platform row counts."""
    google_client: GoogleAdsClient | None = None
    meta_client: MetaAdsClient | None = None

    if "google_ads" in platforms:
        try:
            google_client = build_google_ads()
        except EnvironmentError as exc:
            log.warning("sync.google_ads.skipped", reason=str(exc))

    if "meta" in platforms:
        try:
            meta_client = build_meta()
        except EnvironmentError as exc:
            log.warning("sync.meta.skipped", reason=str(exc))

    totals: dict[str, int] = {"google_ads": 0, "meta": 0}

    async with await psycopg.AsyncConnection.connect(database_url) as conn:
        async with conn.transaction():
            for account in accounts:
                platform = account["platform"]
                external_id = account["external_id"]
                account_uuid = _resolve_account_uuid(conn, platform, external_id)

                if platform == "google_ads" and google_client is not None:
                    totals["google_ads"] += await _sync_google_ads(
                        google_client, external_id, account_uuid, start, end, conn
                    )
                elif platform == "meta" and meta_client is not None:
                    totals["meta"] += await _sync_meta(
                        meta_client, external_id, account_uuid, start, end, conn
                    )

    return totals


def _parse_args() -> argparse.Namespace:
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Sync campaign metrics from ad platforms to DB.")
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=yesterday,
        help="Start date ISO 8601 (default: yesterday)",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=yesterday,
        help="End date ISO 8601 (default: yesterday)",
    )
    parser.add_argument(
        "--platform",
        choices=["google_ads", "meta", "all"],
        default="all",
        help="Which platform to sync (default: all)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    platforms = ["google_ads", "meta"] if args.platform == "all" else [args.platform]

    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        raise EnvironmentError("DATABASE_URL environment variable is not set.")

    accounts_json = os.environ.get("SYNC_ACCOUNTS", "[]")
    try:
        accounts: list[dict[str, str]] = json.loads(accounts_json)
    except json.JSONDecodeError as exc:
        raise EnvironmentError(f"SYNC_ACCOUNTS is not valid JSON: {exc}") from exc

    if not accounts:
        log.warning("sync.no_accounts", hint="Set SYNC_ACCOUNTS env var with account list.")
        return

    totals = asyncio.run(run_sync(platforms, args.start, args.end, database_url, accounts))
    log.info(
        "sync.complete",
        google_ads=totals["google_ads"],
        meta=totals["meta"],
        start=args.start.isoformat(),
        end=args.end.isoformat(),
    )
    print(
        f"Sync complete — Google Ads: {totals['google_ads']} rows, "
        f"Meta: {totals['meta']} rows"
    )


if __name__ == "__main__":
    main()
