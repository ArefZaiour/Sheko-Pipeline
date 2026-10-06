"""Meta Ads (Facebook) API client.

Fetches campaign-level spend and performance metrics via the Marketing API Insights endpoint.

Authentication uses a long-lived User access token or a System User token:
  - META_ACCESS_TOKEN  — access token with `ads_read` permission

Required env vars:
    META_ACCESS_TOKEN

The Ad Account ID is passed per-call (format: `act_<numeric_id>`).

Rate limiting: Meta enforces a sliding-window BUC (Business Use Case) rate limit.
We use tenacity with exponential backoff and respect x-business-use-case-usage headers.
"""
from __future__ import annotations

import os
import time
from datetime import date
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from integrations.base import AdPlatformClient

log = structlog.get_logger(__name__)

_GRAPH_BASE = "https://graph.facebook.com/v21.0"

_INSIGHTS_FIELDS = ",".join(
    [
        "campaign_id",
        "campaign_name",
        "impressions",
        "clicks",
        "spend",
        "actions",
        "action_values",
    ]
)

_PURCHASE_ACTION_TYPES = {"purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase"}


def _extract_purchase_metric(action_list: list[dict[str, Any]], key: str = "value") -> float:
    """Sum `key` across all purchase-type entries in an actions or action_values list."""
    total = 0.0
    for entry in action_list or []:
        if entry.get("action_type") in _PURCHASE_ACTION_TYPES:
            try:
                total += float(entry.get(key, 0) or 0)
            except (TypeError, ValueError):
                pass
    return total


class MetaAdsClient(AdPlatformClient):
    """Meta Ads Marketing API client.

    Args:
        access_token: Long-lived access token with ads_read scope.
        timeout: HTTP timeout in seconds.
    """

    def __init__(self, access_token: str, timeout: int = 60) -> None:
        if not access_token:
            raise EnvironmentError("META_ACCESS_TOKEN is required.")
        self._access_token = access_token.strip()
        self._timeout = timeout

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
    def _get(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        """Issue a GET to the Graph API, respecting rate-limit headers."""
        params = {**params, "access_token": self._access_token}
        response = httpx.get(url, params=params, timeout=self._timeout)

        # Throttle if the BUC cap is near (>= 80 %).
        usage_header = response.headers.get("x-business-use-case-usage", "")
        if '"call_count":' in usage_header:
            import json as _json

            try:
                usage = _json.loads(usage_header)
                for _acct_data in usage.values():
                    for _item in _acct_data if isinstance(_acct_data, list) else []:
                        if int(_item.get("call_count", 0)) >= 80:
                            log.warning("meta_ads.rate_limit.throttling", usage=usage_header)
                            time.sleep(60)
                            break
            except Exception:
                pass

        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def _paginate(self, url: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Collect all pages of a Graph API list response."""
        results: list[dict[str, Any]] = []
        next_url: str | None = url
        next_params: dict[str, Any] | None = params

        while next_url is not None:
            data = self._get(next_url, next_params or {})
            results.extend(data.get("data", []))
            next_url = data.get("paging", {}).get("next")
            next_params = None  # `next` URL already includes all params

        return results

    async def fetch_campaign_metrics(
        self,
        account_id: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Return campaign-level metrics for the given date range.

        Each dict contains:
            campaign_id, campaign_name, date_start, date_stop, impressions,
            clicks, spend_usd, conversions, revenue_usd.

        `account_id` should be in `act_<numeric>` format.
        """
        url = f"{_GRAPH_BASE}/{account_id}/insights"
        params: dict[str, Any] = {
            "fields": _INSIGHTS_FIELDS,
            "level": "campaign",
            "time_range": f'{{"since":"{start_date.isoformat()}","until":"{end_date.isoformat()}"}}',
            "time_increment": 1,  # one row per campaign per day
            "limit": 500,
        }
        rows = self._paginate(url, params)
        results: list[dict[str, Any]] = []
        for row in rows:
            actions = row.get("actions") or []
            action_values = row.get("action_values") or []
            results.append(
                {
                    "campaign_id": row.get("campaign_id", ""),
                    "campaign_name": row.get("campaign_name", ""),
                    "date_start": row.get("date_start", ""),
                    "date_stop": row.get("date_stop", ""),
                    "impressions": int(row.get("impressions", 0) or 0),
                    "clicks": int(row.get("clicks", 0) or 0),
                    "spend_usd": float(row.get("spend", 0) or 0),
                    "conversions": int(_extract_purchase_metric(actions, "value")),
                    "revenue_usd": _extract_purchase_metric(action_values, "value"),
                }
            )
        log.info(
            "meta_ads.metrics.fetched",
            account_id=account_id,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            rows=len(results),
        )
        return results

    async def fetch_budget_pacing(self, account_id: str) -> list[dict[str, Any]]:
        """Return current daily budget and today's spend for active campaigns.

        Each dict contains:
            campaign_id, campaign_name, daily_budget_usd, lifetime_budget_usd,
            status, spend_today_usd, impressions_today.
        """
        # First fetch active campaigns with their budgets.
        campaigns_url = f"{_GRAPH_BASE}/{account_id}/campaigns"
        campaigns = self._paginate(
            campaigns_url,
            {
                "fields": "id,name,status,daily_budget,lifetime_budget",
                "effective_status": '["ACTIVE","PAUSED"]',
                "limit": 200,
            },
        )

        # Then fetch today's insights for those campaign IDs.
        today = date.today().isoformat()
        results: list[dict[str, Any]] = []
        for campaign in campaigns:
            cid = campaign.get("id", "")
            insights_url = f"{_GRAPH_BASE}/{cid}/insights"
            insights = self._paginate(
                insights_url,
                {
                    "fields": "spend,impressions",
                    "time_range": f'{{"since":"{today}","until":"{today}"}}',
                    "limit": 1,
                },
            )
            today_row = insights[0] if insights else {}
            results.append(
                {
                    "campaign_id": cid,
                    "campaign_name": campaign.get("name", ""),
                    "status": campaign.get("status", ""),
                    "daily_budget_usd": float(campaign.get("daily_budget", 0) or 0) / 100,
                    "lifetime_budget_usd": float(campaign.get("lifetime_budget", 0) or 0) / 100,
                    "spend_today_usd": float(today_row.get("spend", 0) or 0),
                    "impressions_today": int(today_row.get("impressions", 0) or 0),
                }
            )
        log.info(
            "meta_ads.pacing.fetched",
            account_id=account_id,
            campaigns=len(results),
        )
        return results


def build_from_env() -> MetaAdsClient:
    """Construct a :class:`MetaAdsClient` from environment variables."""
    access_token = os.environ.get("META_ACCESS_TOKEN", "")
    if not access_token:
        raise EnvironmentError("META_ACCESS_TOKEN environment variable is not set.")
    return MetaAdsClient(access_token=access_token)
