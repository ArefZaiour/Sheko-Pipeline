"""Google Ads API client.

Fetches campaign-level spend and performance metrics via the Google Ads Query Language (GAQL).

Authentication uses OAuth2 with a developer token:
  - GOOGLE_ADS_DEVELOPER_TOKEN  — developer token from the Google Ads API Center
  - GOOGLE_ADS_CLIENT_ID        — OAuth2 client ID
  - GOOGLE_ADS_CLIENT_SECRET    — OAuth2 client secret
  - GOOGLE_ADS_REFRESH_TOKEN    — long-lived refresh token for the account
  - GOOGLE_ADS_LOGIN_CUSTOMER_ID — (optional) MCC/manager account ID for managed accounts

Required env vars:
    GOOGLE_ADS_DEVELOPER_TOKEN
    GOOGLE_ADS_CLIENT_ID
    GOOGLE_ADS_CLIENT_SECRET
    GOOGLE_ADS_REFRESH_TOKEN
"""

from __future__ import annotations

import os
from datetime import date
from typing import Any

import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from integrations.base import AdPlatformClient

log = structlog.get_logger(__name__)

_MICROS_PER_UNIT = 1_000_000

# GAQL query: campaign metrics for a date range.
_METRICS_QUERY = """
    SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value,
        segments.date
    FROM campaign
    WHERE segments.date BETWEEN '{start}' AND '{end}'
      AND campaign.status != 'REMOVED'
    ORDER BY campaign.id
"""

# GAQL query: current budget and pacing for active campaigns.
_PACING_QUERY = """
    SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        campaign_budget.amount_micros,
        campaign_budget.period,
        metrics.cost_micros,
        metrics.impressions
    FROM campaign
    WHERE campaign.status = 'ENABLED'
    ORDER BY campaign.id
"""


class GoogleAdsClient(AdPlatformClient):
    """Google Ads API client using the google-ads Python library.

    Args:
        developer_token: Developer token from the Google Ads API Center.
        client_id: OAuth2 client ID.
        client_secret: OAuth2 client secret.
        refresh_token: Long-lived OAuth2 refresh token.
        login_customer_id: Manager (MCC) account ID, required for managed accounts.
    """

    def __init__(
        self,
        developer_token: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        login_customer_id: str | None = None,
    ) -> None:
        if not developer_token:
            raise OSError("GOOGLE_ADS_DEVELOPER_TOKEN is required.")
        if not client_id or not client_secret or not refresh_token:
            raise OSError(
                "GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, and "
                "GOOGLE_ADS_REFRESH_TOKEN are all required."
            )
        self._developer_token = developer_token
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._login_customer_id = login_customer_id
        self._client: Any = None  # lazy-initialised google.ads.googleads.client.GoogleAdsClient

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        # Import lazily so tests can run without the SDK installed.
        from google.ads.googleads.client import GoogleAdsClient as _GAClient

        config: dict[str, Any] = {
            "developer_token": self._developer_token,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "refresh_token": self._refresh_token,
            "use_proto_plus": True,
        }
        if self._login_customer_id:
            config["login_customer_id"] = self._login_customer_id

        self._client = _GAClient.load_from_dict(config)
        return self._client

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _run_query(self, customer_id: str, query: str) -> list[Any]:
        """Execute a GAQL query and return all result rows."""
        client = self._get_client()
        ga_service = client.get_service("GoogleAdsService")
        request = client.get_type("SearchGoogleAdsStreamRequest")
        request.customer_id = customer_id.replace("-", "")
        request.query = query
        rows = []
        for batch in ga_service.search_stream(request=request):
            rows.extend(batch.results)
        return rows

    async def fetch_campaign_metrics(
        self,
        account_id: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Return campaign-level metrics for the given date range.

        Each dict contains:
            campaign_id, campaign_name, date, impressions, clicks,
            spend_usd (converted from micros), conversions, revenue_usd.
        """
        query = _METRICS_QUERY.format(
            start=start_date.isoformat(),
            end=end_date.isoformat(),
        )
        rows = self._run_query(account_id, query)
        results: list[dict[str, Any]] = []
        for row in rows:
            results.append(
                {
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "date": row.segments.date,
                    "impressions": int(row.metrics.impressions),
                    "clicks": int(row.metrics.clicks),
                    "spend_usd": row.metrics.cost_micros / _MICROS_PER_UNIT,
                    "conversions": int(row.metrics.conversions),
                    "revenue_usd": float(row.metrics.conversions_value),
                }
            )
        log.info(
            "google_ads.metrics.fetched",
            account_id=account_id,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            rows=len(results),
        )
        return results

    async def fetch_budget_pacing(self, account_id: str) -> list[dict[str, Any]]:
        """Return current budget and today-so-far spend for active campaigns.

        Each dict contains:
            campaign_id, campaign_name, budget_usd, budget_period,
            spend_today_usd, impressions_today.
        """
        rows = self._run_query(account_id, _PACING_QUERY)
        results: list[dict[str, Any]] = []
        for row in rows:
            results.append(
                {
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "budget_usd": row.campaign_budget.amount_micros / _MICROS_PER_UNIT,
                    "budget_period": str(row.campaign_budget.period),
                    "spend_today_usd": row.metrics.cost_micros / _MICROS_PER_UNIT,
                    "impressions_today": int(row.metrics.impressions),
                }
            )
        log.info(
            "google_ads.pacing.fetched",
            account_id=account_id,
            campaigns=len(results),
        )
        return results


def build_from_env() -> GoogleAdsClient:
    """Construct a :class:`GoogleAdsClient` from environment variables."""
    return GoogleAdsClient(
        developer_token=os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", ""),
        client_id=os.environ.get("GOOGLE_ADS_CLIENT_ID", ""),
        client_secret=os.environ.get("GOOGLE_ADS_CLIENT_SECRET", ""),
        refresh_token=os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", ""),
        login_customer_id=os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID"),
    )
