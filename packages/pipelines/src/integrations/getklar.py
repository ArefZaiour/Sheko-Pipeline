"""GetKlar Attribution API client.

Provides spend-by-channel data for a given date via the GetKlar attribution endpoint.

Authentication flow (two-token):
  1. Use GETKLAR_API_TOKEN (long-lived JWT from Klar frontend) to obtain a
     short-lived access token via POST /public/auth/token.
  2. Use the access token as Bearer token to call GET /public/attribution.

The API returns one row per ad (ad × date granularity). This module aggregates
rows by channelName to produce per-channel totals.

Required env vars:
    GETKLAR_API_TOKEN   Long-lived JWT from Klar Frontend (already in .env).
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

ATTRIBUTION_BASE_URL = "https://api.getklar.com"
TOKEN_ENDPOINT = "/public/auth/token"
ATTRIBUTION_ENDPOINT = "/public/attribution"

# Default attribution model — marketing mix matches the template.
DEFAULT_METRIC = "marketing_mix"
DEFAULT_WINDOW = "28_day"


@dataclass
class ChannelSpend:
    """Aggregated spend and attribution data for a single marketing channel."""

    channel: str
    spend: float      # Total ad spend in € (sum of 'cost' across all ads in channel)
    orders: float
    revenue: float
    raw_rows: list[dict[str, Any]] = field(default_factory=list)


class GetKlarClient:
    """Client for the GetKlar Attribution API.

    Args:
        api_token: Long-lived JWT from Klar Frontend (GETKLAR_API_TOKEN).
        timeout: HTTP request timeout in seconds.
    """

    def __init__(self, api_token: str, timeout: int = 60) -> None:
        if not api_token:
            raise EnvironmentError(
                "GETKLAR_API_TOKEN is required for the GetKlar Attribution API."
            )
        self._api_token = api_token
        self._timeout = timeout
        self._access_token: str | None = None
        self._access_token_expiry: float = 0.0

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_access_token(self) -> str:
        """Exchange the long-lived token for a short-lived access token."""
        # 30-second safety margin before expiry.
        if self._access_token and time.time() < self._access_token_expiry - 30:
            return self._access_token

        resp = httpx.post(
            f"{ATTRIBUTION_BASE_URL}{TOKEN_ENDPOINT}",
            headers={
                "Content-Type": "application/json",
                "token": self._api_token,
            },
            timeout=self._timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["accessToken"]
        # expiresIn is in milliseconds (e.g. 300000 = 5 min).
        self._access_token_expiry = time.time() + data["expiresIn"] / 1000
        log.debug("getklar.auth.token_refreshed")
        return self._access_token  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_raw_rows(
        self,
        report_date: date,
        metric: str = DEFAULT_METRIC,
        window: str = DEFAULT_WINDOW,
    ) -> list[dict[str, Any]]:
        """Fetch raw attribution rows for a single date (ad-level granularity).

        The API returns one row per ad×date combination. Use
        :meth:`fetch_spend_by_channel` for channel-aggregated totals.

        Note: The API requires startDate < endDate (single-day queries return empty).
        We query startDate=report_date, endDate=report_date+1 and all returned rows
        carry report_date as their date.

        Args:
            report_date: The date to report on.
            metric: Attribution model ('marketing_mix', 'last_touch', etc.)
            window: Attribution window ('28_day', '7_day', 'unlimited', etc.)

        Returns:
            List of raw row dicts. Key fields: channelName, cost, orders,
            netRevenue, campaignName, adGroupName, adName, clicks, impressions.
        """
        access_token = self._get_access_token()
        start_str = report_date.isoformat()
        end_str = (report_date + timedelta(days=1)).isoformat()
        resp = httpx.get(
            f"{ATTRIBUTION_BASE_URL}{ATTRIBUTION_ENDPOINT}",
            headers={"Authorization": f"Bearer {access_token}"},
            params={
                "startDate": start_str,
                "endDate": end_str,
                "metric": metric,
                "window": window,
                "date_breakdown": "order",
            },
            timeout=self._timeout,
        )
        resp.raise_for_status()
        rows: list[dict[str, Any]] = resp.json() or []
        log.info(
            "getklar.attribution.fetched",
            date=start_str,
            metric=metric,
            rows=len(rows),
        )
        return rows

    def fetch_spend_by_channel(
        self,
        report_date: date,
        metric: str = DEFAULT_METRIC,
        window: str = DEFAULT_WINDOW,
    ) -> list[ChannelSpend]:
        """Return spend aggregated by channel for a given date.

        Sums ``cost``, ``orders``, and ``netRevenue`` across all ad-level rows
        for each unique ``channelName``.

        Args:
            report_date: The date to report on (yesterday for daily reports).
            metric: Attribution model.
            window: Attribution window.

        Returns:
            List of :class:`ChannelSpend`, one per channel, sorted by spend desc.
        """
        rows = self.fetch_raw_rows(report_date, metric=metric, window=window)

        # Aggregate by channelName.
        aggregated: dict[str, dict[str, Any]] = {}
        for row in rows:
            channel = str(row.get("channelName") or "Unknown")
            if channel not in aggregated:
                aggregated[channel] = {
                    "spend": 0.0,
                    "orders": 0.0,
                    "revenue": 0.0,
                    "raw_rows": [],
                }
            aggregated[channel]["spend"] += float(row.get("cost") or 0)
            aggregated[channel]["orders"] += float(row.get("orders") or 0)
            aggregated[channel]["revenue"] += float(row.get("netRevenue") or 0)
            aggregated[channel]["raw_rows"].append(row)

        result = [
            ChannelSpend(
                channel=ch,
                spend=agg["spend"],
                orders=agg["orders"],
                revenue=agg["revenue"],
                raw_rows=agg["raw_rows"],
            )
            for ch, agg in aggregated.items()
        ]
        result.sort(key=lambda x: x.spend, reverse=True)

        log.info(
            "getklar.attribution.by_channel",
            date=report_date.isoformat(),
            channels=len(result),
            total_spend=sum(c.spend for c in result),
        )
        return result


def build_from_env() -> GetKlarClient:
    """Construct a :class:`GetKlarClient` from environment variables.

    Reads:
        GETKLAR_API_TOKEN  (required)
    """
    api_token = os.environ.get("GETKLAR_API_TOKEN", "")
    if not api_token:
        raise EnvironmentError(
            "GETKLAR_API_TOKEN environment variable is not set."
        )
    return GetKlarClient(api_token=api_token)


def yesterday() -> date:
    """Return yesterday's date."""
    return date.today() - timedelta(days=1)
