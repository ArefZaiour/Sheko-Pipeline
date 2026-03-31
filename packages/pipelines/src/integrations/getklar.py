"""GetKlar Attribution API client.

Provides spend-by-channel data for a given date via the GetKlar attribution endpoint.

Authentication flow (two-token):
  1. Use GETKLAR_REFRESH_TOKEN (long-lived, from Klar Frontend → Store Settings →
     Attribution API) to obtain a short-lived access token.
  2. Use the access token as Bearer token to call GET /public/attribution.

Required env vars:
    GETKLAR_REFRESH_TOKEN   Long-lived token for the attribution API.
                            Obtained from Klar Frontend (Store Settings → Attribution API).
                            NOTE: This is DIFFERENT from GETKLAR_API_TOKEN which is used
                            for the data-import API (open-api.getklar.com).
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

# Default attribution model — marketing mix is the most balanced for spend comparison.
DEFAULT_METRIC = "marketing_mix"
DEFAULT_WINDOW = "28_day"


@dataclass
class ChannelSpend:
    """Spend and attribution data for a single marketing channel."""

    channel: str
    spend: float  # Actual ad spend in €
    conversions: float
    revenue: float
    raw: dict[str, Any] = field(default_factory=dict)


class GetKlarClient:
    """Client for the GetKlar Attribution API.

    Args:
        refresh_token: Long-lived token from Klar Frontend (Store Settings → Attribution API).
        timeout: HTTP request timeout in seconds.
    """

    def __init__(self, refresh_token: str, timeout: int = 30) -> None:
        if not refresh_token:
            raise EnvironmentError(
                "GETKLAR_REFRESH_TOKEN is required for the GetKlar Attribution API. "
                "Obtain it from Klar Frontend: Store Settings → Attribution API."
            )
        self._refresh_token = refresh_token
        self._timeout = timeout
        self._access_token: str | None = None
        self._access_token_expiry: float = 0.0

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_access_token(self) -> str:
        """Exchange the refresh token for a short-lived access token, with caching."""
        # 30-second safety margin before expiry.
        if self._access_token and time.time() < self._access_token_expiry - 30:
            return self._access_token

        resp = httpx.post(
            f"{ATTRIBUTION_BASE_URL}{TOKEN_ENDPOINT}",
            headers={
                "Content-Type": "application/json",
                "token": self._refresh_token,
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

    def fetch_attribution_report(
        self,
        report_date: date,
        metric: str = DEFAULT_METRIC,
        window: str = DEFAULT_WINDOW,
    ) -> list[dict[str, Any]]:
        """Fetch the full attribution report rows for a single date.

        Args:
            report_date: The date to report on (yesterday for daily reports).
            metric: Attribution model ('marketing_mix', 'last_touch', etc.)
            window: Attribution window ('28_day', '7_day', 'unlimited', etc.)

        Returns:
            List of raw row dicts from the API ``data`` array.
        """
        access_token = self._get_access_token()
        date_str = report_date.isoformat()
        params = {
            "startDate": date_str,
            "endDate": date_str,
            "metric": metric,
            "window": window,
            "date_breakdown": "order",
        }
        resp = httpx.get(
            f"{ATTRIBUTION_BASE_URL}{ATTRIBUTION_ENDPOINT}",
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
            timeout=self._timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        rows: list[dict[str, Any]] = payload.get("data") or []
        log.info(
            "getklar.attribution.fetched",
            date=date_str,
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
        """Return spend grouped by channel for a given date.

        The attribution API returns one row per channel (or per channel/date combo).
        This method normalises rows into :class:`ChannelSpend` objects.

        Known response fields (from API exploration):
            - ``channel`` / ``channelName`` / ``source`` — channel name
            - ``spend`` / ``cost`` / ``adSpend`` — ad spend in €
            - ``conversions`` — attributed conversions
            - ``revenue`` — attributed revenue

        If the API response structure differs, raw rows are logged at DEBUG level
        so the field names can be inspected and this method updated.
        """
        rows = self.fetch_attribution_report(report_date, metric=metric, window=window)

        if rows:
            log.debug("getklar.attribution.sample_row", row=rows[0])

        result: list[ChannelSpend] = []
        for row in rows:
            channel = (
                row.get("channel")
                or row.get("channelName")
                or row.get("source")
                or row.get("name")
                or "Unknown"
            )
            spend = float(
                row.get("spend")
                or row.get("cost")
                or row.get("adSpend")
                or row.get("marketingSpend")
                or 0
            )
            conversions = float(row.get("conversions") or 0)
            revenue = float(row.get("revenue") or 0)
            result.append(
                ChannelSpend(
                    channel=str(channel),
                    spend=spend,
                    conversions=conversions,
                    revenue=revenue,
                    raw=dict(row),
                )
            )

        return result


def build_from_env() -> GetKlarClient:
    """Construct a :class:`GetKlarClient` from environment variables.

    Reads:
        GETKLAR_REFRESH_TOKEN  (required)
    """
    refresh_token = os.environ.get("GETKLAR_REFRESH_TOKEN", "")
    if not refresh_token:
        raise EnvironmentError(
            "GETKLAR_REFRESH_TOKEN environment variable is not set. "
            "Obtain it from Klar Frontend: Store Settings → Attribution API."
        )
    return GetKlarClient(refresh_token=refresh_token)


def yesterday() -> date:
    """Return yesterday's date."""
    return date.today() - timedelta(days=1)
