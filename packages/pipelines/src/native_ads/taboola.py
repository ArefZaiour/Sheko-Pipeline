"""Taboola Backstage API client for campaign duplication and creative upload.

Authentication: OAuth2 client credentials grant.
Token URL:  https://backstage.taboola.com/backstage/oauth/token
Base URL:   https://backstage.taboola.com/backstage/api/1.0

Required env vars:
    TABOOLA_CLIENT_ID       OAuth2 client ID from Taboola Backstage
    TABOOLA_CLIENT_SECRET   OAuth2 client secret from Taboola Backstage
    TABOOLA_ACCOUNT_ID      Taboola advertiser account ID / name
    TABOOLA_TEMPLATE_CAMPAIGN_ID  ID of the campaign to clone
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

BASE_URL = "https://backstage.taboola.com/backstage/api/1.0"
TOKEN_URL = "https://backstage.taboola.com/backstage/oauth/token"


@dataclass
class TaboolaCampaign:
    id: str
    name: str
    budget: float
    daily_budget: float | None
    status: str
    start_date: str
    raw: dict[str, Any]


@dataclass
class TaboolaCreative:
    id: str
    campaign_id: str
    title: str
    url: str
    thumbnail_url: str
    status: str


class TaboolaClient:
    """Taboola Backstage API client.

    Args:
        client_id:     OAuth2 client ID.
        client_secret: OAuth2 client secret.
        account_id:    Taboola advertiser account ID (used in URL paths).
        timeout:       HTTP timeout in seconds.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        account_id: str,
        timeout: int = 60,
    ) -> None:
        if not client_id:
            raise EnvironmentError("TABOOLA_CLIENT_ID is required.")
        if not client_secret:
            raise EnvironmentError("TABOOLA_CLIENT_SECRET is required.")
        if not account_id:
            raise EnvironmentError("TABOOLA_ACCOUNT_ID is required.")
        self._client_id = client_id
        self._client_secret = client_secret
        self._account_id = account_id
        self._timeout = timeout
        self._access_token: str | None = None
        self._token_expiry: float = 0.0

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_access_token(self) -> str:
        """Return a valid OAuth2 bearer token, refreshing if expired."""
        if self._access_token and time.time() < self._token_expiry - 30:
            return self._access_token

        resp = httpx.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self._timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expiry = time.time() + int(data.get("expires_in", 3600))
        log.debug("taboola.auth.token_refreshed")
        return self._access_token  # type: ignore[return-value]

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Campaigns
    # ------------------------------------------------------------------

    def get_campaign(self, campaign_id: str) -> TaboolaCampaign:
        """Fetch a single campaign by ID."""
        resp = httpx.get(
            f"{BASE_URL}/{self._account_id}/campaigns/{campaign_id}",
            headers=self._auth_headers(),
            timeout=self._timeout,
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        log.info("taboola.campaign.fetched", campaign_id=campaign_id, name=data.get("name"))
        return self._parse_campaign(data)

    def duplicate_campaign(
        self,
        template_id: str,
        new_name: str,
        start_date: date | None = None,
    ) -> TaboolaCampaign:
        """Clone a template campaign and schedule it to start on ``start_date``.

        Args:
            template_id: ID of the campaign to clone.
            new_name:    Name for the new campaign.
            start_date:  Launch date; defaults to tomorrow.
        """
        if start_date is None:
            start_date = date.today() + timedelta(days=1)

        template = self.get_campaign(template_id)
        payload: dict[str, Any] = dict(template.raw)

        # Strip server-managed fields.
        for key in ("id", "advertiser_id", "approval_state", "is_active", "spending"):
            payload.pop(key, None)

        payload["name"] = new_name
        payload["start_date"] = start_date.isoformat()
        # Remove end_date so the campaign runs indefinitely.
        payload.pop("end_date", None)
        payload["status"] = "PENDING"

        resp = httpx.post(
            f"{BASE_URL}/{self._account_id}/campaigns/",
            headers=self._auth_headers(),
            json=payload,
            timeout=self._timeout,
        )
        resp.raise_for_status()
        new_campaign = self._parse_campaign(resp.json())
        log.info(
            "taboola.campaign.created",
            campaign_id=new_campaign.id,
            name=new_campaign.name,
            start_date=start_date.isoformat(),
        )
        return new_campaign

    # ------------------------------------------------------------------
    # Creatives (campaign items)
    # ------------------------------------------------------------------

    def create_creative(
        self,
        campaign_id: str,
        title: str,
        landing_url: str,
        image_path: Path,
    ) -> TaboolaCreative:
        """Upload a PNG creative and attach it as a campaign item.

        Taboola Backstage accepts the image as a URL. We upload it via the
        ``thumbnail_url`` field using a data URI or hosted URL. Since
        Taboola's API requires a publicly accessible URL, we first upload
        the image via the Taboola image upload endpoint, then reference it.

        Args:
            campaign_id:  Target campaign ID.
            title:        Ad headline.
            landing_url:  Destination URL for clicks.
            image_path:   Local path to the PNG creative.
        """
        thumbnail_url = self._upload_image(image_path)
        payload = {
            "type": "ITEM",
            "title": title,
            "url": landing_url,
            "thumbnail_url": thumbnail_url,
            "status": "RUNNING",
        }
        resp = httpx.post(
            f"{BASE_URL}/{self._account_id}/campaigns/{campaign_id}/items/",
            headers=self._auth_headers(),
            json=payload,
            timeout=self._timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        creative = TaboolaCreative(
            id=str(data.get("id", "")),
            campaign_id=campaign_id,
            title=data.get("title", title),
            url=data.get("url", landing_url),
            thumbnail_url=thumbnail_url,
            status=data.get("status", "RUNNING"),
        )
        log.info(
            "taboola.creative.created",
            creative_id=creative.id,
            campaign_id=campaign_id,
            filename=image_path.name,
        )
        return creative

    def _upload_image(self, image_path: Path) -> str:
        """Upload an image to Taboola's media library and return the hosted URL."""
        upload_headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
        }
        with image_path.open("rb") as fh:
            resp = httpx.post(
                f"{BASE_URL}/{self._account_id}/medias/",
                headers=upload_headers,
                files={"image": (image_path.name, fh, "image/png")},
                timeout=self._timeout,
            )
        resp.raise_for_status()
        url: str = resp.json()["thumbnail_url"]
        log.info("taboola.image.uploaded", filename=image_path.name, url=url)
        return url

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_campaign(self, campaign: TaboolaCampaign) -> list[str]:
        """Return a list of validation warnings for the campaign."""
        warnings: list[str] = []
        if campaign.budget <= 0 and (campaign.daily_budget is None or campaign.daily_budget <= 0):
            warnings.append(f"Campaign {campaign.id!r}: budget is 0 or missing.")
        if campaign.status not in ("RUNNING", "PENDING", "PAUSED"):
            warnings.append(
                f"Campaign {campaign.id!r}: unexpected status {campaign.status!r}."
            )
        return warnings

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_campaign(data: dict[str, Any]) -> TaboolaCampaign:
        return TaboolaCampaign(
            id=str(data.get("id", "")),
            name=data.get("name", ""),
            budget=float(data.get("budget", 0) or 0),
            daily_budget=float(data["daily_cap"]) if data.get("daily_cap") else None,
            status=data.get("status", ""),
            start_date=data.get("start_date", ""),
            raw=data,
        )


def build_from_env() -> TaboolaClient:
    """Construct a :class:`TaboolaClient` from environment variables.

    Reads:
        TABOOLA_CLIENT_ID       (required)
        TABOOLA_CLIENT_SECRET   (required)
        TABOOLA_ACCOUNT_ID      (required)
    """
    client_id = os.environ.get("TABOOLA_CLIENT_ID", "")
    client_secret = os.environ.get("TABOOLA_CLIENT_SECRET", "")
    account_id = os.environ.get("TABOOLA_ACCOUNT_ID", "")
    missing = [
        name
        for name, val in [
            ("TABOOLA_CLIENT_ID", client_id),
            ("TABOOLA_CLIENT_SECRET", client_secret),
            ("TABOOLA_ACCOUNT_ID", account_id),
        ]
        if not val
    ]
    if missing:
        raise EnvironmentError(
            f"Missing required Taboola env vars: {', '.join(missing)}"
        )
    return TaboolaClient(
        client_id=client_id,
        client_secret=client_secret,
        account_id=account_id,
    )
