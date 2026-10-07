"""Outbrain Amplify API client for campaign duplication and creative upload.

Authentication: API key sent as ``OB-TOKEN-V1`` request header.
Base URL: https://api.outbrain.com/amplify/v0.1

Required env vars:
    OUTBRAIN_API_KEY                  API key from Outbrain Amplify console
    OUTBRAIN_ACCOUNT_ID               Outbrain marketer account ID
    OUTBRAIN_TEMPLATE_CAMPAIGN_ID     ID of the campaign to clone
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

BASE_URL = "https://api.outbrain.com/amplify/v0.1"


@dataclass
class OutbrainCampaign:
    id: str
    name: str
    budget: dict[str, Any]
    targeting: dict[str, Any]
    status: str
    raw: dict[str, Any]


@dataclass
class OutbrainCreative:
    id: str
    campaign_id: str
    title: str
    url: str
    image_url: str
    status: str


class OutbrainClient:
    """Outbrain Amplify API client.

    Args:
        api_key:     Value for the ``OB-TOKEN-V1`` header.
        account_id:  Marketer account ID (used in URL paths).
        timeout:     HTTP timeout in seconds.
    """

    def __init__(self, api_key: str, account_id: str, timeout: int = 60) -> None:
        if not api_key:
            raise OSError("OUTBRAIN_API_KEY is required.")
        if not account_id:
            raise OSError("OUTBRAIN_ACCOUNT_ID is required.")
        self._headers = {"OB-TOKEN-V1": api_key, "Content-Type": "application/json"}
        self._account_id = account_id
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Campaigns
    # ------------------------------------------------------------------

    def get_campaign(self, campaign_id: str) -> OutbrainCampaign:
        """Fetch a single campaign by ID."""
        resp = httpx.get(
            f"{BASE_URL}/campaigns/{campaign_id}",
            headers=self._headers,
            timeout=self._timeout,
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        log.info("outbrain.campaign.fetched", campaign_id=campaign_id, name=data.get("name"))
        return self._parse_campaign(data)

    def duplicate_campaign(
        self,
        template_id: str,
        new_name: str,
        start_date: date | None = None,
    ) -> OutbrainCampaign:
        """Clone a template campaign and schedule it to start on ``start_date``.

        Fetches the template campaign, strips ID/timestamps, sets the new name
        and start date, then POSTs it as a new campaign.

        Args:
            template_id: ID of the campaign to clone.
            new_name:    Name for the new campaign.
            start_date:  Launch date; defaults to tomorrow.
        """
        if start_date is None:
            start_date = date.today() + timedelta(days=1)

        template = self.get_campaign(template_id)
        payload: dict[str, Any] = dict(template.raw)

        # Strip server-managed fields before POSTing.
        for key in ("id", "createdAt", "modifiedAt", "statistics"):
            payload.pop(key, None)

        payload["name"] = new_name
        payload["startTime"] = start_date.isoformat()
        # Keep endTime absent so the campaign runs indefinitely after launch.
        payload.pop("endTime", None)
        # New campaigns start PENDING; Outbrain will activate at startTime.
        payload["status"] = "PENDING"

        resp = httpx.post(
            f"{BASE_URL}/marketers/{self._account_id}/campaigns",
            headers=self._headers,
            json=payload,
            timeout=self._timeout,
        )
        resp.raise_for_status()
        new_campaign = self._parse_campaign(resp.json())
        log.info(
            "outbrain.campaign.created",
            campaign_id=new_campaign.id,
            name=new_campaign.name,
            start_date=start_date.isoformat(),
        )
        return new_campaign

    # ------------------------------------------------------------------
    # Creatives (promoted links)
    # ------------------------------------------------------------------

    def upload_image(self, image_path: Path) -> str:
        """Upload a PNG/JPEG to Outbrain and return the hosted image URL.

        Outbrain's image upload endpoint accepts multipart/form-data with a
        ``file`` field.
        """
        upload_headers = {k: v for k, v in self._headers.items() if k != "Content-Type"}
        with image_path.open("rb") as fh:
            resp = httpx.post(
                f"{BASE_URL}/images/upload",
                headers=upload_headers,
                files={"file": (image_path.name, fh, "image/png")},
                timeout=self._timeout,
            )
        resp.raise_for_status()
        image_url: str = resp.json()["url"]
        log.info("outbrain.image.uploaded", filename=image_path.name, url=image_url)
        return image_url

    def create_creative(
        self,
        campaign_id: str,
        title: str,
        landing_url: str,
        image_path: Path,
    ) -> OutbrainCreative:
        """Upload image and attach it as a promoted link on the campaign.

        Args:
            campaign_id:  Target campaign ID.
            title:        Ad headline (Outbrain character limit: 100).
            landing_url:  Destination URL for clicks.
            image_path:   Local path to the PNG creative.
        """
        image_url = self.upload_image(image_path)
        payload = {
            "title": title,
            "url": landing_url,
            "imageUrl": image_url,
        }
        resp = httpx.post(
            f"{BASE_URL}/campaigns/{campaign_id}/promotedLinks",
            headers=self._headers,
            json=payload,
            timeout=self._timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        creative = OutbrainCreative(
            id=data["id"],
            campaign_id=campaign_id,
            title=data.get("title", title),
            url=data.get("url", landing_url),
            image_url=image_url,
            status=data.get("status", "ACTIVE"),
        )
        log.info(
            "outbrain.creative.created",
            creative_id=creative.id,
            campaign_id=campaign_id,
            filename=image_path.name,
        )
        return creative

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_campaign(self, campaign: OutbrainCampaign) -> list[str]:
        """Return a list of validation warnings for the campaign.

        Returns an empty list if the campaign looks correctly configured.
        """
        warnings: list[str] = []
        budget = campaign.budget
        if not budget or budget.get("amount", 0) <= 0:
            warnings.append(f"Campaign {campaign.id!r}: budget amount is 0 or missing.")
        targeting = campaign.targeting
        if not targeting:
            warnings.append(f"Campaign {campaign.id!r}: no targeting configuration set.")
        if campaign.status not in ("ACTIVE", "PENDING"):
            warnings.append(f"Campaign {campaign.id!r}: unexpected status {campaign.status!r}.")
        return warnings

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_campaign(data: dict[str, Any]) -> OutbrainCampaign:
        return OutbrainCampaign(
            id=data["id"],
            name=data.get("name", ""),
            budget=data.get("budget", {}),
            targeting=data.get("targeting", {}),
            status=data.get("status", ""),
            raw=data,
        )


def build_from_env() -> OutbrainClient:
    """Construct an :class:`OutbrainClient` from environment variables.

    Reads:
        OUTBRAIN_API_KEY       (required)
        OUTBRAIN_ACCOUNT_ID    (required)
    """
    api_key = os.environ.get("OUTBRAIN_API_KEY", "")
    account_id = os.environ.get("OUTBRAIN_ACCOUNT_ID", "")
    if not api_key:
        raise OSError("OUTBRAIN_API_KEY environment variable is not set.")
    if not account_id:
        raise OSError("OUTBRAIN_ACCOUNT_ID environment variable is not set.")
    return OutbrainClient(api_key=api_key, account_id=account_id)
