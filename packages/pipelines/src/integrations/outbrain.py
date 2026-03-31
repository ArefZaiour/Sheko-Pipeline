"""Outbrain Amplify API client — duplicate campaign and upload creatives.

Docs: https://amplifyv01.docs.apiary.io/

Required env vars:
    OUTBRAIN_API_KEY         — API key from Outbrain Amplify console
    OUTBRAIN_ACCOUNT_ID      — Marketer (account) ID, e.g. ``00eee1234567890abc``
    OUTBRAIN_TEMPLATE_CAMPAIGN_ID — Campaign ID to clone as template

The Outbrain Amplify v1 API uses HTTP Basic auth on the token endpoint and
then a Bearer token for all subsequent calls.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

BASE_URL = "https://api.outbrain.com/amplify/v0.1"


class OutbrainUploader:
    """Duplicate a template campaign, upload creatives, and schedule for next day.

    Args:
        api_key: Outbrain Amplify API key.
        account_id: Marketer/account ID.
        template_campaign_id: ID of the campaign to clone as a template.
    """

    def __init__(
        self,
        api_key: str,
        account_id: str,
        template_campaign_id: str,
    ) -> None:
        self.account_id = account_id
        self.template_campaign_id = template_campaign_id
        self._http = httpx.Client(
            base_url=BASE_URL,
            headers={
                "OB-TOKEN-V1": api_key,
                "Content-Type": "application/json",
            },
            timeout=60,
        )

    @classmethod
    def from_env(cls) -> "OutbrainUploader":
        """Build an instance from environment variables."""
        return cls(
            api_key=_require_env("OUTBRAIN_API_KEY"),
            account_id=_require_env("OUTBRAIN_ACCOUNT_ID"),
            template_campaign_id=_require_env("OUTBRAIN_TEMPLATE_CAMPAIGN_ID"),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upload_campaign(
        self,
        ad_title: str,
        png_paths: list[Path],
        go_live_date: date | None = None,
    ) -> dict[str, Any]:
        """Duplicate template, upload PNGs, validate, and schedule.

        Args:
            ad_title: Name for the new campaign (e.g. ``NATIVE_MS_2173_STATIC_...``).
            png_paths: Local PNG files to upload as ad creatives.
            go_live_date: Date to activate the campaign (default: tomorrow).

        Returns:
            Dict with ``campaign_id``, ``campaign_name``, ``creatives``, and
            ``go_live_date`` keys.
        """
        if go_live_date is None:
            go_live_date = date.today() + timedelta(days=1)

        log.info("outbrain.upload.start", title=ad_title, pngs=len(png_paths))

        campaign = self._duplicate_campaign(ad_title)
        campaign_id = campaign["id"]

        creatives = []
        for png in png_paths:
            creative = self._upload_creative(campaign_id, png, ad_title)
            creatives.append(creative)

        self._validate_campaign(campaign_id)
        self._schedule_campaign(campaign_id, go_live_date)

        log.info(
            "outbrain.upload.done",
            campaign_id=campaign_id,
            creatives=len(creatives),
            go_live=go_live_date.isoformat(),
        )
        return {
            "campaign_id": campaign_id,
            "campaign_name": campaign.get("name", ad_title),
            "creatives": [c.get("id") for c in creatives],
            "go_live_date": go_live_date.isoformat(),
        }

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "OutbrainUploader":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _duplicate_campaign(self, new_name: str) -> dict[str, Any]:
        """Clone the template campaign with a new name."""
        resp = self._http.get(f"/marketers/{self.account_id}/campaigns/{self.template_campaign_id}")
        resp.raise_for_status()
        template = resp.json()

        payload = {
            "name": new_name,
            "budget": template.get("budget"),
            "targeting": template.get("targeting"),
            "status": "INACTIVE",  # keep inactive until scheduled
        }
        # Remove id/readonly fields before POSTing
        for key in ("id", "createdAt", "updatedAt", "statistics"):
            payload.pop(key, None)

        create_resp = self._http.post(
            f"/marketers/{self.account_id}/campaigns",
            json=payload,
        )
        create_resp.raise_for_status()
        campaign = create_resp.json()
        log.info("outbrain.campaign.created", campaign_id=campaign["id"])
        return campaign

    def _upload_creative(
        self,
        campaign_id: str,
        png_path: Path,
        ad_title: str,
    ) -> dict[str, Any]:
        """Upload a single PNG as an ad creative in the campaign."""
        with open(png_path, "rb") as f:
            image_bytes = f.read()

        # Step 1: upload image to Outbrain CDN
        img_resp = self._http.post(
            f"/marketers/{self.account_id}/images",
            content=image_bytes,
            headers={"Content-Type": "image/png"},
        )
        img_resp.raise_for_status()
        image_url = img_resp.json().get("url") or img_resp.json().get("imageUrl")

        # Step 2: create ad (promoted link) with the image
        ad_payload = {
            "url": "",  # Placeholder — board must supply landing-page URL
            "text": ad_title,
            "imageUrl": image_url,
        }
        ad_resp = self._http.post(
            f"/campaigns/{campaign_id}/promotedLinks",
            json=ad_payload,
        )
        ad_resp.raise_for_status()
        creative = ad_resp.json()
        log.info("outbrain.creative.uploaded", creative_id=creative.get("id"))
        return creative

    def _validate_campaign(self, campaign_id: str) -> None:
        """Fetch campaign and assert required fields are present and valid."""
        resp = self._http.get(f"/marketers/{self.account_id}/campaigns/{campaign_id}")
        resp.raise_for_status()
        campaign = resp.json()

        issues: list[str] = []
        if not campaign.get("budget"):
            issues.append("budget missing")
        if not campaign.get("targeting"):
            issues.append("targeting missing")
        if campaign.get("status") not in ("ACTIVE", "INACTIVE", "PENDING"):
            issues.append(f"unexpected status: {campaign.get('status')}")

        if issues:
            raise ValueError(f"Outbrain campaign {campaign_id} validation failed: {issues}")

        log.info("outbrain.campaign.validated", campaign_id=campaign_id)

    def _schedule_campaign(self, campaign_id: str, go_live_date: date) -> None:
        """Set campaign start date and activate it."""
        payload = {
            "status": "ACTIVE",
            "startDate": go_live_date.isoformat(),
        }
        resp = self._http.put(
            f"/marketers/{self.account_id}/campaigns/{campaign_id}",
            json=payload,
        )
        resp.raise_for_status()
        log.info("outbrain.campaign.scheduled", campaign_id=campaign_id, go_live=go_live_date.isoformat())


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise EnvironmentError(f"Required env var {name!r} is not set")
    return value
