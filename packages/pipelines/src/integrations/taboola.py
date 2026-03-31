"""Taboola Backstage API client — duplicate campaign and upload creatives.

Docs: https://developers.taboola.com/backstage-api/reference

Required env vars:
    TABOOLA_CLIENT_ID          — OAuth2 client ID
    TABOOLA_CLIENT_SECRET      — OAuth2 client secret
    TABOOLA_ACCOUNT_ID         — Publisher/advertiser account name (e.g. ``sheko-gmbh``)
    TABOOLA_TEMPLATE_CAMPAIGN_ID — Campaign ID to clone as template

Taboola uses OAuth2 client_credentials flow.  The token endpoint returns an
``access_token`` valid for 43200 seconds (12 h); we fetch it once per session.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

TOKEN_URL = "https://authentication.taboola.com/calibrate/oauth/token"
BASE_URL = "https://backstage.taboola.com/backstage/api/1.0"


class TaboolaUploader:
    """Duplicate a template campaign, upload creatives, and schedule for next day.

    Args:
        client_id: OAuth2 client ID.
        client_secret: OAuth2 client secret.
        account_id: Taboola account name (string, not numeric ID).
        template_campaign_id: ID of the campaign to clone as a template.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        account_id: str,
        template_campaign_id: str,
    ) -> None:
        self.account_id = account_id
        self.template_campaign_id = template_campaign_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._http = httpx.Client(base_url=BASE_URL, timeout=60)
        self._access_token: str | None = None

    @classmethod
    def from_env(cls) -> "TaboolaUploader":
        """Build an instance from environment variables."""
        return cls(
            client_id=_require_env("TABOOLA_CLIENT_ID"),
            client_secret=_require_env("TABOOLA_CLIENT_SECRET"),
            account_id=_require_env("TABOOLA_ACCOUNT_ID"),
            template_campaign_id=_require_env("TABOOLA_TEMPLATE_CAMPAIGN_ID"),
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
            ad_title: Name for the new campaign.
            png_paths: Local PNG files to upload as ad creatives.
            go_live_date: Date to activate the campaign (default: tomorrow).

        Returns:
            Dict with ``campaign_id``, ``campaign_name``, ``creatives``, and
            ``go_live_date`` keys.
        """
        if go_live_date is None:
            go_live_date = date.today() + timedelta(days=1)

        self._ensure_token()

        log.info("taboola.upload.start", title=ad_title, pngs=len(png_paths))

        campaign = self._duplicate_campaign(ad_title)
        campaign_id = str(campaign["id"])

        creatives = []
        for png in png_paths:
            creative = self._upload_creative(campaign_id, png, ad_title)
            creatives.append(creative)

        self._validate_campaign(campaign_id)
        self._schedule_campaign(campaign_id, go_live_date)

        log.info(
            "taboola.upload.done",
            campaign_id=campaign_id,
            creatives=len(creatives),
            go_live=go_live_date.isoformat(),
        )
        return {
            "campaign_id": campaign_id,
            "campaign_name": campaign.get("name", ad_title),
            "creatives": [str(c.get("id")) for c in creatives],
            "go_live_date": go_live_date.isoformat(),
        }

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "TaboolaUploader":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_token(self) -> None:
        if self._access_token:
            return
        resp = httpx.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        self._access_token = resp.json()["access_token"]
        self._http.headers["Authorization"] = f"Bearer {self._access_token}"
        log.info("taboola.auth.ok")

    def _duplicate_campaign(self, new_name: str) -> dict[str, Any]:
        """Fetch template and POST a new campaign with same settings."""
        resp = self._http.get(
            f"/{self.account_id}/campaigns/{self.template_campaign_id}"
        )
        resp.raise_for_status()
        template = resp.json()

        # Carry over targeting and budget; reset lifecycle fields
        payload: dict[str, Any] = {
            "name": new_name,
            "branding_text": template.get("branding_text", ""),
            "cpc": template.get("cpc"),
            "daily_cap": template.get("daily_cap"),
            "spending_limit": template.get("spending_limit"),
            "spending_limit_model": template.get("spending_limit_model"),
            "country_targeting": template.get("country_targeting"),
            "platform_targeting": template.get("platform_targeting"),
            "start_date": None,  # set later by _schedule_campaign
            "status": "PAUSED",
        }
        # Drop None values — Taboola rejects explicit nulls for some fields
        payload = {k: v for k, v in payload.items() if v is not None}

        create_resp = self._http.post(
            f"/{self.account_id}/campaigns/",
            json=payload,
        )
        create_resp.raise_for_status()
        campaign = create_resp.json()
        log.info("taboola.campaign.created", campaign_id=campaign["id"])
        return campaign

    def _upload_creative(
        self,
        campaign_id: str,
        png_path: Path,
        ad_title: str,
    ) -> dict[str, Any]:
        """Upload a PNG and create a campaign item (creative)."""
        # Step 1: upload image to Taboola media service
        with open(png_path, "rb") as f:
            img_resp = self._http.post(
                f"/{self.account_id}/campaigns/{campaign_id}/items/",
                files={"file": (png_path.name, f, "image/png")},
            )
        img_resp.raise_for_status()
        uploaded = img_resp.json()

        # Step 2: if needed, create the item using the returned URL
        if "thumbnail_url" in uploaded:
            item_payload = {
                "type": "ITEM",
                "title": ad_title,
                "url": "",  # Placeholder — board must supply landing-page URL
                "thumbnail_url": uploaded["thumbnail_url"],
            }
            item_resp = self._http.post(
                f"/{self.account_id}/campaigns/{campaign_id}/items/",
                json=item_payload,
            )
            item_resp.raise_for_status()
            creative = item_resp.json()
        else:
            creative = uploaded

        log.info("taboola.creative.uploaded", creative_id=creative.get("id"))
        return creative

    def _validate_campaign(self, campaign_id: str) -> None:
        """Assert required fields are present and valid."""
        resp = self._http.get(f"/{self.account_id}/campaigns/{campaign_id}")
        resp.raise_for_status()
        campaign = resp.json()

        issues: list[str] = []
        if not campaign.get("cpc"):
            issues.append("cpc (bid) missing")
        if not campaign.get("daily_cap") and not campaign.get("spending_limit"):
            issues.append("no budget (daily_cap or spending_limit) set")
        if not campaign.get("country_targeting"):
            issues.append("country_targeting missing")
        if campaign.get("status") not in ("RUNNING", "PAUSED", "PENDING_APPROVAL"):
            issues.append(f"unexpected status: {campaign.get('status')}")

        if issues:
            raise ValueError(f"Taboola campaign {campaign_id} validation failed: {issues}")

        log.info("taboola.campaign.validated", campaign_id=campaign_id)

    def _schedule_campaign(self, campaign_id: str, go_live_date: date) -> None:
        """Set start date and activate the campaign."""
        payload = {
            "start_date": go_live_date.isoformat(),
            "status": "RUNNING",
        }
        resp = self._http.put(
            f"/{self.account_id}/campaigns/{campaign_id}",
            json=payload,
        )
        resp.raise_for_status()
        log.info("taboola.campaign.scheduled", campaign_id=campaign_id, go_live=go_live_date.isoformat())


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise EnvironmentError(f"Required env var {name!r} is not set")
    return value
