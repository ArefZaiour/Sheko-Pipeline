"""Orchestrates native ad campaign upload across Outbrain and Taboola.

High-level flow for each platform:
  1. Duplicate the template campaign (scheduled for next day).
  2. For each PNG in the source folder, upload as a creative.
  3. Validate campaign settings and log any warnings.

Both platforms are optional — set ``skip_outbrain`` or ``skip_taboola`` to
bypass one while developing/testing.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)


@dataclass
class UploadResult:
    platform: str
    campaign_id: str
    campaign_name: str
    creatives_uploaded: int
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


def _collect_images(folder: Path) -> list[Path]:
    """Return all PNG/JPEG files in ``folder``, sorted by name."""
    images = sorted(
        p for p in folder.iterdir() if p.suffix.lower() in {".png", ".jpg", ".jpeg"}
    )
    if not images:
        raise FileNotFoundError(f"No PNG/JPEG images found in {folder}")
    log.info("uploader.images.found", folder=str(folder), count=len(images))
    return images


def upload_to_outbrain(
    images: list[Path],
    campaign_name: str,
    landing_url: str,
    start_date: date | None = None,
    template_campaign_id: str | None = None,
) -> UploadResult:
    """Duplicate the Outbrain template campaign and upload all images as creatives.

    Args:
        images:               List of PNG/JPEG paths to upload.
        campaign_name:        Name for the new campaign.
        landing_url:          Click-through destination URL for all creatives.
        start_date:           Schedule date; defaults to tomorrow.
        template_campaign_id: Overrides ``OUTBRAIN_TEMPLATE_CAMPAIGN_ID`` env var.
    """
    from native_ads.outbrain import build_from_env as ob_client  # lazy import

    result = UploadResult(
        platform="outbrain",
        campaign_id="",
        campaign_name=campaign_name,
        creatives_uploaded=0,
    )
    try:
        client = ob_client()
        tpl_id = template_campaign_id or os.environ.get("OUTBRAIN_TEMPLATE_CAMPAIGN_ID", "")
        if not tpl_id:
            result.errors.append("OUTBRAIN_TEMPLATE_CAMPAIGN_ID is not set.")
            return result

        campaign = client.duplicate_campaign(tpl_id, campaign_name, start_date)
        result.campaign_id = campaign.id
        result.warnings.extend(client.validate_campaign(campaign))

        for image in images:
            try:
                client.create_creative(
                    campaign_id=campaign.id,
                    title=image.stem.replace("_", " ").replace("-", " ").title(),
                    landing_url=landing_url,
                    image_path=image,
                )
                result.creatives_uploaded += 1
            except Exception as exc:
                result.errors.append(f"Creative upload failed for {image.name}: {exc}")

    except Exception as exc:
        result.errors.append(f"Outbrain campaign setup failed: {exc}")

    return result


def upload_to_taboola(
    images: list[Path],
    campaign_name: str,
    landing_url: str,
    start_date: date | None = None,
    template_campaign_id: str | None = None,
) -> UploadResult:
    """Duplicate the Taboola template campaign and upload all images as creatives.

    Args:
        images:               List of PNG/JPEG paths to upload.
        campaign_name:        Name for the new campaign.
        landing_url:          Click-through destination URL for all creatives.
        start_date:           Schedule date; defaults to tomorrow.
        template_campaign_id: Overrides ``TABOOLA_TEMPLATE_CAMPAIGN_ID`` env var.
    """
    from native_ads.taboola import build_from_env as tb_client  # lazy import

    result = UploadResult(
        platform="taboola",
        campaign_id="",
        campaign_name=campaign_name,
        creatives_uploaded=0,
    )
    try:
        client = tb_client()
        tpl_id = template_campaign_id or os.environ.get("TABOOLA_TEMPLATE_CAMPAIGN_ID", "")
        if not tpl_id:
            result.errors.append("TABOOLA_TEMPLATE_CAMPAIGN_ID is not set.")
            return result

        campaign = client.duplicate_campaign(tpl_id, campaign_name, start_date)
        result.campaign_id = campaign.id
        result.warnings.extend(client.validate_campaign(campaign))

        for image in images:
            try:
                client.create_creative(
                    campaign_id=campaign.id,
                    title=image.stem.replace("_", " ").replace("-", " ").title(),
                    landing_url=landing_url,
                    image_path=image,
                )
                result.creatives_uploaded += 1
            except Exception as exc:
                result.errors.append(f"Creative upload failed for {image.name}: {exc}")

    except Exception as exc:
        result.errors.append(f"Taboola campaign setup failed: {exc}")

    return result


def run_upload(
    folder: Path,
    campaign_name: str,
    landing_url: str,
    start_date: date | None = None,
    skip_outbrain: bool = False,
    skip_taboola: bool = False,
    outbrain_template_id: str | None = None,
    taboola_template_id: str | None = None,
) -> list[UploadResult]:
    """Run the full upload pipeline for the given image folder.

    Returns a list of :class:`UploadResult` — one per platform that was attempted.
    """
    images = _collect_images(folder)
    results: list[UploadResult] = []

    if not skip_outbrain:
        log.info("uploader.outbrain.start", campaign=campaign_name, images=len(images))
        ob_result = upload_to_outbrain(
            images,
            campaign_name=campaign_name,
            landing_url=landing_url,
            start_date=start_date,
            template_campaign_id=outbrain_template_id,
        )
        results.append(ob_result)
        if ob_result.success:
            log.info(
                "uploader.outbrain.done",
                campaign_id=ob_result.campaign_id,
                creatives=ob_result.creatives_uploaded,
                warnings=ob_result.warnings,
            )
        else:
            log.error("uploader.outbrain.failed", errors=ob_result.errors)

    if not skip_taboola:
        log.info("uploader.taboola.start", campaign=campaign_name, images=len(images))
        tb_result = upload_to_taboola(
            images,
            campaign_name=campaign_name,
            landing_url=landing_url,
            start_date=start_date,
            template_campaign_id=taboola_template_id,
        )
        results.append(tb_result)
        if tb_result.success:
            log.info(
                "uploader.taboola.done",
                campaign_id=tb_result.campaign_id,
                creatives=tb_result.creatives_uploaded,
                warnings=tb_result.warnings,
            )
        else:
            log.error("uploader.taboola.failed", errors=tb_result.errors)

    return results
