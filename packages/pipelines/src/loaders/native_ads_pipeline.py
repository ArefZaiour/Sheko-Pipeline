"""End-to-end Native Ads pipeline: Slack → Dropbox → Outbrain/Taboola.

Flow:
  1. Poll the ext-sheko Slack channel for NATIVE_MS ad packages.
  2. For each new package, download PNGs from the attached Dropbox folder.
  3. Upload each package to Outbrain and/or Taboola as a new campaign.
  4. Print a summary of all uploads.

Usage:
    python -m loaders.native_ads_pipeline [--platform outbrain|taboola|all]

Environment variables:
    SLACK_BOT_TOKEN            — Bot token with channels:history scope (required)
    SLACK_CHANNEL_ID           — Channel to poll (default: C09FCDHFGCU)
    NATIVE_ADS_LANDING_URL     — Click-through URL applied to all creatives (required)
    OUTBRAIN_API_KEY           — Outbrain Amplify API key
    OUTBRAIN_ACCOUNT_ID        — Outbrain marketer account ID
    OUTBRAIN_TEMPLATE_CAMPAIGN_ID — Campaign to clone as template
    TABOOLA_CLIENT_ID          — Taboola Backstage OAuth2 client ID
    TABOOLA_CLIENT_SECRET      — Taboola Backstage OAuth2 client secret
    TABOOLA_ACCOUNT_ID         — Taboola account ID
    TABOOLA_TEMPLATE_CAMPAIGN_ID — Campaign to clone as template
    PIPELINE_DOWNLOAD_DIR      — Local root for downloaded files (default: downloads/slack)
    PIPELINE_OLDEST_TS         — Only process messages newer than this Unix timestamp
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import structlog
from dotenv import load_dotenv

from integrations.slack import build_from_env as build_slack_monitor
from native_ads.uploader import UploadResult, run_upload

log = structlog.get_logger(__name__)


@dataclass
class PackageResult:
    """Result for one NATIVE_MS package (one Slack attachment)."""

    package_name: str
    folder: Path
    images_downloaded: int
    upload_results: list[UploadResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return bool(self.upload_results) and all(r.success for r in self.upload_results)

    @property
    def errors(self) -> list[str]:
        errs = []
        for r in self.upload_results:
            errs.extend(r.errors)
        return errs


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="native_ads_pipeline",
        description="Poll Slack for NATIVE_MS packages, download, and upload to Outbrain/Taboola.",
    )
    p.add_argument(
        "--platform",
        choices=["outbrain", "taboola", "all"],
        default="all",
        help="Target platform(s) (default: all).",
    )
    p.add_argument(
        "--download-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Root directory for downloaded images (default: PIPELINE_DOWNLOAD_DIR env or downloads/slack).",
    )
    p.add_argument(
        "--oldest-ts",
        default=None,
        metavar="TS",
        help="Only process messages newer than this Unix timestamp string.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Download images but skip the upload step.",
    )
    return p.parse_args(argv)


def run_pipeline(
    *,
    platform: str = "all",
    download_dir: Path | None = None,
    oldest_ts: str | None = None,
    landing_url: str | None = None,
    dry_run: bool = False,
) -> list[PackageResult]:
    """Run the full Slack → download → upload pipeline once.

    Args:
        platform: "outbrain", "taboola", or "all".
        download_dir: Root directory for downloaded images.
        oldest_ts: Only process messages newer than this Unix timestamp.
        landing_url: Click-through URL for all creatives.  Falls back to
            the ``NATIVE_ADS_LANDING_URL`` environment variable.
        dry_run: If True, download images but skip the upload step.

    Returns:
        One :class:`PackageResult` per NATIVE_MS package found.
    """
    if landing_url is None:
        landing_url = os.environ.get("NATIVE_ADS_LANDING_URL", "")
    if not landing_url and not dry_run:
        raise EnvironmentError(
            "NATIVE_ADS_LANDING_URL is not set. "
            "Provide it as an env var or pass landing_url= to run_pipeline()."
        )

    effective_download_dir = download_dir or Path(
        os.environ.get("PIPELINE_DOWNLOAD_DIR", "downloads/slack")
    )
    effective_oldest_ts = oldest_ts or os.environ.get("PIPELINE_OLDEST_TS")

    # Step 1: Poll Slack and download new packages.
    monitor = build_slack_monitor(
        download_dir=effective_download_dir,
        oldest_ts=effective_oldest_ts,
    )
    downloaded_paths = monitor.poll_once()

    if not downloaded_paths:
        log.info("pipeline.no_new_packages")
        return []

    # Group downloaded paths by their parent folder (one folder per NATIVE_MS package).
    folders: dict[Path, list[Path]] = {}
    for p in downloaded_paths:
        folders.setdefault(p.parent, []).append(p)

    log.info("pipeline.packages_found", count=len(folders))

    results: list[PackageResult] = []
    for folder, images in folders.items():
        package_name = folder.name  # e.g. "NATIVE_MS_2173_STATIC_..."
        log.info("pipeline.package.start", name=package_name, images=len(images))

        if dry_run:
            log.info("pipeline.package.dry_run", name=package_name)
            results.append(
                PackageResult(
                    package_name=package_name,
                    folder=folder,
                    images_downloaded=len(images),
                )
            )
            continue

        # Step 2: Upload to Outbrain / Taboola.
        upload_results = run_upload(
            folder=folder,
            campaign_name=package_name,
            landing_url=landing_url,
            skip_outbrain=(platform == "taboola"),
            skip_taboola=(platform == "outbrain"),
        )

        pkg = PackageResult(
            package_name=package_name,
            folder=folder,
            images_downloaded=len(images),
            upload_results=upload_results,
        )
        results.append(pkg)

        if pkg.success:
            log.info("pipeline.package.done", name=package_name)
        else:
            log.error("pipeline.package.failed", name=package_name, errors=pkg.errors)

    return results


def _print_summary(results: list[PackageResult]) -> None:
    if not results:
        print("No new NATIVE_MS packages found.")
        return

    print(f"\n{'='*60}")
    print(f"Native Ads Pipeline — {len(results)} package(s) processed")
    print(f"{'='*60}")
    for pkg in results:
        status = "OK" if pkg.success else ("DRY-RUN" if not pkg.upload_results else "FAILED")
        print(f"\n  [{status}] {pkg.package_name}")
        print(f"    Images downloaded: {pkg.images_downloaded}")
        for r in pkg.upload_results:
            plat_ok = "OK" if r.success else "FAILED"
            print(f"    {r.platform:10} [{plat_ok}]  campaign={r.campaign_id}  creatives={r.creatives_uploaded}")
            for err in r.errors:
                print(f"      ERROR: {err}")
            for w in r.warnings:
                print(f"      WARN:  {w}")
    print(f"\n{'='*60}\n")


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = _parse_args(argv)

    try:
        results = run_pipeline(
            platform=args.platform,
            download_dir=args.download_dir,
            oldest_ts=args.oldest_ts,
            dry_run=args.dry_run,
        )
    except EnvironmentError as exc:
        log.error("pipeline.startup_error", error=str(exc))
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        log.error("pipeline.unexpected_error", error=str(exc))
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1

    _print_summary(results)

    failed = [r for r in results if r.upload_results and not r.success]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
