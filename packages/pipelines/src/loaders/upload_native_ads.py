"""CLI: upload native-ad creatives from Dropbox to Outbrain and Taboola.

Usage:
    # Download PNGs from Slack/Dropbox first (Phase 1 — GRO-8), then:
    python -m loaders.upload_native_ads --png-dir downloads/slack/2026-04-01/NATIVE_MS_2173 --title NATIVE_MS_2173

    # Or pass a Dropbox URL directly (download + upload in one step):
    python -m loaders.upload_native_ads --dropbox-url "https://www.dropbox.com/sh/..." --title NATIVE_MS_2173

    # Limit to a single platform:
    python -m loaders.upload_native_ads --png-dir ./pngs --title NATIVE_MS_2173 --platform outbrain

Required env vars (see .env.example):
    OUTBRAIN_API_KEY, OUTBRAIN_ACCOUNT_ID, OUTBRAIN_TEMPLATE_CAMPAIGN_ID
    TABOOLA_CLIENT_ID, TABOOLA_CLIENT_SECRET, TABOOLA_ACCOUNT_ID, TABOOLA_TEMPLATE_CAMPAIGN_ID
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import structlog
from dotenv import load_dotenv

load_dotenv()

log = structlog.get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload NATIVE_MS creatives to Outbrain and/or Taboola"
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--png-dir", type=Path, help="Directory of PNG files to upload")
    source.add_argument(
        "--dropbox-url",
        help="Dropbox shared-folder URL; downloads PNGs to a temp dir first",
    )
    parser.add_argument("--title", required=True, help="Campaign name / ad title")
    parser.add_argument(
        "--platform",
        choices=["outbrain", "taboola", "both"],
        default="both",
        help="Which platform(s) to upload to (default: both)",
    )
    parser.add_argument(
        "--go-live",
        help="Go-live date in YYYY-MM-DD format (default: tomorrow)",
    )
    return parser.parse_args()


def _load_pngs_from_dir(png_dir: Path) -> list[Path]:
    pngs = sorted(png_dir.glob("*.png"))
    if not pngs:
        log.error("upload.no_pngs", directory=str(png_dir))
        sys.exit(1)
    return pngs


def _download_pngs_from_dropbox(dropbox_url: str, title: str) -> list[Path]:
    import tempfile

    from integrations.dropbox import download_normal_pngs

    dest = Path(tempfile.mkdtemp()) / title
    dest.mkdir(parents=True, exist_ok=True)
    pngs = download_normal_pngs(dropbox_url, dest)
    if not pngs:
        log.error("upload.dropbox.no_pngs", url=dropbox_url[:80])
        sys.exit(1)
    return pngs


def main() -> None:
    args = _parse_args()

    # Resolve PNGs
    if args.png_dir:
        pngs = _load_pngs_from_dir(args.png_dir)
    else:
        pngs = _download_pngs_from_dropbox(args.dropbox_url, args.title)

    log.info("upload.pngs_ready", count=len(pngs), title=args.title)

    # Parse go-live date
    go_live = None
    if args.go_live:
        from datetime import date
        go_live = date.fromisoformat(args.go_live)

    results: dict[str, object] = {}

    if args.platform in ("outbrain", "both"):
        from integrations.outbrain import OutbrainUploader

        with OutbrainUploader.from_env() as ob:
            result = ob.upload_campaign(args.title, pngs, go_live)
        results["outbrain"] = result
        log.info("upload.outbrain.done", **result)

    if args.platform in ("taboola", "both"):
        from integrations.taboola import TaboolaUploader

        with TaboolaUploader.from_env() as tb:
            result = tb.upload_campaign(args.title, pngs, go_live)
        results["taboola"] = result
        log.info("upload.taboola.done", **result)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
