"""CLI entry point for the native ads campaign uploader.

Usage:
    python -m native_ads.cli \\
        --folder /path/to/pngs \\
        --name "NATIVE_MS_2026-10-08" \\
        --url "https://example.com/landing" \\
        [--platform outbrain|taboola|all] \\
        [--start-date 2026-10-08] \\
        [--outbrain-template CAMPAIGN_ID] \\
        [--taboola-template CAMPAIGN_ID]

Exit codes:
    0  All attempted platforms succeeded (warnings are non-fatal).
    1  At least one platform failed.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import structlog
from dotenv import load_dotenv

log = structlog.get_logger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="native_ads",
        description="Upload native ad creatives to Outbrain and/or Taboola.",
    )
    parser.add_argument(
        "--folder",
        required=True,
        type=Path,
        help="Directory containing PNG/JPEG creative images.",
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Campaign name for the newly created campaigns.",
    )
    parser.add_argument(
        "--url",
        required=True,
        help="Click-through landing page URL applied to all creatives.",
    )
    parser.add_argument(
        "--platform",
        choices=["outbrain", "taboola", "all"],
        default="all",
        help="Which platform(s) to upload to (default: all).",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=None,
        help="Campaign launch date (ISO format, e.g. 2026-10-08). Defaults to tomorrow.",
    )
    parser.add_argument(
        "--outbrain-template",
        default=None,
        help="Outbrain template campaign ID. Overrides OUTBRAIN_TEMPLATE_CAMPAIGN_ID.",
    )
    parser.add_argument(
        "--taboola-template",
        default=None,
        help="Taboola template campaign ID. Overrides TABOOLA_TEMPLATE_CAMPAIGN_ID.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = _parse_args(argv)

    if not args.folder.is_dir():
        print(f"ERROR: --folder {args.folder} is not a directory.", file=sys.stderr)
        return 1

    from native_ads.uploader import run_upload

    results = run_upload(
        folder=args.folder,
        campaign_name=args.name,
        landing_url=args.url,
        start_date=args.start_date,
        skip_outbrain=(args.platform == "taboola"),
        skip_taboola=(args.platform == "outbrain"),
        outbrain_template_id=args.outbrain_template,
        taboola_template_id=args.taboola_template,
    )

    print("\n=== Upload Summary ===")
    overall_success = True
    for result in results:
        status = "OK" if result.success else "FAILED"
        print(f"\n[{result.platform.upper()}] {status}")
        print(f"  Campaign:   {result.campaign_name} (ID: {result.campaign_id or 'n/a'})")
        print(f"  Creatives:  {result.creatives_uploaded} uploaded")
        if result.warnings:
            print("  Warnings:")
            for w in result.warnings:
                print(f"    - {w}")
        if result.errors:
            overall_success = False
            print("  Errors:")
            for e in result.errors:
                print(f"    - {e}")

    return 0 if overall_success else 1


if __name__ == "__main__":
    sys.exit(main())
