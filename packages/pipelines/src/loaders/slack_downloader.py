"""CLI entry-point for the Slack native-ad file downloader.

Usage (one-shot backfill):
    python -m perf_marketing_pipelines.loaders.slack_downloader

Usage (continuous polling loop):
    python -m perf_marketing_pipelines.loaders.slack_downloader --loop

Environment variables:
    SLACK_BOT_TOKEN   (required) Bot token with channels:history + files:read
    SLACK_CHANNEL_ID  (optional) Channel to monitor; defaults to ext-sheko
    DOWNLOAD_DIR      (optional) Root download directory; defaults to downloads/slack
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import structlog
from dotenv import load_dotenv

from integrations.slack import (
    DEFAULT_POLL_INTERVAL,
    build_from_env,
)

log = structlog.get_logger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Slack file attachments from the ext-sheko channel."
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run in continuous polling mode (Ctrl-C to stop).",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL,
        metavar="SECONDS",
        help=f"Poll interval in seconds when --loop is set (default: {DEFAULT_POLL_INTERVAL}).",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=Path("downloads/slack"),
        metavar="DIR",
        help="Root directory for downloaded files (default: downloads/slack).",
    )
    parser.add_argument(
        "--oldest-ts",
        default=None,
        metavar="TS",
        help="Only fetch messages newer than this Unix timestamp string. "
        "Omit to default to the last 24 hours.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = _parse_args(argv)

    try:
        downloader = build_from_env(
            download_dir=args.download_dir,
            oldest_ts=args.oldest_ts,
        )
    except EnvironmentError as exc:
        log.error("startup.error", error=str(exc))
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.loop:
        downloader.run_loop(interval=args.interval)
    else:
        downloaded = downloader.poll_once()
        print(f"Downloaded {len(downloaded)} file(s).")

    return 0


if __name__ == "__main__":
    sys.exit(main())
