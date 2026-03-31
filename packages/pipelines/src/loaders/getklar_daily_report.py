"""GetKlar daily spend report pipeline.

Fetches yesterday's actual marketing spend from GetKlar (by channel),
compares it against target allocations from a Google Sheet template,
and posts a formatted Ist-vs-Soll table to Microsoft Teams.

Usage (one-shot, defaults to yesterday):
    python -m perf_marketing_pipelines.loaders.getklar_daily_report

Usage (specific date):
    python -m perf_marketing_pipelines.loaders.getklar_daily_report --date 2026-03-30

Environment variables:
    GETKLAR_REFRESH_TOKEN   (required) Long-lived token from Klar Frontend → Attribution API
    GETKLAR_TEMPLATE_URL    (required) Google Sheets URL for channel allocation template
    TEAMS_WEBHOOK_URL       (optional) MS Teams incoming webhook; if unset, prints to stdout
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import httpx
import structlog
from dotenv import load_dotenv

from integrations.getklar import ChannelSpend, GetKlarClient, build_from_env, yesterday

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Template loader
# ---------------------------------------------------------------------------


@dataclass
class ChannelTarget:
    """Target allocation for a single channel from the template sheet."""

    channel: str
    target_pct: float  # e.g. 28.7 for 28.7%


def _gsheet_csv_url(sheet_url: str) -> str:
    """Convert a Google Sheets /edit URL to a direct CSV export URL."""
    # Extract the spreadsheet ID from the URL.
    match = re.search(r"/spreadsheets/d/([^/]+)", sheet_url)
    if not match:
        raise ValueError(f"Cannot extract spreadsheet ID from URL: {sheet_url}")
    sheet_id = match.group(1)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"


def _parse_percent(value: str) -> float:
    """Parse German-formatted percentage string like '28,7%' → 28.7."""
    cleaned = value.strip().rstrip("%").replace(",", ".").replace("\xa0", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_euro(value: str) -> float:
    """Parse German-formatted Euro value like '373.209€' → 373209.0."""
    cleaned = (
        value.strip()
        .rstrip("€")
        .replace(".", "")  # thousands separator
        .replace(",", ".")  # decimal separator
        .replace("\xa0", "")
        .strip()
    )
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def load_targets_from_sheet(template_url: str) -> list[ChannelTarget]:
    """Download the Google Sheet template and extract target % per channel.

    The sheet is expected to have a table with columns:
        Channel | Spend (€) | % Spend | ...
    Rows ending in 'TOTAL' or blank are skipped.

    Args:
        template_url: Google Sheets URL (any variant — /edit, /view, export).

    Returns:
        List of :class:`ChannelTarget` objects, excluding the TOTAL row.
    """
    csv_url = _gsheet_csv_url(template_url)
    resp = httpx.get(csv_url, follow_redirects=True, timeout=30)
    resp.raise_for_status()

    reader = csv.reader(io.StringIO(resp.text))
    targets: list[ChannelTarget] = []
    header_found = False

    channel_col: int = -1
    pct_col: int = -1

    for row in reader:
        # Find the header row that contains "Channel" and "% Spend"
        if not header_found:
            lower = [c.lower().strip() for c in row]
            if "channel" in lower:
                channel_col = lower.index("channel")
                for i, h in enumerate(lower):
                    if "%" in h or "spend" in h and i != channel_col:
                        # First candidate is % Spend column
                        if i != channel_col:
                            # Try to find the "% Spend" column specifically
                            pass
                # Precise search for "% spend"
                for i, h in enumerate(lower):
                    if "%" in h:
                        pct_col = i
                        break
                if pct_col == -1 and len(row) > channel_col + 1:
                    pct_col = channel_col + 2  # fallback: 3rd column
                header_found = True
            continue

        if not row or not any(c.strip() for c in row):
            continue

        if channel_col >= len(row):
            continue

        channel = row[channel_col].strip()
        if not channel or channel.upper().startswith("TOTAL"):
            continue

        pct_raw = row[pct_col].strip() if pct_col < len(row) else ""
        pct = _parse_percent(pct_raw)

        targets.append(ChannelTarget(channel=channel, target_pct=pct))

    log.info("template.loaded", channels=len(targets))
    return targets


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------


@dataclass
class ReportRow:
    channel: str
    actual_spend: float
    actual_pct: float
    target_pct: float
    delta_pct: float  # actual_pct - target_pct


def build_report(
    spend_data: list[ChannelSpend],
    targets: list[ChannelTarget],
) -> list[ReportRow]:
    """Compare actual spend against target allocations.

    Channels from the template but missing in actual data are shown with 0 spend.
    """
    total_spend = sum(s.spend for s in spend_data)

    # Build lookup: normalised channel name → spend
    actual_by_channel: dict[str, float] = {}
    for s in spend_data:
        actual_by_channel[s.channel.lower().strip()] = s.spend

    rows: list[ReportRow] = []
    for t in targets:
        key = t.channel.lower().strip()
        actual_spend = actual_by_channel.get(key, 0.0)
        actual_pct = (actual_spend / total_spend * 100) if total_spend > 0 else 0.0
        delta = actual_pct - t.target_pct
        rows.append(
            ReportRow(
                channel=t.channel,
                actual_spend=actual_spend,
                actual_pct=actual_pct,
                target_pct=t.target_pct,
                delta_pct=delta,
            )
        )

    return rows


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------


def format_markdown_table(rows: list[ReportRow], report_date: date) -> str:
    """Format the Ist-vs-Soll comparison as a markdown table."""
    lines = [
        f"## Daily Spend Report — {report_date.isoformat()}",
        "",
        "| Channel | Spend (€) | Ist % | Soll % | Delta |",
        "|---------|----------:|------:|-------:|------:|",
    ]
    for r in rows:
        delta_sign = "+" if r.delta_pct >= 0 else ""
        lines.append(
            f"| {r.channel} "
            f"| {r.actual_spend:,.0f}€ "
            f"| {r.actual_pct:.1f}% "
            f"| {r.target_pct:.1f}% "
            f"| {delta_sign}{r.delta_pct:.1f}% |"
        )

    # Summary totals
    total_spend = sum(r.actual_spend for r in rows)
    lines.append(f"| **TOTAL** | **{total_spend:,.0f}€** | **100%** | **100%** | — |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Teams delivery
# ---------------------------------------------------------------------------


def send_to_teams(webhook_url: str, markdown_body: str) -> None:
    """POST the report to a Microsoft Teams incoming webhook as an Adaptive Card."""
    payload: dict[str, Any] = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": markdown_body,
                            "wrap": True,
                            "fontType": "Monospace",
                        }
                    ],
                },
            }
        ],
    }
    resp = httpx.post(webhook_url, json=payload, timeout=15)
    resp.raise_for_status()
    log.info("teams.webhook.sent", status=resp.status_code)


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


def run_pipeline(report_date: date) -> str:
    """Execute the full pipeline for *report_date*.

    Returns the formatted markdown report string.
    """
    template_url = os.environ.get("GETKLAR_TEMPLATE_URL", "")
    if not template_url:
        raise EnvironmentError("GETKLAR_TEMPLATE_URL is not set.")

    teams_webhook = os.environ.get("TEAMS_WEBHOOK_URL", "")

    # 1. Load targets from Google Sheet
    log.info("pipeline.step", step="load_targets")
    targets = load_targets_from_sheet(template_url)

    # 2. Fetch actual spend from GetKlar
    log.info("pipeline.step", step="fetch_spend", date=str(report_date))
    client = build_from_env()
    spend_data = client.fetch_spend_by_channel(report_date)

    # 3. Build comparison
    log.info("pipeline.step", step="build_report")
    rows = build_report(spend_data, targets)

    # 4. Format
    report_md = format_markdown_table(rows, report_date)

    # 5. Deliver
    if teams_webhook:
        log.info("pipeline.step", step="send_teams")
        send_to_teams(teams_webhook, report_md)
    else:
        log.info(
            "pipeline.step",
            step="teams_skipped",
            reason="TEAMS_WEBHOOK_URL not set — printing to stdout",
        )

    return report_md


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="GetKlar daily spend report: Ist vs Soll by channel."
    )
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        metavar="YYYY-MM-DD",
        help="Report date (default: yesterday).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = _parse_args(argv)
    report_date = args.date or yesterday()

    try:
        report = run_pipeline(report_date)
    except EnvironmentError as exc:
        log.error("pipeline.env_error", error=str(exc))
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except httpx.HTTPStatusError as exc:
        log.error("pipeline.http_error", status=exc.response.status_code, error=str(exc))
        print(f"HTTP error: {exc}", file=sys.stderr)
        return 1

    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
