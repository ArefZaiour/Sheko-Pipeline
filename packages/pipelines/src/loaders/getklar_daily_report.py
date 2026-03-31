"""GetKlar daily spend report pipeline.

Fetches yesterday's actual marketing spend from GetKlar (by channel),
compares it against target allocations from a Google Sheet template,
and posts a formatted Ist-vs-Soll table to Microsoft Teams.

Usage (one-shot, defaults to yesterday):
    python -m perf_marketing_pipelines.loaders.getklar_daily_report

Usage (specific date):
    python -m perf_marketing_pipelines.loaders.getklar_daily_report --date 2026-03-30

Environment variables:
    GETKLAR_API_TOKEN       (required) Long-lived JWT from Klar Frontend
    GETKLAR_TEMPLATE_URL    (required) Google Sheets URL for channel allocation template
    TEAMS_WEBHOOK_URL       (optional) MS Teams incoming webhook; if unset, prints to stdout

Scheduling (daily at 9:00 AM CET/CEST):
    0 7 * * * cd /path/to/project && python -m perf_marketing_pipelines.loaders.getklar_daily_report
    (7 UTC = 8/9 CET/CEST depending on DST)
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


def _parse_roas(value: str) -> float:
    """Parse German-formatted ROAS like '1,22x' → 1.22."""
    cleaned = value.strip().rstrip("x").replace(",", ".").replace("\xa0", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


@dataclass
class ChannelTarget:
    """Target allocation and attribution benchmarks for a single channel."""

    channel: str
    target_pct: float       # e.g. 28.7 for 28.7%
    target_spend: float     # absolute spend in €
    net_rev_mm: float       # Net Revenue (Marketing Mix) in €
    roas_mm: float          # ROAS (Marketing Mix)
    nc_orders: float        # New Customer orders
    nc_rev: float           # New Customer revenue in €
    nc_roas: float          # New Customer ROAS
    cac: float              # Customer Acquisition Cost in €
    bewertung: str          # Assessment text


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


def _find_col(headers: list[str], *candidates: str) -> int:
    """Find column index matching any candidate substring (case-insensitive)."""
    for i, h in enumerate(headers):
        for c in candidates:
            if c in h:
                return i
    return -1


def load_targets_from_sheet(template_url: str) -> list[ChannelTarget]:
    """Download the Google Sheet template and extract all columns per channel.

    Expected columns:
        Channel | Spend (€) | % Spend | Net Rev MM (€) | ROAS MM |
        NC Orders | NC Rev (€) | NC ROAS | CAC (€) | Bewertung

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
    cols: dict[str, int] = {}

    for row in reader:
        if not header_found:
            lower = [c.lower().strip() for c in row]
            if "channel" in lower:
                cols["channel"] = lower.index("channel")
                cols["spend"] = _find_col(lower, "spend (€)", "spend(€)")
                cols["pct"] = _find_col(lower, "% spend")
                cols["net_rev"] = _find_col(lower, "net rev")
                cols["roas_mm"] = _find_col(lower, "roas mm")
                cols["nc_orders"] = _find_col(lower, "nc orders")
                cols["nc_rev"] = _find_col(lower, "nc rev")
                cols["nc_roas"] = _find_col(lower, "nc roas")
                cols["cac"] = _find_col(lower, "cac")
                cols["bewertung"] = _find_col(lower, "bewertung")
                header_found = True
            continue

        if not row or not any(c.strip() for c in row):
            continue

        ch_col = cols.get("channel", -1)
        if ch_col < 0 or ch_col >= len(row):
            continue

        channel = row[ch_col].strip()
        if not channel:
            continue
        if channel.upper().startswith("TOTAL"):
            break

        def _cell(key: str) -> str:
            idx = cols.get(key, -1)
            return row[idx].strip() if 0 <= idx < len(row) else ""

        targets.append(ChannelTarget(
            channel=channel,
            target_pct=_parse_percent(_cell("pct")),
            target_spend=_parse_euro(_cell("spend")),
            net_rev_mm=_parse_euro(_cell("net_rev")),
            roas_mm=_parse_roas(_cell("roas_mm")),
            nc_orders=_parse_euro(_cell("nc_orders")),  # reuse euro parser for numbers with dots
            nc_rev=_parse_euro(_cell("nc_rev")),
            nc_roas=_parse_roas(_cell("nc_roas")),
            cac=_parse_euro(_cell("cac")),
            bewertung=_cell("bewertung"),
        ))

    log.info("template.loaded", channels=len(targets))
    return targets


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------


@dataclass
class ReportRow:
    channel: str
    # Spend comparison
    actual_spend: float
    actual_pct: float
    target_pct: float
    delta_pct: float
    # Attribution comparison (Ist from GetKlar vs Soll from Excel)
    actual_revenue: float
    actual_roas: float
    target_roas: float
    actual_orders: float
    target_nc_orders: float
    target_nc_rev: float
    target_nc_roas: float
    target_cac: float
    actual_cac: float
    bewertung: str


def _normalise_channel(name: str) -> str:
    """Normalise a channel name for fuzzy matching."""
    n = name.lower().strip()
    n = n.removeprefix("google ")
    n = n.replace(" paid search", " search")
    n = n.replace(" (tool cost)", "")
    return n.strip()


def _find_channel_spend(
    spend_data: list[ChannelSpend], channel_name: str
) -> ChannelSpend | None:
    """Find a ChannelSpend by exact or normalised name match."""
    exact_key = channel_name.lower().strip()
    norm_key = _normalise_channel(channel_name)
    for s in spend_data:
        if s.channel.lower().strip() == exact_key:
            return s
        if _normalise_channel(s.channel) == norm_key:
            return s
    return None


def build_report(
    spend_data: list[ChannelSpend],
    targets: list[ChannelTarget],
) -> list[ReportRow]:
    """Compare actual spend and attribution against targets."""
    total_spend = sum(s.spend for s in spend_data)

    rows: list[ReportRow] = []
    for t in targets:
        cs = _find_channel_spend(spend_data, t.channel)
        actual_spend = cs.spend if cs else 0.0
        actual_revenue = cs.revenue if cs else 0.0
        actual_orders = cs.orders if cs else 0.0
        actual_pct = (actual_spend / total_spend * 100) if total_spend > 0 else 0.0
        actual_roas = (actual_revenue / actual_spend) if actual_spend > 0 else 0.0
        actual_cac = (actual_spend / actual_orders) if actual_orders > 0 else 0.0
        delta = actual_pct - t.target_pct

        rows.append(
            ReportRow(
                channel=t.channel,
                actual_spend=actual_spend,
                actual_pct=actual_pct,
                target_pct=t.target_pct,
                delta_pct=delta,
                actual_revenue=actual_revenue,
                actual_roas=actual_roas,
                target_roas=t.roas_mm,
                actual_orders=actual_orders,
                target_nc_orders=t.nc_orders,
                target_nc_rev=t.net_rev_mm,
                target_nc_roas=t.nc_roas,
                target_cac=t.cac,
                actual_cac=actual_cac,
                bewertung=t.bewertung,
            )
        )

    return rows


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------


def format_markdown_table(rows: list[ReportRow], report_date: date) -> str:
    """Format the Ist-vs-Soll comparison as a markdown table."""
    total_spend = sum(r.actual_spend for r in rows)
    total_rev = sum(r.actual_revenue for r in rows)
    total_orders = sum(r.actual_orders for r in rows)
    blended_roas = (total_rev / total_spend) if total_spend > 0 else 0.0

    lines = [
        f"## SHEKO Daily Spend Report — {report_date.strftime('%d.%m.%Y')}",
        "",
        "### Spend Allocation (Ist vs Soll)",
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
    lines.append(f"| **TOTAL** | **{total_spend:,.0f}€** | **100%** | **100%** | — |")

    lines.extend([
        "",
        "### Marketing Mix Attribution (Ist vs Soll)",
        "",
        "| Channel | Revenue (€) | ROAS Ist | ROAS Soll | Orders | CAC Ist | CAC Soll |",
        "|---------|------------:|---------:|----------:|-------:|--------:|---------:|",
    ])
    for r in rows:
        lines.append(
            f"| {r.channel} "
            f"| {r.actual_revenue:,.0f}€ "
            f"| {r.actual_roas:.2f}x "
            f"| {r.target_roas:.2f}x "
            f"| {r.actual_orders:,.0f} "
            f"| {r.actual_cac:,.0f}€ "
            f"| {r.target_cac:,.0f}€ |"
        )
    lines.append(
        f"| **TOTAL** | **{total_rev:,.0f}€** | **{blended_roas:.2f}x** | — "
        f"| **{total_orders:,.0f}** | — | — |"
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Teams delivery
# ---------------------------------------------------------------------------


TEAMS_MENTIONS = [
    {"email": "dustin@sheko.com", "name": "Dustin"},
    {"email": "aref@sheko.com", "name": "Aref"},
    {"email": "dalibor@sheko.com", "name": "Dalibor"},
]


def _delta_color(delta: float) -> str:
    """Return 'good' (green), 'warning' (yellow), or 'attention' (red) based on delta severity."""
    abs_d = abs(delta)
    if abs_d <= 2.0:
        return "good"
    elif abs_d <= 5.0:
        return "warning"
    return "attention"


def _delta_icon(delta: float) -> str:
    """Return an icon for delta direction."""
    if delta > 0.5:
        return "🔺"
    elif delta < -0.5:
        return "🔻"
    return "✅"


def _roas_color(actual: float, target: float) -> str:
    """Color for ROAS comparison: green if at/above target, red if below."""
    if target <= 0:
        return "default"
    if actual >= target:
        return "good"
    if actual >= target * 0.8:
        return "warning"
    return "attention"


def _build_channel_line(r: ReportRow) -> str:
    """Format a single channel as a compact line for mobile-friendly display."""
    delta_sign = "+" if r.delta_pct >= 0 else ""
    icon = _delta_icon(r.delta_pct)
    return (
        f"**{r.channel}**\n"
        f"Spend: {r.actual_spend:,.0f}€ · Ist {r.actual_pct:.1f}% · Soll {r.target_pct:.1f}% · {icon} {delta_sign}{r.delta_pct:.1f}%"
    )


def _build_attribution_line(r: ReportRow) -> str:
    """Format a single channel's attribution data as a compact line."""
    roas_icon = "✅" if r.target_roas > 0 and r.actual_roas >= r.target_roas else "⚠️"
    return (
        f"**{r.channel}**\n"
        f"Rev: {r.actual_revenue:,.0f}€ · ROAS: {roas_icon} {r.actual_roas:.2f}x (Soll {r.target_roas:.2f}x) · "
        f"Orders: {r.actual_orders:,.0f} · CAC: {r.actual_cac:,.0f}€"
    )


def _build_adaptive_card(rows: list[ReportRow], report_date: date) -> dict[str, Any]:
    """Build a mobile-friendly Adaptive Card using single-column TextBlocks."""
    total_spend = sum(r.actual_spend for r in rows)
    total_rev = sum(r.actual_revenue for r in rows)
    total_orders = sum(r.actual_orders for r in rows)
    blended_roas = (total_rev / total_spend) if total_spend > 0 else 0.0

    body: list[dict[str, Any]] = [
        {
            "type": "TextBlock",
            "text": f"📊 SHEKO Daily Report — {report_date.strftime('%d.%m.%Y')}",
            "size": "Large",
            "weight": "Bolder",
            "wrap": True,
        },
        {
            "type": "FactSet",
            "facts": [
                {"title": "💰 Spend", "value": f"{total_spend:,.0f}€"},
                {"title": "📈 Revenue", "value": f"{total_rev:,.0f}€"},
                {"title": "🎯 ROAS", "value": f"{blended_roas:.2f}x"},
                {"title": "🛒 Orders", "value": f"{total_orders:,.0f}"},
            ],
        },
        # --- Section 1: Spend Allocation ---
        {
            "type": "TextBlock",
            "text": "━━━ **Spend Allocation (Ist vs Soll)** ━━━",
            "separator": True,
            "spacing": "Medium",
            "wrap": True,
        },
    ]

    # Each channel as a compact text block
    for r in rows:
        body.append({
            "type": "TextBlock",
            "text": _build_channel_line(r),
            "wrap": True,
            "spacing": "Small",
            "size": "Small",
        })

    # Total line
    body.append({
        "type": "TextBlock",
        "text": f"**TOTAL: {total_spend:,.0f}€**",
        "weight": "Bolder",
        "spacing": "Small",
        "size": "Small",
    })

    # --- Section 2: Marketing Mix Attribution ---
    body.append({
        "type": "TextBlock",
        "text": "━━━ **Marketing Mix Attribution (Ist vs Soll)** ━━━",
        "separator": True,
        "spacing": "Medium",
        "wrap": True,
    })

    for r in rows:
        body.append({
            "type": "TextBlock",
            "text": _build_attribution_line(r),
            "wrap": True,
            "spacing": "Small",
            "size": "Small",
        })

    body.append({
        "type": "TextBlock",
        "text": f"**TOTAL: {total_rev:,.0f}€ Rev · {blended_roas:.2f}x ROAS · {total_orders:,.0f} Orders**",
        "weight": "Bolder",
        "spacing": "Small",
        "size": "Small",
        "wrap": True,
    })

    # Highlights
    over = sorted([r for r in rows if r.delta_pct > 2.0], key=lambda r: -r.delta_pct)
    under = sorted([r for r in rows if r.delta_pct < -2.0], key=lambda r: r.delta_pct)
    low_roas = [r for r in rows if r.target_roas > 0 and r.actual_roas < r.target_roas * 0.8]

    highlights = []
    for r in over[:3]:
        highlights.append(f"🔺 **{r.channel}** Spend über Soll (+{r.delta_pct:.1f}%)")
    for r in under[:3]:
        highlights.append(f"🔻 **{r.channel}** Spend unter Soll ({r.delta_pct:.1f}%)")
    for r in low_roas[:3]:
        highlights.append(f"⚠️ **{r.channel}** ROAS {r.actual_roas:.2f}x vs Soll {r.target_roas:.2f}x")

    if highlights:
        body.append({
            "type": "TextBlock",
            "text": "**⚡ Auffälligkeiten:**\n" + "\n".join(f"- {h}" for h in highlights),
            "wrap": True,
            "separator": True,
            "spacing": "Medium",
        })

    # Mentions
    mention_names = ", ".join(f"<at>{m['name']}</at>" for m in TEAMS_MENTIONS)
    body.append({
        "type": "TextBlock",
        "text": f"cc {mention_names}",
        "spacing": "Medium",
        "size": "Small",
        "isSubtle": True,
        "wrap": True,
    })

    mentions = [
        {
            "type": "mention",
            "text": f"<at>{m['name']}</at>",
            "mentioned": {"id": m["email"], "name": m["name"]},
        }
        for m in TEAMS_MENTIONS
    ]

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
        "msteams": {"entities": mentions},
    }


def send_to_teams(webhook_url: str, rows: list[ReportRow], report_date: date) -> None:
    """POST the report to a Microsoft Teams incoming webhook as a rich Adaptive Card."""
    card = _build_adaptive_card(rows, report_date)
    payload: dict[str, Any] = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": card,
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
        send_to_teams(teams_webhook, rows, report_date)
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
