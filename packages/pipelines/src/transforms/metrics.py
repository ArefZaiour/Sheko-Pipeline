"""Campaign metrics transforms and derived-metric calculations.

Converts raw ad-platform dicts into normalised CampaignMetrics records
and computes derived KPIs (CTR, CPC, CPM, ROAS, CPA) used by reports
and the dashboard API.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass
class CampaignMetrics:
    """Normalised campaign performance row (matches the Postgres schema + derived KPIs)."""

    date: str  # ISO 8601 date string
    platform: str  # "google_ads" | "meta"
    account_id: str  # UUID of the ad_accounts row
    campaign_id: str
    campaign_name: str
    impressions: int
    clicks: int
    spend_usd: float
    conversions: int
    revenue_usd: float

    # Derived — populated by compute_derived()
    ctr: float = 0.0  # clicks / impressions  (0–1)
    cpc_usd: float = 0.0  # spend / clicks
    cpm_usd: float = 0.0  # spend / (impressions / 1000)
    roas: float = 0.0  # revenue / spend
    cpa_usd: float = 0.0  # spend / conversions

    def compute_derived(self) -> CampaignMetrics:
        """Compute and set all derived KPIs in-place; returns self."""
        self.ctr = self.clicks / self.impressions if self.impressions else 0.0
        self.cpc_usd = self.spend_usd / self.clicks if self.clicks else 0.0
        self.cpm_usd = (self.spend_usd / self.impressions * 1000) if self.impressions else 0.0
        self.roas = self.revenue_usd / self.spend_usd if self.spend_usd else 0.0
        self.cpa_usd = self.spend_usd / self.conversions if self.conversions else 0.0
        return self


@dataclass
class PacingStatus:
    """Budget pacing snapshot with on-pace / over / under classification."""

    platform: str
    account_id: str
    campaign_id: str
    campaign_name: str
    budget_usd: float  # Daily budget
    spend_today_usd: float
    day_elapsed_pct: float  # 0–100 fraction of the day elapsed
    pacing_pct: float  # spend_today / (budget * day_elapsed_pct) * 100
    status: str  # "on_pace" | "underpacing" | "overpacing" | "no_budget"

    _UNDER_THRESHOLD: float = field(default=80.0, init=False, repr=False)
    _OVER_THRESHOLD: float = field(default=120.0, init=False, repr=False)


def normalize_google_ads_row(
    row: dict[str, Any],
    account_uuid: str,
) -> CampaignMetrics:
    """Convert a Google Ads metrics dict to a :class:`CampaignMetrics`.

    Args:
        row: Dict from :meth:`GoogleAdsClient.fetch_campaign_metrics`.
        account_uuid: The ad_accounts.id UUID for this account.
    """
    m = CampaignMetrics(
        date=row["date"],
        platform="google_ads",
        account_id=account_uuid,
        campaign_id=str(row["campaign_id"]),
        campaign_name=row["campaign_name"],
        impressions=int(row.get("impressions", 0) or 0),
        clicks=int(row.get("clicks", 0) or 0),
        spend_usd=float(row.get("spend_usd", 0) or 0),
        conversions=int(row.get("conversions", 0) or 0),
        revenue_usd=float(row.get("revenue_usd", 0) or 0),
    )
    return m.compute_derived()


def normalize_meta_row(
    row: dict[str, Any],
    account_uuid: str,
) -> CampaignMetrics:
    """Convert a Meta Ads insights dict to a :class:`CampaignMetrics`.

    Args:
        row: Dict from :meth:`MetaAdsClient.fetch_campaign_metrics`.
        account_uuid: The ad_accounts.id UUID for this account.
    """
    row_date = row.get("date_start") or row.get("date_stop") or date.today().isoformat()
    m = CampaignMetrics(
        date=row_date,
        platform="meta",
        account_id=account_uuid,
        campaign_id=str(row.get("campaign_id", "")),
        campaign_name=row.get("campaign_name", ""),
        impressions=int(row.get("impressions", 0) or 0),
        clicks=int(row.get("clicks", 0) or 0),
        spend_usd=float(row.get("spend_usd", 0) or 0),
        conversions=int(row.get("conversions", 0) or 0),
        revenue_usd=float(row.get("revenue_usd", 0) or 0),
    )
    return m.compute_derived()


def aggregate_by_campaign(metrics: list[CampaignMetrics]) -> list[CampaignMetrics]:
    """Aggregate a list of daily metric rows into one row per campaign.

    Sums impressions, clicks, spend, conversions, revenue across all dates
    and recomputes derived KPIs on the aggregated totals.  The `date` field
    is set to the latest date in each group.
    """
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for m in metrics:
        key = (m.platform, m.account_id, m.campaign_id)
        if key not in grouped:
            grouped[key] = {
                "platform": m.platform,
                "account_id": m.account_id,
                "campaign_id": m.campaign_id,
                "campaign_name": m.campaign_name,
                "date": m.date,
                "impressions": 0,
                "clicks": 0,
                "spend_usd": 0.0,
                "conversions": 0,
                "revenue_usd": 0.0,
            }
        g = grouped[key]
        g["date"] = max(g["date"], m.date)  # keep latest date
        g["impressions"] += m.impressions
        g["clicks"] += m.clicks
        g["spend_usd"] += m.spend_usd
        g["conversions"] += m.conversions
        g["revenue_usd"] += m.revenue_usd

    result = []
    for g in grouped.values():
        agg = CampaignMetrics(**{k: v for k, v in g.items()})
        result.append(agg.compute_derived())
    return result


def calculate_pacing(
    campaign_id: str,
    campaign_name: str,
    platform: str,
    account_id: str,
    daily_budget_usd: float,
    spend_today_usd: float,
    day_elapsed_pct: float,
) -> PacingStatus:
    """Compute pacing status for a single campaign.

    Args:
        day_elapsed_pct: Fraction of the day elapsed, expressed as 0–100.
                         Pass 0 to indicate start of day, 100 for end.
    """
    if daily_budget_usd <= 0:
        return PacingStatus(
            platform=platform,
            account_id=account_id,
            campaign_id=campaign_id,
            campaign_name=campaign_name,
            budget_usd=daily_budget_usd,
            spend_today_usd=spend_today_usd,
            day_elapsed_pct=day_elapsed_pct,
            pacing_pct=0.0,
            status="no_budget",
        )

    expected_spend = daily_budget_usd * (day_elapsed_pct / 100)
    pacing_pct = (spend_today_usd / expected_spend * 100) if expected_spend > 0 else 0.0

    if pacing_pct >= 120:
        status = "overpacing"
    elif pacing_pct <= 80:
        status = "underpacing"
    else:
        status = "on_pace"

    return PacingStatus(
        platform=platform,
        account_id=account_id,
        campaign_id=campaign_id,
        campaign_name=campaign_name,
        budget_usd=daily_budget_usd,
        spend_today_usd=spend_today_usd,
        day_elapsed_pct=day_elapsed_pct,
        pacing_pct=pacing_pct,
        status=status,
    )
