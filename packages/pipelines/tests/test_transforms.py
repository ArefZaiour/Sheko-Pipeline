"""Tests for campaign metrics transforms and derived-metric calculations."""
from __future__ import annotations

import pytest

from transforms.metrics import (
    CampaignMetrics,
    PacingStatus,
    aggregate_by_campaign,
    calculate_pacing,
    normalize_google_ads_row,
    normalize_meta_row,
)


# ---------------------------------------------------------------------------
# CampaignMetrics.compute_derived
# ---------------------------------------------------------------------------


def test_compute_derived_calculates_ctr() -> None:
    m = CampaignMetrics(
        date="2026-10-06", platform="google_ads", account_id="acc",
        campaign_id="1", campaign_name="C",
        impressions=10000, clicks=200, spend_usd=100.0, conversions=10, revenue_usd=500.0,
    )
    m.compute_derived()
    assert m.ctr == pytest.approx(0.02)


def test_compute_derived_calculates_cpc() -> None:
    m = CampaignMetrics(
        date="2026-10-06", platform="meta", account_id="acc",
        campaign_id="1", campaign_name="C",
        impressions=5000, clicks=100, spend_usd=50.0, conversions=5, revenue_usd=200.0,
    )
    m.compute_derived()
    assert m.cpc_usd == pytest.approx(0.50)


def test_compute_derived_calculates_cpm() -> None:
    m = CampaignMetrics(
        date="2026-10-06", platform="meta", account_id="acc",
        campaign_id="1", campaign_name="C",
        impressions=10000, clicks=100, spend_usd=20.0, conversions=2, revenue_usd=100.0,
    )
    m.compute_derived()
    assert m.cpm_usd == pytest.approx(2.0)


def test_compute_derived_calculates_roas() -> None:
    m = CampaignMetrics(
        date="2026-10-06", platform="google_ads", account_id="acc",
        campaign_id="1", campaign_name="C",
        impressions=1000, clicks=50, spend_usd=100.0, conversions=5, revenue_usd=350.0,
    )
    m.compute_derived()
    assert m.roas == pytest.approx(3.5)


def test_compute_derived_calculates_cpa() -> None:
    m = CampaignMetrics(
        date="2026-10-06", platform="meta", account_id="acc",
        campaign_id="1", campaign_name="C",
        impressions=5000, clicks=200, spend_usd=80.0, conversions=4, revenue_usd=240.0,
    )
    m.compute_derived()
    assert m.cpa_usd == pytest.approx(20.0)


def test_compute_derived_zero_impressions_safe() -> None:
    m = CampaignMetrics(
        date="2026-10-06", platform="meta", account_id="acc",
        campaign_id="1", campaign_name="C",
        impressions=0, clicks=0, spend_usd=0.0, conversions=0, revenue_usd=0.0,
    )
    m.compute_derived()
    assert m.ctr == 0.0
    assert m.cpc_usd == 0.0
    assert m.cpm_usd == 0.0
    assert m.roas == 0.0
    assert m.cpa_usd == 0.0


# ---------------------------------------------------------------------------
# normalize_google_ads_row
# ---------------------------------------------------------------------------


def test_normalize_google_ads_row_maps_fields() -> None:
    row = {
        "campaign_id": "c1",
        "campaign_name": "Alpha",
        "date": "2026-10-06",
        "impressions": 8000,
        "clicks": 160,
        "spend_usd": 80.0,
        "conversions": 8,
        "revenue_usd": 400.0,
    }
    m = normalize_google_ads_row(row, account_uuid="uuid-123")
    assert m.platform == "google_ads"
    assert m.account_id == "uuid-123"
    assert m.campaign_id == "c1"
    assert m.impressions == 8000
    assert m.spend_usd == pytest.approx(80.0)
    assert m.roas == pytest.approx(5.0)


def test_normalize_google_ads_row_handles_missing_fields() -> None:
    row = {"campaign_id": "x", "campaign_name": "X", "date": "2026-10-06"}
    m = normalize_google_ads_row(row, account_uuid="uuid")
    assert m.impressions == 0
    assert m.spend_usd == 0.0
    assert m.ctr == 0.0


# ---------------------------------------------------------------------------
# normalize_meta_row
# ---------------------------------------------------------------------------


def test_normalize_meta_row_maps_fields() -> None:
    row = {
        "campaign_id": "m1",
        "campaign_name": "Meta Camp",
        "date_start": "2026-10-06",
        "date_stop": "2026-10-06",
        "impressions": 12000,
        "clicks": 300,
        "spend_usd": 150.0,
        "conversions": 15,
        "revenue_usd": 750.0,
    }
    m = normalize_meta_row(row, account_uuid="uuid-456")
    assert m.platform == "meta"
    assert m.date == "2026-10-06"
    assert m.clicks == 300
    assert m.cpc_usd == pytest.approx(0.5)


def test_normalize_meta_row_uses_date_start_fallback() -> None:
    row = {
        "campaign_id": "m2", "campaign_name": "C",
        "date_start": "2026-10-05", "date_stop": None,
        "impressions": 0, "clicks": 0, "spend_usd": 0.0,
        "conversions": 0, "revenue_usd": 0.0,
    }
    m = normalize_meta_row(row, account_uuid="uuid")
    assert m.date == "2026-10-05"


# ---------------------------------------------------------------------------
# aggregate_by_campaign
# ---------------------------------------------------------------------------


def _make_metric(
    platform: str, campaign_id: str, date: str,
    impressions: int, clicks: int, spend: float, conv: int, rev: float,
) -> CampaignMetrics:
    return CampaignMetrics(
        date=date, platform=platform, account_id="acc",
        campaign_id=campaign_id, campaign_name=f"Camp {campaign_id}",
        impressions=impressions, clicks=clicks, spend_usd=spend,
        conversions=conv, revenue_usd=rev,
    ).compute_derived()


def test_aggregate_sums_across_dates() -> None:
    metrics = [
        _make_metric("google_ads", "c1", "2026-10-05", 1000, 20, 10.0, 1, 50.0),
        _make_metric("google_ads", "c1", "2026-10-06", 2000, 40, 20.0, 2, 100.0),
    ]
    result = aggregate_by_campaign(metrics)
    assert len(result) == 1
    agg = result[0]
    assert agg.impressions == 3000
    assert agg.clicks == 60
    assert agg.spend_usd == pytest.approx(30.0)
    assert agg.conversions == 3
    assert agg.revenue_usd == pytest.approx(150.0)
    assert agg.date == "2026-10-06"  # latest date


def test_aggregate_keeps_separate_campaigns() -> None:
    metrics = [
        _make_metric("google_ads", "c1", "2026-10-06", 1000, 20, 10.0, 1, 50.0),
        _make_metric("google_ads", "c2", "2026-10-06", 2000, 30, 15.0, 2, 80.0),
    ]
    result = aggregate_by_campaign(metrics)
    assert len(result) == 2


def test_aggregate_separates_platforms() -> None:
    metrics = [
        _make_metric("google_ads", "c1", "2026-10-06", 1000, 20, 10.0, 1, 50.0),
        _make_metric("meta", "c1", "2026-10-06", 1000, 20, 10.0, 1, 50.0),
    ]
    result = aggregate_by_campaign(metrics)
    assert len(result) == 2  # same campaign_id but different platform = different rows


def test_aggregate_recomputes_derived() -> None:
    metrics = [
        _make_metric("meta", "c1", "2026-10-05", 1000, 50, 25.0, 2, 100.0),
        _make_metric("meta", "c1", "2026-10-06", 1000, 50, 25.0, 2, 100.0),
    ]
    result = aggregate_by_campaign(metrics)
    agg = result[0]
    # Aggregated: 2000 impressions, 100 clicks, 50 spend, 200 revenue
    assert agg.ctr == pytest.approx(0.05)   # 100 / 2000
    assert agg.cpc_usd == pytest.approx(0.50)  # 50 / 100
    assert agg.roas == pytest.approx(4.0)    # 200 / 50


def test_aggregate_empty_list() -> None:
    assert aggregate_by_campaign([]) == []


# ---------------------------------------------------------------------------
# calculate_pacing
# ---------------------------------------------------------------------------


def test_pacing_on_pace() -> None:
    p = calculate_pacing("c1", "C", "google_ads", "acc", 100.0, 50.0, 50.0)
    assert p.status == "on_pace"
    assert p.pacing_pct == pytest.approx(100.0)


def test_pacing_underpacing() -> None:
    p = calculate_pacing("c1", "C", "meta", "acc", 100.0, 30.0, 50.0)
    # expected spend at 50% of day = 50.0; actual = 30 → pacing = 60%
    assert p.status == "underpacing"
    assert p.pacing_pct == pytest.approx(60.0)


def test_pacing_overpacing() -> None:
    p = calculate_pacing("c1", "C", "meta", "acc", 100.0, 70.0, 50.0)
    # expected spend = 50; actual = 70 → pacing = 140%
    assert p.status == "overpacing"
    assert p.pacing_pct == pytest.approx(140.0)


def test_pacing_no_budget() -> None:
    p = calculate_pacing("c1", "C", "google_ads", "acc", 0.0, 0.0, 50.0)
    assert p.status == "no_budget"
    assert p.pacing_pct == 0.0


def test_pacing_start_of_day_no_spend() -> None:
    p = calculate_pacing("c1", "C", "google_ads", "acc", 100.0, 0.0, 0.0)
    # day_elapsed_pct = 0, expected_spend = 0, pacing = 0
    assert p.pacing_pct == 0.0
    assert p.status == "underpacing"
