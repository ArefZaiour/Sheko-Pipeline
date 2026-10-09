"""Tests for Google Ads and Meta Ads integrations.

All tests use unittest.mock — no real HTTP calls or API credentials required.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from integrations.google_ads import GoogleAdsClient
from integrations.google_ads import build_from_env as build_google_ads
from integrations.meta import (
    MetaAdsClient,
    _extract_purchase_metric,
)
from integrations.meta import (
    build_from_env as build_meta,
)

# ---------------------------------------------------------------------------
# GoogleAdsClient — construction
# ---------------------------------------------------------------------------


def test_google_ads_client_raises_without_developer_token() -> None:
    with pytest.raises(EnvironmentError, match="GOOGLE_ADS_DEVELOPER_TOKEN"):
        GoogleAdsClient(
            developer_token="",
            client_id="cid",
            client_secret="csec",
            refresh_token="rt",
        )


def test_google_ads_client_raises_without_client_id() -> None:
    with pytest.raises(EnvironmentError, match="GOOGLE_ADS_CLIENT_ID"):
        GoogleAdsClient(
            developer_token="devtoken",
            client_id="",
            client_secret="csec",
            refresh_token="rt",
        )


def test_google_ads_build_from_env_raises_without_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in [
        "GOOGLE_ADS_DEVELOPER_TOKEN",
        "GOOGLE_ADS_CLIENT_ID",
        "GOOGLE_ADS_CLIENT_SECRET",
        "GOOGLE_ADS_REFRESH_TOKEN",
    ]:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(EnvironmentError):
        build_google_ads()


# ---------------------------------------------------------------------------
# GoogleAdsClient — fetch_campaign_metrics
# ---------------------------------------------------------------------------


def _make_ga_row(
    campaign_id: str,
    campaign_name: str,
    row_date: str,
    impressions: int,
    clicks: int,
    cost_micros: int,
    conversions: float,
    conversions_value: float,
) -> MagicMock:
    row = MagicMock()
    row.campaign.id = int(campaign_id)
    row.campaign.name = campaign_name
    row.segments.date = row_date
    row.metrics.impressions = impressions
    row.metrics.clicks = clicks
    row.metrics.cost_micros = cost_micros
    row.metrics.conversions = conversions
    row.metrics.conversions_value = conversions_value
    return row


@pytest.fixture()
def ga_client() -> GoogleAdsClient:
    return GoogleAdsClient(
        developer_token="devtoken",
        client_id="cid",
        client_secret="csec",
        refresh_token="rt",
    )


@pytest.mark.asyncio
async def test_ga_fetch_campaign_metrics_returns_rows(ga_client: GoogleAdsClient) -> None:
    fake_rows = [
        _make_ga_row("111", "Campaign Alpha", "2026-10-06", 5000, 100, 50_000_000, 10.0, 500.0),
        _make_ga_row("222", "Campaign Beta", "2026-10-06", 3000, 80, 30_000_000, 5.0, 200.0),
    ]
    with patch.object(ga_client, "_run_query", return_value=fake_rows):
        results = await ga_client.fetch_campaign_metrics(
            "123-456-7890", date(2026, 10, 6), date(2026, 10, 6)
        )

    assert len(results) == 2
    alpha = next(r for r in results if r["campaign_name"] == "Campaign Alpha")
    assert alpha["campaign_id"] == "111"
    assert alpha["impressions"] == 5000
    assert alpha["clicks"] == 100
    assert alpha["spend_usd"] == pytest.approx(50.0)  # 50_000_000 / 1_000_000
    assert alpha["conversions"] == 10
    assert alpha["revenue_usd"] == pytest.approx(500.0)


@pytest.mark.asyncio
async def test_ga_fetch_campaign_metrics_converts_micros(ga_client: GoogleAdsClient) -> None:
    fake_rows = [_make_ga_row("1", "C", "2026-10-06", 0, 0, 1_234_500, 0.0, 0.0)]
    with patch.object(ga_client, "_run_query", return_value=fake_rows):
        results = await ga_client.fetch_campaign_metrics(
            "123", date(2026, 10, 6), date(2026, 10, 6)
        )
    assert results[0]["spend_usd"] == pytest.approx(1.2345)


@pytest.mark.asyncio
async def test_ga_fetch_campaign_metrics_empty(ga_client: GoogleAdsClient) -> None:
    with patch.object(ga_client, "_run_query", return_value=[]):
        results = await ga_client.fetch_campaign_metrics(
            "123", date(2026, 10, 1), date(2026, 10, 6)
        )
    assert results == []


# ---------------------------------------------------------------------------
# GoogleAdsClient — fetch_budget_pacing
# ---------------------------------------------------------------------------


def _make_pacing_row(
    campaign_id: str,
    name: str,
    budget_micros: int,
    period: str,
    cost_micros: int,
    impressions: int,
) -> MagicMock:
    row = MagicMock()
    row.campaign.id = int(campaign_id)
    row.campaign.name = name
    row.campaign_budget.amount_micros = budget_micros
    row.campaign_budget.period = period
    row.metrics.cost_micros = cost_micros
    row.metrics.impressions = impressions
    return row


@pytest.mark.asyncio
async def test_ga_fetch_budget_pacing(ga_client: GoogleAdsClient) -> None:
    fake_rows = [
        _make_pacing_row("111", "Alpha", 100_000_000, "DAILY", 40_000_000, 10000),
    ]
    with patch.object(ga_client, "_run_query", return_value=fake_rows):
        results = await ga_client.fetch_budget_pacing("123")

    assert len(results) == 1
    assert results[0]["budget_usd"] == pytest.approx(100.0)
    assert results[0]["spend_today_usd"] == pytest.approx(40.0)
    assert results[0]["impressions_today"] == 10000


# ---------------------------------------------------------------------------
# MetaAdsClient — construction
# ---------------------------------------------------------------------------


def test_meta_client_raises_without_token() -> None:
    with pytest.raises(EnvironmentError, match="META_ACCESS_TOKEN"):
        MetaAdsClient(access_token="")


def test_meta_build_from_env_raises_without_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("META_ACCESS_TOKEN", raising=False)
    with pytest.raises(EnvironmentError, match="META_ACCESS_TOKEN"):
        build_meta()


# ---------------------------------------------------------------------------
# _extract_purchase_metric
# ---------------------------------------------------------------------------


def test_extract_purchase_metric_sums_purchase_types() -> None:
    actions = [
        {"action_type": "purchase", "value": "3"},
        {"action_type": "omni_purchase", "value": "2"},
        {"action_type": "link_click", "value": "100"},
    ]
    assert _extract_purchase_metric(actions) == pytest.approx(5.0)


def test_extract_purchase_metric_returns_zero_for_empty() -> None:
    assert _extract_purchase_metric([]) == 0.0


def test_extract_purchase_metric_handles_none_value() -> None:
    actions = [{"action_type": "purchase", "value": None}]
    assert _extract_purchase_metric(actions) == 0.0


# ---------------------------------------------------------------------------
# MetaAdsClient — fetch_campaign_metrics
# ---------------------------------------------------------------------------


@pytest.fixture()
def meta_client() -> MetaAdsClient:
    return MetaAdsClient(access_token="fake-meta-token")


def _make_meta_insights_response(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"data": rows, "paging": {}}


@pytest.mark.asyncio
async def test_meta_fetch_campaign_metrics_returns_rows(meta_client: MetaAdsClient) -> None:
    fake_response = _make_meta_insights_response(
        [
            {
                "campaign_id": "c1",
                "campaign_name": "Meta Campaign A",
                "date_start": "2026-10-06",
                "date_stop": "2026-10-06",
                "impressions": "12000",
                "clicks": "300",
                "spend": "150.50",
                "actions": [{"action_type": "purchase", "value": "8"}],
                "action_values": [{"action_type": "purchase", "value": "400.00"}],
            }
        ]
    )
    mock_resp = MagicMock()
    mock_resp.headers = {}
    mock_resp.json.return_value = fake_response
    mock_resp.raise_for_status = MagicMock()

    with patch("integrations.meta.httpx.get", return_value=mock_resp):
        results = await meta_client.fetch_campaign_metrics(
            "act_123456", date(2026, 10, 6), date(2026, 10, 6)
        )

    assert len(results) == 1
    r = results[0]
    assert r["campaign_id"] == "c1"
    assert r["campaign_name"] == "Meta Campaign A"
    assert r["impressions"] == 12000
    assert r["clicks"] == 300
    assert r["spend_usd"] == pytest.approx(150.50)
    assert r["conversions"] == 8
    assert r["revenue_usd"] == pytest.approx(400.0)


@pytest.mark.asyncio
async def test_meta_fetch_campaign_metrics_handles_no_actions(
    meta_client: MetaAdsClient,
) -> None:
    fake_response = _make_meta_insights_response(
        [
            {
                "campaign_id": "c2",
                "campaign_name": "No Conv Campaign",
                "date_start": "2026-10-06",
                "date_stop": "2026-10-06",
                "impressions": "5000",
                "clicks": "50",
                "spend": "20.00",
            }
        ]
    )
    mock_resp = MagicMock()
    mock_resp.headers = {}
    mock_resp.json.return_value = fake_response
    mock_resp.raise_for_status = MagicMock()

    with patch("integrations.meta.httpx.get", return_value=mock_resp):
        results = await meta_client.fetch_campaign_metrics(
            "act_123", date(2026, 10, 6), date(2026, 10, 6)
        )

    assert results[0]["conversions"] == 0
    assert results[0]["revenue_usd"] == 0.0


@pytest.mark.asyncio
async def test_meta_fetch_campaign_metrics_paginates(meta_client: MetaAdsClient) -> None:
    """Paginator follows `next` links until exhausted."""
    page1 = {
        "data": [
            {
                "campaign_id": "c1",
                "campaign_name": "Page 1 Camp",
                "date_start": "2026-10-06",
                "date_stop": "2026-10-06",
                "impressions": "100",
                "clicks": "5",
                "spend": "10.00",
            }
        ],
        "paging": {"next": "https://graph.facebook.com/v21.0/act_1/insights?after=cursor"},
    }
    page2 = {
        "data": [
            {
                "campaign_id": "c2",
                "campaign_name": "Page 2 Camp",
                "date_start": "2026-10-06",
                "date_stop": "2026-10-06",
                "impressions": "200",
                "clicks": "10",
                "spend": "20.00",
            }
        ],
        "paging": {},
    }
    responses = [page1, page2]

    call_count = 0

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        nonlocal call_count
        resp = MagicMock()
        resp.headers = {}
        resp.json.return_value = responses[call_count]
        resp.raise_for_status = MagicMock()
        call_count += 1
        return resp

    with patch("integrations.meta.httpx.get", side_effect=fake_get):
        results = await meta_client.fetch_campaign_metrics(
            "act_1", date(2026, 10, 6), date(2026, 10, 6)
        )

    assert len(results) == 2
    assert results[0]["campaign_id"] == "c1"
    assert results[1]["campaign_id"] == "c2"


# ---------------------------------------------------------------------------
# MetaAdsClient — fetch_budget_pacing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_meta_fetch_budget_pacing(meta_client: MetaAdsClient) -> None:
    campaigns_response = {
        "data": [
            {
                "id": "c1",
                "name": "Active Camp",
                "status": "ACTIVE",
                "daily_budget": "5000",  # cents → $50
                "lifetime_budget": "0",
            }
        ],
        "paging": {},
    }
    insights_response = {
        "data": [{"spend": "12.50", "impressions": "3000"}],
        "paging": {},
    }

    call_count = 0

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        nonlocal call_count
        resp = MagicMock()
        resp.headers = {}
        resp.raise_for_status = MagicMock()
        if "campaigns" in url:
            resp.json.return_value = campaigns_response
        else:
            resp.json.return_value = insights_response
        call_count += 1
        return resp

    with patch("integrations.meta.httpx.get", side_effect=fake_get):
        results = await meta_client.fetch_budget_pacing("act_123")

    assert len(results) == 1
    r = results[0]
    assert r["campaign_id"] == "c1"
    assert r["daily_budget_usd"] == pytest.approx(50.0)  # 5000 cents / 100
    assert r["spend_today_usd"] == pytest.approx(12.50)
    assert r["impressions_today"] == 3000


# ---------------------------------------------------------------------------
# MetaAdsClient — fetch_campaign_spend_by_day
# ---------------------------------------------------------------------------


def test_meta_fetch_campaign_spend_by_day_returns_rows(meta_client: MetaAdsClient) -> None:
    fake_response = {
        "data": [
            {
                "date_start": "2026-10-07",
                "date_stop": "2026-10-07",
                "spend": "120.00",
                "action_values": [{"action_type": "purchase", "value": "300.00"}],
            },
            {
                "date_start": "2026-10-08",
                "date_stop": "2026-10-08",
                "spend": "80.00",
                "action_values": [{"action_type": "purchase", "value": "200.00"}],
            },
        ],
        "paging": {},
    }
    mock_resp = MagicMock()
    mock_resp.headers = {}
    mock_resp.json.return_value = fake_response
    mock_resp.raise_for_status = MagicMock()

    with patch("integrations.meta.httpx.get", return_value=mock_resp):
        results = meta_client.fetch_campaign_spend_by_day(
            "52529430959117", date(2026, 10, 7), date(2026, 10, 8)
        )

    assert len(results) == 2
    r0 = results[0]
    assert r0["date_start"] == "2026-10-07"
    assert r0["spend_usd"] == pytest.approx(120.0)
    assert r0["revenue_usd"] == pytest.approx(300.0)
    assert r0["roas"] == pytest.approx(2.5)

    r1 = results[1]
    assert r1["spend_usd"] == pytest.approx(80.0)
    assert r1["roas"] == pytest.approx(2.5)


def test_meta_fetch_campaign_spend_by_day_zero_spend_gives_zero_roas(
    meta_client: MetaAdsClient,
) -> None:
    fake_response = {
        "data": [
            {
                "date_start": "2026-10-07",
                "date_stop": "2026-10-07",
                "spend": "0",
                "action_values": [{"action_type": "purchase", "value": "100.00"}],
            }
        ],
        "paging": {},
    }
    mock_resp = MagicMock()
    mock_resp.headers = {}
    mock_resp.json.return_value = fake_response
    mock_resp.raise_for_status = MagicMock()

    with patch("integrations.meta.httpx.get", return_value=mock_resp):
        results = meta_client.fetch_campaign_spend_by_day(
            "camp-id", date(2026, 10, 7), date(2026, 10, 7)
        )

    assert results[0]["roas"] == 0.0


def test_meta_fetch_campaign_spend_by_day_empty(meta_client: MetaAdsClient) -> None:
    mock_resp = MagicMock()
    mock_resp.headers = {}
    mock_resp.json.return_value = {"data": [], "paging": {}}
    mock_resp.raise_for_status = MagicMock()

    with patch("integrations.meta.httpx.get", return_value=mock_resp):
        results = meta_client.fetch_campaign_spend_by_day(
            "camp-id", date(2026, 10, 7), date(2026, 10, 7)
        )

    assert results == []


# ---------------------------------------------------------------------------
# MetaAdsClient — fetch_ad_sets
# ---------------------------------------------------------------------------


def test_meta_fetch_ad_sets_returns_rows(meta_client: MetaAdsClient) -> None:
    fake_response = {
        "data": [
            {
                "id": "as-1",
                "name": "ABO_Testing_US",
                "status": "PAUSED",
                "effective_status": "CAMPAIGN_PAUSED",
                "daily_budget": "2000",  # cents → $20
            },
            {
                "id": "as-2",
                "name": "BIDCAP_Scaling",
                "status": "ACTIVE",
                "effective_status": "ACTIVE",
                "daily_budget": "5000",  # cents → $50
            },
        ],
        "paging": {},
    }
    mock_resp = MagicMock()
    mock_resp.headers = {}
    mock_resp.json.return_value = fake_response
    mock_resp.raise_for_status = MagicMock()

    with patch("integrations.meta.httpx.get", return_value=mock_resp):
        results = meta_client.fetch_ad_sets("52529430959117")

    assert len(results) == 2
    r0 = results[0]
    assert r0["ad_set_id"] == "as-1"
    assert r0["ad_set_name"] == "ABO_Testing_US"
    assert r0["status"] == "PAUSED"
    assert r0["effective_status"] == "CAMPAIGN_PAUSED"
    assert r0["daily_budget_usd"] == pytest.approx(20.0)

    r1 = results[1]
    assert r1["daily_budget_usd"] == pytest.approx(50.0)


def test_meta_fetch_ad_sets_status_filter_sent_in_params(meta_client: MetaAdsClient) -> None:
    mock_resp = MagicMock()
    mock_resp.headers = {}
    mock_resp.json.return_value = {"data": [], "paging": {}}
    mock_resp.raise_for_status = MagicMock()

    captured_params: dict[str, Any] = {}

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        captured_params.update(kwargs.get("params", {}))
        return mock_resp

    with patch("integrations.meta.httpx.get", side_effect=fake_get):
        meta_client.fetch_ad_sets("camp-id", status_filter=["ACTIVE", "PAUSED"])

    assert "effective_status" in captured_params
    assert "ACTIVE" in captured_params["effective_status"]
    assert "PAUSED" in captured_params["effective_status"]


def test_meta_fetch_ad_sets_no_filter_omits_param(meta_client: MetaAdsClient) -> None:
    mock_resp = MagicMock()
    mock_resp.headers = {}
    mock_resp.json.return_value = {"data": [], "paging": {}}
    mock_resp.raise_for_status = MagicMock()

    captured_params: dict[str, Any] = {}

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        captured_params.update(kwargs.get("params", {}))
        return mock_resp

    with patch("integrations.meta.httpx.get", side_effect=fake_get):
        meta_client.fetch_ad_sets("camp-id")

    assert "effective_status" not in captured_params


def test_meta_fetch_ad_sets_missing_daily_budget(meta_client: MetaAdsClient) -> None:
    fake_response = {
        "data": [
            {
                "id": "as-3",
                "name": "CBO_Set",
                "status": "ACTIVE",
                "effective_status": "ACTIVE",
                # no daily_budget key — CBO campaigns manage budget at campaign level
            }
        ],
        "paging": {},
    }
    mock_resp = MagicMock()
    mock_resp.headers = {}
    mock_resp.json.return_value = fake_response
    mock_resp.raise_for_status = MagicMock()

    with patch("integrations.meta.httpx.get", return_value=mock_resp):
        results = meta_client.fetch_ad_sets("camp-id")

    assert results[0]["daily_budget_usd"] == pytest.approx(0.0)
