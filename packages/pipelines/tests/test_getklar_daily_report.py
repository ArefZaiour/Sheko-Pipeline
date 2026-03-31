"""Unit tests for the GetKlar daily spend report pipeline.

All tests use unittest.mock — no real HTTP calls or API credentials required.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from integrations.getklar import (
    ChannelSpend,
    GetKlarClient,
    build_from_env,
    yesterday,
)
from loaders.getklar_daily_report import (
    ChannelTarget,
    ReportRow,
    _gsheet_csv_url,
    _normalise_channel,
    _parse_euro,
    _parse_percent,
    build_report,
    format_markdown_table,
    load_targets_from_sheet,
    send_to_teams,
)


# ---------------------------------------------------------------------------
# Helpers / parsing
# ---------------------------------------------------------------------------


def test_parse_percent_german_format() -> None:
    assert _parse_percent("28,7%") == pytest.approx(28.7)


def test_parse_percent_integer() -> None:
    assert _parse_percent("100%") == pytest.approx(100.0)


def test_parse_percent_empty() -> None:
    assert _parse_percent("") == 0.0


def test_parse_euro_german_thousands() -> None:
    assert _parse_euro("373.209€") == pytest.approx(373209.0)


def test_parse_euro_with_decimal() -> None:
    assert _parse_euro("1.300.080€") == pytest.approx(1300080.0)


def test_parse_euro_plain() -> None:
    assert _parse_euro("1234€") == pytest.approx(1234.0)


def test_gsheet_csv_url_extracts_id() -> None:
    url = "https://docs.google.com/spreadsheets/d/1qP38HluiX-en5ezMCbQb5WiAIQ2qYEF_/edit?usp=sharing"
    csv_url = _gsheet_csv_url(url)
    assert "1qP38HluiX-en5ezMCbQb5WiAIQ2qYEF_" in csv_url
    assert csv_url.endswith("export?format=csv")


def test_gsheet_csv_url_raises_on_invalid() -> None:
    with pytest.raises(ValueError, match="Cannot extract"):
        _gsheet_csv_url("https://example.com/not-a-sheet")


# ---------------------------------------------------------------------------
# Channel normalisation
# ---------------------------------------------------------------------------


def test_normalise_channel_strips_google_prefix() -> None:
    assert _normalise_channel("Google Demand Gen") == "demand gen"


def test_normalise_channel_strips_paid_suffix() -> None:
    assert _normalise_channel("Bing Generic Paid Search") == "bing generic search"
    assert _normalise_channel("Bing Branded Paid Search") == "bing branded search"


def test_normalise_channel_strips_tool_cost() -> None:
    assert _normalise_channel("Email (Tool Cost)") == "email"


def test_normalise_channel_no_change_for_simple() -> None:
    assert _normalise_channel("Meta Ads") == "meta ads"


# ---------------------------------------------------------------------------
# load_targets_from_sheet
# ---------------------------------------------------------------------------

SAMPLE_CSV = """\
,,,,,,,,,,
,Channel Analyse,,,,,,,,,
,,,,,,,,,,
,Channel,Spend (€),% Spend,Net Rev MM (€),ROAS MM,NC Orders,NC Rev (€),NC ROAS,CAC (€),Bewertung
,Meta Ads,373.209€,"28,7%",453.701€,"1,22x",5.048,396.333€,"1,06x",74€,OK
,Google Generic Search,190.336€,"14,6%",320.036€,"1,68x",3.488,272.792€,"1,43x",55€,OK
,TOTAL / BLENDED,1.300.080€,"100,0%",2.889.089€,"2,22x",16.361,1.281.483€,"1,04x",,
"""


def test_load_targets_parses_channels(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_resp = MagicMock()
    mock_resp.text = SAMPLE_CSV
    mock_resp.raise_for_status = MagicMock()

    with patch("loaders.getklar_daily_report.httpx.get", return_value=mock_resp):
        targets = load_targets_from_sheet(
            "https://docs.google.com/spreadsheets/d/FAKEID/edit"
        )

    assert len(targets) == 2
    assert targets[0].channel == "Meta Ads"
    assert targets[0].target_pct == pytest.approx(28.7)
    assert targets[1].channel == "Google Generic Search"
    assert targets[1].target_pct == pytest.approx(14.6)


def test_load_targets_excludes_total(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_resp = MagicMock()
    mock_resp.text = SAMPLE_CSV
    mock_resp.raise_for_status = MagicMock()

    with patch("loaders.getklar_daily_report.httpx.get", return_value=mock_resp):
        targets = load_targets_from_sheet(
            "https://docs.google.com/spreadsheets/d/FAKEID/edit"
        )

    channel_names = [t.channel for t in targets]
    assert not any("TOTAL" in c.upper() for c in channel_names)


# ---------------------------------------------------------------------------
# build_report
# ---------------------------------------------------------------------------

_TARGETS = [
    ChannelTarget(channel="Meta Ads", target_pct=28.7),
    ChannelTarget(channel="Demand Gen", target_pct=10.2),
    ChannelTarget(channel="Bing Generic Search", target_pct=2.2),
    ChannelTarget(channel="Email (Tool Cost)", target_pct=0.1),
]


def test_build_report_calculates_actual_pct() -> None:
    spend_data = [
        ChannelSpend(channel="Meta Ads", spend=300.0, orders=100, revenue=400.0),
        ChannelSpend(channel="Google Demand Gen", spend=100.0, orders=50, revenue=150.0),
        ChannelSpend(channel="Bing Generic Paid Search", spend=100.0, orders=10, revenue=50.0),
    ]
    rows = build_report(spend_data, _TARGETS)

    total = 500.0
    meta_row = next(r for r in rows if r.channel == "Meta Ads")
    assert meta_row.actual_spend == pytest.approx(300.0)
    assert meta_row.actual_pct == pytest.approx(300 / total * 100)
    assert meta_row.target_pct == pytest.approx(28.7)
    assert meta_row.delta_pct == pytest.approx(meta_row.actual_pct - 28.7)


def test_build_report_fuzzy_matches_demand_gen() -> None:
    spend_data = [
        ChannelSpend(channel="Google Demand Gen", spend=200.0, orders=10, revenue=300.0),
    ]
    rows = build_report(spend_data, [ChannelTarget(channel="Demand Gen", target_pct=10.2)])
    assert rows[0].actual_spend == pytest.approx(200.0)


def test_build_report_fuzzy_matches_bing_paid_search() -> None:
    spend_data = [
        ChannelSpend(channel="Bing Generic Paid Search", spend=50.0, orders=5, revenue=80.0),
    ]
    rows = build_report(spend_data, [ChannelTarget(channel="Bing Generic Search", target_pct=2.2)])
    assert rows[0].actual_spend == pytest.approx(50.0)


def test_build_report_fuzzy_matches_email_tool_cost() -> None:
    spend_data = [
        ChannelSpend(channel="Email", spend=30.0, orders=2, revenue=50.0),
    ]
    rows = build_report(spend_data, [ChannelTarget(channel="Email (Tool Cost)", target_pct=0.1)])
    assert rows[0].actual_spend == pytest.approx(30.0)


def test_build_report_zero_spend_for_missing_channel() -> None:
    spend_data = [
        ChannelSpend(channel="Meta Ads", spend=500.0, orders=100, revenue=600.0),
    ]
    rows = build_report(spend_data, _TARGETS)

    bing_row = next(r for r in rows if "Bing" in r.channel)
    assert bing_row.actual_spend == 0.0
    assert bing_row.actual_pct == 0.0


def test_build_report_empty_spend_gives_zero_pct() -> None:
    rows = build_report([], _TARGETS)
    for r in rows:
        assert r.actual_spend == 0.0
        assert r.actual_pct == 0.0


# ---------------------------------------------------------------------------
# format_markdown_table
# ---------------------------------------------------------------------------


def test_format_markdown_table_contains_headers() -> None:
    rows = [
        ReportRow(
            channel="Meta Ads",
            actual_spend=300.0,
            actual_pct=60.0,
            target_pct=28.7,
            delta_pct=31.3,
        )
    ]
    table = format_markdown_table(rows, date(2026, 3, 30))
    assert "2026-03-30" in table
    assert "Meta Ads" in table
    assert "Ist %" in table
    assert "Soll %" in table
    assert "Delta" in table


def test_format_markdown_table_shows_total_row() -> None:
    rows = [
        ReportRow(
            channel="Meta Ads",
            actual_spend=300.0,
            actual_pct=60.0,
            target_pct=28.7,
            delta_pct=31.3,
        )
    ]
    table = format_markdown_table(rows, date(2026, 3, 30))
    assert "TOTAL" in table


# ---------------------------------------------------------------------------
# send_to_teams
# ---------------------------------------------------------------------------


def test_send_to_teams_posts_json() -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 202
    mock_resp.raise_for_status = MagicMock()

    with patch("loaders.getklar_daily_report.httpx.post", return_value=mock_resp) as mock_post:
        send_to_teams("https://webhook.example.com/teams", "# Report\n\nSome data")

    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == "https://webhook.example.com/teams"
    body = call_kwargs[1]["json"]
    assert body["type"] == "message"


# ---------------------------------------------------------------------------
# GetKlarClient
# ---------------------------------------------------------------------------


def test_getklar_client_raises_without_token() -> None:
    with pytest.raises(EnvironmentError, match="GETKLAR_API_TOKEN"):
        GetKlarClient(api_token="")


def test_build_from_env_raises_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GETKLAR_API_TOKEN", raising=False)
    with pytest.raises(EnvironmentError, match="GETKLAR_API_TOKEN"):
        build_from_env()


def test_getklar_client_caches_access_token() -> None:
    client = GetKlarClient(api_token="fake-api-token")

    mock_resp = MagicMock()
    mock_resp.json.return_value = {"accessToken": "at-123", "expiresIn": 300_000}
    mock_resp.raise_for_status = MagicMock()

    with patch("integrations.getklar.httpx.post", return_value=mock_resp) as mock_post:
        token1 = client._get_access_token()
        token2 = client._get_access_token()

    assert token1 == "at-123"
    assert token2 == "at-123"
    # Should only call the token endpoint once (second call uses cache).
    mock_post.assert_called_once()


def test_getklar_client_fetch_spend_by_channel_aggregates() -> None:
    """fetch_spend_by_channel should aggregate ad-level rows by channelName."""
    client = GetKlarClient(api_token="fake-api-token")

    import time as _time
    client._access_token = "cached-token"
    client._access_token_expiry = _time.time() + 3600

    # Two rows for the same channel (different ads), one for another channel.
    fake_rows = [
        {
            "channelName": "Meta Ads",
            "cost": 100.0,
            "orders": 5,
            "netRevenue": 150.0,
        },
        {
            "channelName": "Meta Ads",
            "cost": 50.0,
            "orders": 2,
            "netRevenue": 80.0,
        },
        {
            "channelName": "Google Generic Search",
            "cost": 80.0,
            "orders": 3,
            "netRevenue": 120.0,
        },
    ]
    mock_resp = MagicMock()
    mock_resp.json.return_value = fake_rows
    mock_resp.raise_for_status = MagicMock()

    with patch("integrations.getklar.httpx.get", return_value=mock_resp):
        result = client.fetch_spend_by_channel(date(2026, 3, 30))

    assert len(result) == 2
    meta = next(r for r in result if r.channel == "Meta Ads")
    assert meta.spend == pytest.approx(150.0)  # 100 + 50
    assert meta.orders == pytest.approx(7.0)   # 5 + 2

    google = next(r for r in result if r.channel == "Google Generic Search")
    assert google.spend == pytest.approx(80.0)


def test_getklar_client_sorted_by_spend_desc() -> None:
    client = GetKlarClient(api_token="fake-api-token")

    import time as _time
    client._access_token = "cached-token"
    client._access_token_expiry = _time.time() + 3600

    fake_rows = [
        {"channelName": "Small Channel", "cost": 10.0, "orders": 1, "netRevenue": 15.0},
        {"channelName": "Big Channel", "cost": 500.0, "orders": 20, "netRevenue": 800.0},
    ]
    mock_resp = MagicMock()
    mock_resp.json.return_value = fake_rows
    mock_resp.raise_for_status = MagicMock()

    with patch("integrations.getklar.httpx.get", return_value=mock_resp):
        result = client.fetch_spend_by_channel(date(2026, 3, 30))

    assert result[0].channel == "Big Channel"
    assert result[1].channel == "Small Channel"


def test_yesterday_is_one_day_before_today() -> None:
    from datetime import date as _date, timedelta
    assert yesterday() == _date.today() - timedelta(days=1)
