"""Unit tests for bidcap_monitor._make_recommendation.

Tests the pure recommendation logic without any HTTP calls.
"""

from __future__ import annotations

from unittest.mock import patch

import loaders.bidcap_monitor as monitor_mod
from loaders.bidcap_monitor import DayResult, _make_recommendation

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _day(day: int, spend: float, revenue: float, roas: float) -> DayResult:
    return DayResult(
        day=day,
        date_str=f"2026-10-{6 + day:02d}",
        spend_usd=spend,
        revenue_usd=revenue,
        roas=roas,
    )


# ---------------------------------------------------------------------------
# _make_recommendation
# ---------------------------------------------------------------------------


def test_no_data_returns_no_data() -> None:
    rec, action = _make_recommendation([])
    assert rec == "NO_DATA"
    assert "no data" in action.lower()


def test_low_spend_returns_loosen_bid_caps() -> None:
    with patch.object(monitor_mod, "_MIN_DAILY_SPEND", 50.0):
        days = [_day(1, spend=20.0, revenue=0.0, roas=0.0)]
        rec, action = _make_recommendation(days)

    assert rec == "LOOSEN_BID_CAPS"
    assert "bid cap" in action.lower() or "Increase" in action


def test_low_spend_message_includes_threshold_and_new_cap() -> None:
    with (
        patch.object(monitor_mod, "_MIN_DAILY_SPEND", 50.0),
        patch.object(monitor_mod, "_BID_CAP_CURRENT", 57.0),
        patch.object(monitor_mod, "_BID_CAP_LOOSE", 87.0),
    ):
        days = [_day(2, spend=10.0, revenue=0.0, roas=0.0)]
        _, action = _make_recommendation(days)

    assert "57" in action
    assert "87" in action


def test_day4_above_roas_threshold_returns_scale_budget() -> None:
    with (
        patch.object(monitor_mod, "_SCALE_ROAS", 1.5),
        patch.object(monitor_mod, "_MIN_DAILY_SPEND", 50.0),
    ):
        days = [_day(4, spend=200.0, revenue=320.0, roas=1.6)]
        rec, action = _make_recommendation(days)

    assert rec == "SCALE_BUDGET"
    assert "1.60" in action or "1.6" in action


def test_day4_below_roas_threshold_returns_continue_test() -> None:
    with (
        patch.object(monitor_mod, "_SCALE_ROAS", 1.5),
        patch.object(monitor_mod, "_MIN_DAILY_SPEND", 50.0),
    ):
        days = [_day(4, spend=200.0, revenue=280.0, roas=1.4)]
        rec, action = _make_recommendation(days)

    assert rec == "CONTINUE_TEST"
    assert "1.40" in action or "1.4" in action


def test_day5_exact_threshold_returns_scale_budget() -> None:
    with (
        patch.object(monitor_mod, "_SCALE_ROAS", 1.5),
        patch.object(monitor_mod, "_MIN_DAILY_SPEND", 50.0),
    ):
        days = [_day(5, spend=200.0, revenue=300.0, roas=1.5)]
        rec, _ = _make_recommendation(days)

    assert rec == "SCALE_BUDGET"


def test_pre_day4_with_good_spend_returns_continue_test() -> None:
    with (
        patch.object(monitor_mod, "_SCALE_ROAS", 1.5),
        patch.object(monitor_mod, "_MIN_DAILY_SPEND", 50.0),
    ):
        days = [
            _day(1, spend=150.0, revenue=250.0, roas=1.67),
            _day(2, spend=160.0, revenue=260.0, roas=1.63),
        ]
        rec, action = _make_recommendation(days)

    assert rec == "CONTINUE_TEST"
    assert "Day" in action


def test_multiple_day4_plus_uses_latest() -> None:
    """With days 4 and 5 present, the day-5 data drives the decision."""
    with (
        patch.object(monitor_mod, "_SCALE_ROAS", 1.5),
        patch.object(monitor_mod, "_MIN_DAILY_SPEND", 50.0),
    ):
        days = [
            _day(4, spend=200.0, revenue=280.0, roas=1.4),  # below threshold
            _day(5, spend=200.0, revenue=340.0, roas=1.7),  # above
        ]
        rec, action = _make_recommendation(days)

    assert rec == "SCALE_BUDGET"
    # Should reference day 5
    assert "Day 5" in action


def test_scale_budget_action_mentions_budget() -> None:
    with (
        patch.object(monitor_mod, "_SCALE_ROAS", 1.5),
        patch.object(monitor_mod, "_MIN_DAILY_SPEND", 50.0),
        patch.object(monitor_mod, "_SCALE_BUDGET_EUR", 3000.0),
    ):
        days = [_day(4, spend=200.0, revenue=340.0, roas=1.7)]
        _, action = _make_recommendation(days)

    assert "3000" in action or "€3000" in action


def test_low_spend_takes_priority_over_day4_check() -> None:
    """Even on day 4, low spend → LOOSEN_BID_CAPS before ROAS is evaluated."""
    with (
        patch.object(monitor_mod, "_SCALE_ROAS", 1.5),
        patch.object(monitor_mod, "_MIN_DAILY_SPEND", 50.0),
    ):
        # Day 4 with great ROAS but spend below threshold
        days = [_day(4, spend=20.0, revenue=100.0, roas=5.0)]
        rec, _ = _make_recommendation(days)

    assert rec == "LOOSEN_BID_CAPS"
