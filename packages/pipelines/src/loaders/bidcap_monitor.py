"""BIDCAP test monitor — runs after Meta Ads reconnects.

Context (GRO-64 / GRO-100):
  Campaign: WEIGHTLOSS_CBO_Scaling_USA_BIDCAP (ID: 52529430959117)
  Account:  Aremido EUR (act_3460090067465234)
  Test:     5-day bid cap test started Oct 7, 2026
  Goal:     If Day-4 ROAS >1.5 → scale to €3k/day on Oct 11

Usage:
    python -m loaders.bidcap_monitor

Required env vars:
    META_ACCESS_TOKEN

Optional env vars:
    BIDCAP_CAMPAIGN_ID       (default: 52529430959117)
    BIDCAP_ACCOUNT_ID        (default: act_3460090067465234)
    ABO_TESTING_CAMPAIGN_ID  (default: lookup by name)
    BIDCAP_START_DATE        (ISO date, default: 2026-10-07)
    BIDCAP_SCALE_ROAS        (float, default: 1.5)
    BIDCAP_SCALE_BUDGET_EUR  (float, default: 3000.0)
    BID_CAP_CURRENT_EUR      (float, default: 57.5  — midpoint of 50–65 range)
    BID_CAP_LOOSE_EUR        (float, default: 87.5  — midpoint of 75–100 range)
    MIN_DAILY_SPEND_EUR      (float, default: 50.0  — below this → caps too tight)
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from datetime import date

import structlog

from integrations.meta import MetaAdsClient, build_from_env

log = structlog.get_logger(__name__)

_BIDCAP_CAMPAIGN_ID = os.environ.get("BIDCAP_CAMPAIGN_ID", "52529430959117")
_BIDCAP_ACCOUNT_ID = os.environ.get("BIDCAP_ACCOUNT_ID", "act_3460090067465234")
_ABO_TESTING_CAMPAIGN_ID = os.environ.get("ABO_TESTING_CAMPAIGN_ID", "")

_SCALE_ROAS = float(os.environ.get("BIDCAP_SCALE_ROAS", "1.5"))
_SCALE_BUDGET_EUR = float(os.environ.get("BIDCAP_SCALE_BUDGET_EUR", "3000.0"))
_BID_CAP_CURRENT = float(os.environ.get("BID_CAP_CURRENT_EUR", "57.5"))
_BID_CAP_LOOSE = float(os.environ.get("BID_CAP_LOOSE_EUR", "87.5"))
_MIN_DAILY_SPEND = float(os.environ.get("MIN_DAILY_SPEND_EUR", "50.0"))

# Oct 7 = Day 1 of the test (Day 4 = Oct 10 → needed for Oct 11 decision)
_TEST_START = date.fromisoformat(os.environ.get("BIDCAP_START_DATE", "2026-10-07"))


@dataclass
class DayResult:
    day: int
    date_str: str
    spend_usd: float
    revenue_usd: float
    roas: float
    verdict: str = ""


@dataclass
class BidCapReport:
    run_date: str
    days: list[DayResult] = field(default_factory=list)
    abo_paused_count: int = 0
    abo_active_count: int = 0
    abo_verified: bool = False
    recommendation: str = ""
    action_required: str = ""


def _make_recommendation(days: list[DayResult]) -> tuple[str, str]:
    """Return (recommendation, action_required) based on cumulative data."""
    today = date.today()
    test_day = (today - _TEST_START).days + 1  # 1-indexed

    if not days:
        return "NO_DATA", f"No data yet — test started {_TEST_START}. Re-run after Meta Ads reconnects."  # noqa: E501

    # Check if spend is too low on recent days → bid caps too tight
    recent = days[-1]
    if recent.spend_usd < _MIN_DAILY_SPEND:
        return (
            "LOOSEN_BID_CAPS",
            f"Day {recent.day} spend €{recent.spend_usd:.2f} < €{_MIN_DAILY_SPEND:.0f} threshold. "
            f"Increase bid caps: €{_BID_CAP_CURRENT:.0f} → €{_BID_CAP_LOOSE:.0f}.",
        )

    # Day 4+ and ROAS above threshold → scale decision
    day4_or_later = [d for d in days if d.day >= 4]
    if day4_or_later:
        latest = day4_or_later[-1]
        if latest.roas >= _SCALE_ROAS:
            return (
                "SCALE_BUDGET",
                f"Day {latest.day} ROAS {latest.roas:.2f} ≥ {_SCALE_ROAS} target. "
                f"Scale WEIGHTLOSS_CBO_Scaling_USA_BIDCAP to €{_SCALE_BUDGET_EUR:.0f}/day.",
            )
        else:
            return (
                "CONTINUE_TEST",
                f"Day {latest.day} ROAS {latest.roas:.2f} < {_SCALE_ROAS} target. "
                f"Continue test; monitor daily.",
            )

    days_remaining = 4 - test_day
    return (
        "CONTINUE_TEST",
        f"Day {test_day} of 5. {days_remaining} more day(s) before scale decision.",
    )


def run_monitor() -> BidCapReport:
    client: MetaAdsClient = build_from_env()
    today = date.today()
    report = BidCapReport(run_date=today.isoformat())

    # Pull BIDCAP campaign spend: Oct 7 → today
    log.info("bidcap.fetching_spend", campaign_id=_BIDCAP_CAMPAIGN_ID)
    raw_days = client.fetch_campaign_spend_by_day(
        _BIDCAP_CAMPAIGN_ID,
        start_date=_TEST_START,
        end_date=today,
    )

    for row in raw_days:
        day_date = date.fromisoformat(row["date_start"])
        day_num = (day_date - _TEST_START).days + 1
        verdict = "ok"
        if row["spend_usd"] < _MIN_DAILY_SPEND:
            verdict = "low_spend"
        elif row["roas"] >= _SCALE_ROAS:
            verdict = "scale_ready"
        report.days.append(
            DayResult(
                day=day_num,
                date_str=row["date_start"],
                spend_usd=row["spend_usd"],
                revenue_usd=row["revenue_usd"],
                roas=row["roas"],
                verdict=verdict,
            )
        )

    # Verify ABO_Testing ad set pauses (if campaign ID known)
    if _ABO_TESTING_CAMPAIGN_ID:
        log.info("bidcap.checking_abo_pauses", campaign_id=_ABO_TESTING_CAMPAIGN_ID)
        ad_sets = client.fetch_ad_sets(_ABO_TESTING_CAMPAIGN_ID)
        for ads in ad_sets:
            if ads["effective_status"] in ("PAUSED", "CAMPAIGN_PAUSED", "ADSET_PAUSED"):
                report.abo_paused_count += 1
            else:
                report.abo_active_count += 1
        report.abo_verified = True

    rec, action = _make_recommendation(report.days)
    report.recommendation = rec
    report.action_required = action

    return report


def print_report(report: BidCapReport) -> None:
    print(f"\n{'='*60}")
    print(f"BIDCAP Monitor — {report.run_date}")
    print(f"{'='*60}")
    print(f"\nCampaign: WEIGHTLOSS_CBO_Scaling_USA_BIDCAP ({_BIDCAP_CAMPAIGN_ID})")
    print(f"Test start: {_TEST_START}  |  Scale ROAS threshold: {_SCALE_ROAS}\n")

    print("Daily spend + ROAS:")
    if not report.days:
        print("  (no data)")
    for d in report.days:
        flag = " ⚠️ " if d.verdict == "low_spend" else (" ✅" if d.verdict == "scale_ready" else "")
        print(
            f"  Day {d.day} ({d.date_str}): "
            f"€{d.spend_usd:.2f} spend | "
            f"ROAS {d.roas:.2f}{flag}"
        )

    if report.abo_verified:
        print(
            f"\nABO_Testing ad sets: "
            f"{report.abo_paused_count} paused, {report.abo_active_count} ACTIVE"
        )
        if report.abo_active_count > 0:
            print(f"  ⚠️  {report.abo_active_count} ad set(s) NOT paused — re-apply pauses!")

    print(f"\nRecommendation: {report.recommendation}")
    print(f"Action:         {report.action_required}")
    print(f"{'='*60}\n")


def main() -> None:
    try:
        report = run_monitor()
        print_report(report)
        # Exit non-zero if action is needed
        if report.recommendation in ("LOOSEN_BID_CAPS", "SCALE_BUDGET"):
            sys.exit(2)
        if report.abo_verified and report.abo_active_count > 0:
            sys.exit(2)
    except OSError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
