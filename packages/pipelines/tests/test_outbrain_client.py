"""Unit tests for the Outbrain Amplify API client.

All tests use unittest.mock — no real HTTP calls or credentials required.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from native_ads.outbrain import OutbrainCampaign, OutbrainClient, build_from_env

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_CAMPAIGN_DATA: dict[str, Any] = {
    "id": "camp-ob-1",
    "createdAt": "2026-09-01T10:00:00Z",
    "modifiedAt": "2026-10-01T08:00:00Z",
    "statistics": {"clicks": 12000, "impressions": 500000},
    "name": "Template OB Campaign",
    "budget": {"amount": 500.0, "currency": "EUR"},
    "targeting": {"geoLocations": ["US"]},
    "status": "ACTIVE",
    "startTime": "2026-10-07",
    "endTime": "2026-12-31",
    "extra_field": "should_be_preserved",
}


def _make_mock(json_body: dict[str, Any]) -> MagicMock:
    m = MagicMock()
    m.raise_for_status = MagicMock()
    m.json.return_value = json_body
    return m


@pytest.fixture()
def client() -> OutbrainClient:
    return OutbrainClient(api_key="test-api-key", account_id="test-account")


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_raises_without_api_key() -> None:
    with pytest.raises(OSError, match="OUTBRAIN_API_KEY"):
        OutbrainClient(api_key="", account_id="acct")


def test_raises_without_account_id() -> None:
    with pytest.raises(OSError, match="OUTBRAIN_ACCOUNT_ID"):
        OutbrainClient(api_key="key", account_id="")


def test_auth_header_set_on_construction() -> None:
    c = OutbrainClient(api_key="my-key", account_id="acct")
    assert c._headers["OB-TOKEN-V1"] == "my-key"


# ---------------------------------------------------------------------------
# build_from_env
# ---------------------------------------------------------------------------


def test_build_from_env_raises_without_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OUTBRAIN_API_KEY", raising=False)
    monkeypatch.delenv("OUTBRAIN_ACCOUNT_ID", raising=False)
    with pytest.raises(OSError, match="OUTBRAIN_API_KEY"):
        build_from_env()


def test_build_from_env_raises_without_account_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OUTBRAIN_API_KEY", "key")
    monkeypatch.delenv("OUTBRAIN_ACCOUNT_ID", raising=False)
    with pytest.raises(OSError, match="OUTBRAIN_ACCOUNT_ID"):
        build_from_env()


def test_build_from_env_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OUTBRAIN_API_KEY", "k")
    monkeypatch.setenv("OUTBRAIN_ACCOUNT_ID", "a")
    c = build_from_env()
    assert c._account_id == "a"


# ---------------------------------------------------------------------------
# get_campaign
# ---------------------------------------------------------------------------


def test_get_campaign_parses_response(client: OutbrainClient) -> None:
    mock_resp = _make_mock(_FAKE_CAMPAIGN_DATA)
    with patch("native_ads.outbrain.httpx.get", return_value=mock_resp):
        campaign = client.get_campaign("camp-ob-1")

    assert campaign.id == "camp-ob-1"
    assert campaign.name == "Template OB Campaign"
    assert campaign.budget == {"amount": 500.0, "currency": "EUR"}
    assert campaign.targeting == {"geoLocations": ["US"]}
    assert campaign.status == "ACTIVE"
    assert campaign.raw["extra_field"] == "should_be_preserved"


def test_get_campaign_url_contains_campaign_id(client: OutbrainClient) -> None:
    mock_resp = _make_mock(_FAKE_CAMPAIGN_DATA)
    captured_url: list[str] = []

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        captured_url.append(url)
        return mock_resp

    with patch("native_ads.outbrain.httpx.get", side_effect=fake_get):
        client.get_campaign("camp-ob-1")

    assert "camp-ob-1" in captured_url[0]


# ---------------------------------------------------------------------------
# duplicate_campaign
# ---------------------------------------------------------------------------


def test_duplicate_campaign_strips_server_fields(client: OutbrainClient) -> None:
    get_resp = _make_mock(_FAKE_CAMPAIGN_DATA)
    new_camp_data = {
        "id": "camp-ob-2",
        "name": "New OB Campaign",
        "budget": {"amount": 500.0, "currency": "EUR"},
        "targeting": {"geoLocations": ["US"]},
        "status": "PENDING",
        "startTime": "2026-10-10",
    }
    post_resp = _make_mock(new_camp_data)

    captured_payload: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> MagicMock:
        captured_payload.update(kwargs.get("json", {}))
        return post_resp

    with (
        patch("native_ads.outbrain.httpx.get", return_value=get_resp),
        patch("native_ads.outbrain.httpx.post", side_effect=fake_post),
    ):
        new_campaign = client.duplicate_campaign(
            template_id="camp-ob-1",
            new_name="New OB Campaign",
            start_date=date(2026, 10, 10),
        )

    # Server-managed fields must be stripped.
    for stripped in ("id", "createdAt", "modifiedAt", "statistics"):
        assert stripped not in captured_payload, f"Field {stripped!r} was not stripped"

    # endTime must be removed (indefinite run).
    assert "endTime" not in captured_payload

    # Overrides applied.
    assert captured_payload["name"] == "New OB Campaign"
    assert captured_payload["startTime"] == "2026-10-10"
    assert captured_payload["status"] == "PENDING"

    # Extra fields preserved.
    assert captured_payload.get("extra_field") == "should_be_preserved"

    assert new_campaign.id == "camp-ob-2"
    assert new_campaign.name == "New OB Campaign"


def test_duplicate_campaign_defaults_to_tomorrow(client: OutbrainClient) -> None:
    get_resp = _make_mock(_FAKE_CAMPAIGN_DATA)
    post_resp = _make_mock(
        {"id": "c3", "name": "X", "budget": {}, "targeting": {}, "status": "PENDING"}
    )

    captured: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> MagicMock:
        captured.update(kwargs.get("json", {}))
        return post_resp

    tomorrow = (date.today() + __import__("datetime").timedelta(days=1)).isoformat()

    with (
        patch("native_ads.outbrain.httpx.get", return_value=get_resp),
        patch("native_ads.outbrain.httpx.post", side_effect=fake_post),
    ):
        client.duplicate_campaign(template_id="camp-ob-1", new_name="X")

    assert captured["startTime"] == tomorrow


# ---------------------------------------------------------------------------
# upload_image
# ---------------------------------------------------------------------------


def test_upload_image_returns_url(client: OutbrainClient, tmp_path: Path) -> None:
    img = tmp_path / "ad.png"
    img.write_bytes(b"\x89PNG\r\n")
    post_resp = _make_mock({"url": "https://images.outbrain.com/ad.png"})

    with patch("native_ads.outbrain.httpx.post", return_value=post_resp):
        url = client.upload_image(img)

    assert url == "https://images.outbrain.com/ad.png"


def test_upload_image_sends_correct_filename(client: OutbrainClient, tmp_path: Path) -> None:
    img = tmp_path / "my_creative.png"
    img.write_bytes(b"\x89PNG")
    post_resp = _make_mock({"url": "https://images.outbrain.com/my_creative.png"})

    posted_files: list[Any] = []

    def fake_post(url: str, **kwargs: Any) -> MagicMock:
        posted_files.append(kwargs.get("files"))
        return post_resp

    with patch("native_ads.outbrain.httpx.post", side_effect=fake_post):
        client.upload_image(img)

    assert posted_files
    file_tuple = posted_files[0]["file"]
    assert file_tuple[0] == "my_creative.png"
    assert file_tuple[2] == "image/png"


def test_upload_image_excludes_content_type_header(client: OutbrainClient, tmp_path: Path) -> None:
    """Content-Type must be absent so httpx sets the multipart boundary correctly."""
    img = tmp_path / "ad.png"
    img.write_bytes(b"\x89PNG")
    post_resp = _make_mock({"url": "https://images.outbrain.com/ad.png"})

    captured_headers: dict[str, str] = {}

    def fake_post(url: str, **kwargs: Any) -> MagicMock:
        captured_headers.update(kwargs.get("headers", {}))
        return post_resp

    with patch("native_ads.outbrain.httpx.post", side_effect=fake_post):
        client.upload_image(img)

    assert "Content-Type" not in captured_headers


# ---------------------------------------------------------------------------
# create_creative
# ---------------------------------------------------------------------------


def test_create_creative_returns_creative(client: OutbrainClient, tmp_path: Path) -> None:
    img = tmp_path / "ad.png"
    img.write_bytes(b"\x89PNG")

    upload_resp = _make_mock({"url": "https://images.outbrain.com/ad.png"})
    link_resp = _make_mock(
        {
            "id": "promo-1",
            "title": "Lose Weight Now",
            "url": "https://example.com/lp",
            "status": "ACTIVE",
        }
    )

    call_count = 0

    def fake_post(url: str, **kwargs: Any) -> MagicMock:
        nonlocal call_count
        call_count += 1
        if "images" in url:
            return upload_resp
        return link_resp

    with patch("native_ads.outbrain.httpx.post", side_effect=fake_post):
        creative = client.create_creative(
            campaign_id="camp-ob-1",
            title="Lose Weight Now",
            landing_url="https://example.com/lp",
            image_path=img,
        )

    assert creative.id == "promo-1"
    assert creative.campaign_id == "camp-ob-1"
    assert creative.title == "Lose Weight Now"
    assert creative.image_url == "https://images.outbrain.com/ad.png"
    assert creative.status == "ACTIVE"


# ---------------------------------------------------------------------------
# validate_campaign
# ---------------------------------------------------------------------------


def test_validate_campaign_warns_on_zero_budget() -> None:
    campaign = OutbrainCampaign(
        id="c1",
        name="Zero Budget",
        budget={"amount": 0},
        targeting={"geo": ["US"]},
        status="ACTIVE",
        raw={},
    )
    warnings = OutbrainClient(api_key="k", account_id="a").validate_campaign(campaign)
    assert any("budget" in w for w in warnings)


def test_validate_campaign_warns_on_missing_targeting() -> None:
    campaign = OutbrainCampaign(
        id="c1",
        name="No Targeting",
        budget={"amount": 100},
        targeting={},
        status="ACTIVE",
        raw={},
    )
    warnings = OutbrainClient(api_key="k", account_id="a").validate_campaign(campaign)
    assert any("targeting" in w for w in warnings)


def test_validate_campaign_warns_on_unexpected_status() -> None:
    campaign = OutbrainCampaign(
        id="c1",
        name="Bad Status",
        budget={"amount": 100},
        targeting={"geo": ["US"]},
        status="SUSPENDED",
        raw={},
    )
    warnings = OutbrainClient(api_key="k", account_id="a").validate_campaign(campaign)
    assert any("status" in w for w in warnings)


def test_validate_campaign_no_warnings_when_valid() -> None:
    campaign = OutbrainCampaign(
        id="c1",
        name="Good Campaign",
        budget={"amount": 500.0, "currency": "EUR"},
        targeting={"geoLocations": ["US"]},
        status="ACTIVE",
        raw={},
    )
    warnings = OutbrainClient(api_key="k", account_id="a").validate_campaign(campaign)
    assert warnings == []
