"""Unit tests for the Taboola Backstage API client.

All tests use unittest.mock — no real HTTP calls or credentials required.
"""

from __future__ import annotations

import time
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from native_ads.taboola import TaboolaCampaign, TaboolaClient, build_from_env

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_TOKEN_RESPONSE = {
    "access_token": "test-access-token",
    "expires_in": 3600,
    "token_type": "Bearer",
}

_FAKE_CAMPAIGN_DATA: dict[str, Any] = {
    "id": "camp-1",
    "advertiser_id": "adv-99",
    "approval_state": "APPROVED",
    "is_active": True,
    "spending": 500.0,
    "name": "Template Campaign",
    "budget": 1000.0,
    "daily_cap": "100",
    "status": "RUNNING",
    "start_date": "2026-10-07",
    "end_date": "2026-12-31",
    "extra_field": "should_be_preserved",
}


def _make_post_mock(json_body: dict[str, Any]) -> MagicMock:
    m = MagicMock()
    m.raise_for_status = MagicMock()
    m.json.return_value = json_body
    return m


def _make_get_mock(json_body: dict[str, Any]) -> MagicMock:
    m = MagicMock()
    m.raise_for_status = MagicMock()
    m.json.return_value = json_body
    return m


@pytest.fixture()
def client() -> TaboolaClient:
    return TaboolaClient(
        client_id="test-client-id",
        client_secret="test-client-secret",
        account_id="test-account",
    )


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_raises_without_client_id() -> None:
    with pytest.raises(OSError, match="TABOOLA_CLIENT_ID"):
        TaboolaClient(client_id="", client_secret="s", account_id="a")


def test_raises_without_client_secret() -> None:
    with pytest.raises(OSError, match="TABOOLA_CLIENT_SECRET"):
        TaboolaClient(client_id="c", client_secret="", account_id="a")


def test_raises_without_account_id() -> None:
    with pytest.raises(OSError, match="TABOOLA_ACCOUNT_ID"):
        TaboolaClient(client_id="c", client_secret="s", account_id="")


# ---------------------------------------------------------------------------
# build_from_env
# ---------------------------------------------------------------------------


def test_build_from_env_raises_on_missing_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TABOOLA_CLIENT_ID", raising=False)
    monkeypatch.delenv("TABOOLA_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("TABOOLA_ACCOUNT_ID", raising=False)
    with pytest.raises(OSError, match="TABOOLA_CLIENT_ID"):
        build_from_env()


def test_build_from_env_succeeds_with_all_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TABOOLA_CLIENT_ID", "cid")
    monkeypatch.setenv("TABOOLA_CLIENT_SECRET", "csec")
    monkeypatch.setenv("TABOOLA_ACCOUNT_ID", "acct")
    c = build_from_env()
    assert c._client_id == "cid"
    assert c._account_id == "acct"


# ---------------------------------------------------------------------------
# Token acquisition and caching
# ---------------------------------------------------------------------------


def test_get_access_token_fetches_and_caches(client: TaboolaClient) -> None:
    mock_resp = _make_post_mock(_FAKE_TOKEN_RESPONSE)
    with patch("native_ads.taboola.httpx.post", return_value=mock_resp) as mock_post:
        token1 = client._get_access_token()
        token2 = client._get_access_token()

    assert token1 == "test-access-token"
    assert token2 == "test-access-token"
    # Second call should use cached token — only one HTTP call.
    assert mock_post.call_count == 1


def test_get_access_token_refreshes_when_expired(client: TaboolaClient) -> None:
    mock_resp = _make_post_mock(_FAKE_TOKEN_RESPONSE)
    with patch("native_ads.taboola.httpx.post", return_value=mock_resp) as mock_post:
        client._get_access_token()
        # Force expiry.
        client._token_expiry = time.time() - 1
        client._get_access_token()

    assert mock_post.call_count == 2


# ---------------------------------------------------------------------------
# get_campaign
# ---------------------------------------------------------------------------


def test_get_campaign_parses_response(client: TaboolaClient) -> None:
    token_resp = _make_post_mock(_FAKE_TOKEN_RESPONSE)
    camp_resp = _make_get_mock(_FAKE_CAMPAIGN_DATA)

    with (
        patch("native_ads.taboola.httpx.post", return_value=token_resp),
        patch("native_ads.taboola.httpx.get", return_value=camp_resp),
    ):
        campaign = client.get_campaign("camp-1")

    assert campaign.id == "camp-1"
    assert campaign.name == "Template Campaign"
    assert campaign.budget == pytest.approx(1000.0)
    assert campaign.daily_budget == pytest.approx(100.0)
    assert campaign.status == "RUNNING"
    assert campaign.start_date == "2026-10-07"
    assert campaign.raw["extra_field"] == "should_be_preserved"


def test_get_campaign_no_daily_cap(client: TaboolaClient) -> None:
    data = {**_FAKE_CAMPAIGN_DATA, "daily_cap": None}
    token_resp = _make_post_mock(_FAKE_TOKEN_RESPONSE)
    camp_resp = _make_get_mock(data)

    with (
        patch("native_ads.taboola.httpx.post", return_value=token_resp),
        patch("native_ads.taboola.httpx.get", return_value=camp_resp),
    ):
        campaign = client.get_campaign("camp-1")

    assert campaign.daily_budget is None


# ---------------------------------------------------------------------------
# duplicate_campaign
# ---------------------------------------------------------------------------


def test_duplicate_campaign_strips_server_fields(client: TaboolaClient) -> None:
    token_resp = _make_post_mock(_FAKE_TOKEN_RESPONSE)
    camp_resp = _make_get_mock(_FAKE_CAMPAIGN_DATA)

    new_camp_data = {
        "id": "camp-2",
        "advertiser_id": "adv-99",
        "name": "New Campaign",
        "budget": 1000.0,
        "daily_cap": "100",
        "status": "PENDING",
        "start_date": "2026-10-10",
        "approval_state": "PENDING_REVIEW",
        "is_active": False,
        "spending": 0.0,
    }
    create_resp = _make_post_mock(new_camp_data)

    captured_payload: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> MagicMock:
        if "oauth" in url:
            return token_resp
        captured_payload.update(kwargs.get("json", {}))
        return create_resp

    with (
        patch("native_ads.taboola.httpx.post", side_effect=fake_post),
        patch("native_ads.taboola.httpx.get", return_value=camp_resp),
    ):
        new_campaign = client.duplicate_campaign(
            template_id="camp-1",
            new_name="New Campaign",
            start_date=date(2026, 10, 10),
        )

    # Server-managed fields must be stripped from the POST body.
    for stripped in ("id", "advertiser_id", "approval_state", "is_active", "spending"):
        assert stripped not in captured_payload, f"Field {stripped!r} was not stripped"

    # end_date must also be removed (runs indefinitely).
    assert "end_date" not in captured_payload

    # Overrides applied.
    assert captured_payload["name"] == "New Campaign"
    assert captured_payload["start_date"] == "2026-10-10"
    assert captured_payload["status"] == "PENDING"

    # Extra fields preserved.
    assert captured_payload.get("extra_field") == "should_be_preserved"

    assert new_campaign.id == "camp-2"
    assert new_campaign.name == "New Campaign"


def test_duplicate_campaign_defaults_to_tomorrow(client: TaboolaClient) -> None:
    token_resp = _make_post_mock(_FAKE_TOKEN_RESPONSE)
    camp_resp = _make_get_mock(_FAKE_CAMPAIGN_DATA)
    create_resp = _make_post_mock(
        {"id": "c3", "name": "X", "budget": 0, "status": "PENDING", "start_date": ""}
    )

    captured: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> MagicMock:
        if "oauth" in url:
            return token_resp
        captured.update(kwargs.get("json", {}))
        return create_resp

    tomorrow = (date.today() + __import__("datetime").timedelta(days=1)).isoformat()

    with (
        patch("native_ads.taboola.httpx.post", side_effect=fake_post),
        patch("native_ads.taboola.httpx.get", return_value=camp_resp),
    ):
        client.duplicate_campaign(template_id="camp-1", new_name="X")

    assert captured["start_date"] == tomorrow


# ---------------------------------------------------------------------------
# _upload_image
# ---------------------------------------------------------------------------


def test_upload_image_returns_thumbnail_url(
    client: TaboolaClient,
    tmp_path: Path,
) -> None:
    img = tmp_path / "creative.png"
    img.write_bytes(b"\x89PNG\r\n\x1a\n")  # minimal PNG header

    token_resp = _make_post_mock(_FAKE_TOKEN_RESPONSE)
    upload_resp = _make_post_mock({"thumbnail_url": "https://cdn.taboola.com/img/creative.png"})

    def fake_post(url: str, **kwargs: Any) -> MagicMock:
        if "oauth" in url:
            return token_resp
        return upload_resp

    with patch("native_ads.taboola.httpx.post", side_effect=fake_post):
        url = client._upload_image(img)

    assert url == "https://cdn.taboola.com/img/creative.png"


def test_upload_image_sends_correct_filename(
    client: TaboolaClient,
    tmp_path: Path,
) -> None:
    img = tmp_path / "my_ad.png"
    img.write_bytes(b"\x89PNG")

    token_resp = _make_post_mock(_FAKE_TOKEN_RESPONSE)
    upload_resp = _make_post_mock({"thumbnail_url": "https://cdn.taboola.com/my_ad.png"})

    posted_files: list[Any] = []

    def fake_post(url: str, **kwargs: Any) -> MagicMock:
        if "oauth" in url:
            return token_resp
        posted_files.append(kwargs.get("files"))
        return upload_resp

    with patch("native_ads.taboola.httpx.post", side_effect=fake_post):
        client._upload_image(img)

    assert posted_files, "No files posted"
    image_tuple = posted_files[0]["image"]
    assert image_tuple[0] == "my_ad.png"
    assert image_tuple[2] == "image/png"


# ---------------------------------------------------------------------------
# create_creative
# ---------------------------------------------------------------------------


def test_create_creative_returns_creative(
    client: TaboolaClient,
    tmp_path: Path,
) -> None:
    img = tmp_path / "ad.png"
    img.write_bytes(b"\x89PNG")

    token_resp = _make_post_mock(_FAKE_TOKEN_RESPONSE)
    upload_resp = _make_post_mock({"thumbnail_url": "https://cdn.taboola.com/ad.png"})
    item_resp = _make_post_mock(
        {
            "id": "item-42",
            "title": "Lose weight fast",
            "url": "https://example.com/lp",
            "thumbnail_url": "https://cdn.taboola.com/ad.png",
            "status": "RUNNING",
        }
    )

    call_count = 0

    def fake_post(url: str, **kwargs: Any) -> MagicMock:
        nonlocal call_count
        call_count += 1
        if "oauth" in url:
            return token_resp
        if "medias" in url:
            return upload_resp
        return item_resp

    with patch("native_ads.taboola.httpx.post", side_effect=fake_post):
        creative = client.create_creative(
            campaign_id="camp-1",
            title="Lose weight fast",
            landing_url="https://example.com/lp",
            image_path=img,
        )

    assert creative.id == "item-42"
    assert creative.campaign_id == "camp-1"
    assert creative.title == "Lose weight fast"
    assert creative.thumbnail_url == "https://cdn.taboola.com/ad.png"
    assert creative.status == "RUNNING"


# ---------------------------------------------------------------------------
# validate_campaign
# ---------------------------------------------------------------------------


def test_validate_campaign_warns_on_zero_budget() -> None:
    campaign = TaboolaCampaign(
        id="c1",
        name="Zero Budget",
        budget=0.0,
        daily_budget=None,
        status="RUNNING",
        start_date="2026-10-07",
        raw={},
    )
    warnings = TaboolaClient(
        client_id="x", client_secret="y", account_id="z"
    ).validate_campaign(campaign)
    assert any("budget" in w for w in warnings)


def test_validate_campaign_warns_on_unknown_status() -> None:
    campaign = TaboolaCampaign(
        id="c1",
        name="Weird Status",
        budget=500.0,
        daily_budget=50.0,
        status="ARCHIVED",
        start_date="2026-10-07",
        raw={},
    )
    warnings = TaboolaClient(
        client_id="x", client_secret="y", account_id="z"
    ).validate_campaign(campaign)
    assert any("status" in w for w in warnings)


def test_validate_campaign_no_warnings_when_valid() -> None:
    campaign = TaboolaCampaign(
        id="c1",
        name="Good Campaign",
        budget=500.0,
        daily_budget=50.0,
        status="RUNNING",
        start_date="2026-10-07",
        raw={},
    )
    warnings = TaboolaClient(
        client_id="x", client_secret="y", account_id="z"
    ).validate_campaign(campaign)
    assert warnings == []
