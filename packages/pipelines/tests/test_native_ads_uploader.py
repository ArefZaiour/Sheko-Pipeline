"""Tests for Outbrain and Taboola campaign uploaders.

All HTTP calls are intercepted with httpx.MockTransport so no real credentials
are needed to run the test suite.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _png(tmp_path: Path, name: str = "ad.png") -> Path:
    """Write a minimal PNG file and return its path."""
    p = tmp_path / name
    # 1×1 red pixel PNG (89 bytes)
    p.write_bytes(
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00"
        b"\x00\x01\x01\x00\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    return p


# ---------------------------------------------------------------------------
# Outbrain tests
# ---------------------------------------------------------------------------

class TestOutbrainUploader:
    def _make_client(self) -> "OutbrainUploader":
        from integrations.outbrain import OutbrainUploader
        return OutbrainUploader(
            api_key="test_key",
            account_id="acc123",
            template_campaign_id="tpl456",
        )

    def test_from_env_missing_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("OUTBRAIN_API_KEY", raising=False)
        from integrations.outbrain import OutbrainUploader
        with pytest.raises(EnvironmentError, match="OUTBRAIN_API_KEY"):
            OutbrainUploader.from_env()

    def test_from_env_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OUTBRAIN_API_KEY", "k")
        monkeypatch.setenv("OUTBRAIN_ACCOUNT_ID", "a")
        monkeypatch.setenv("OUTBRAIN_TEMPLATE_CAMPAIGN_ID", "t")
        from integrations.outbrain import OutbrainUploader
        client = OutbrainUploader.from_env()
        assert client.account_id == "a"
        assert client.template_campaign_id == "t"
        client.close()

    def test_upload_campaign_happy_path(self, tmp_path: Path) -> None:
        import httpx
        from integrations.outbrain import OutbrainUploader

        png = _png(tmp_path)

        call_log: list[str] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            call_log.append(f"{request.method} {path}")

            if request.method == "GET" and "campaigns/tpl456" in path:
                return httpx.Response(200, json={
                    "id": "tpl456",
                    "name": "Template",
                    "budget": {"amount": 100},
                    "targeting": {"geo": ["DE"]},
                    "status": "ACTIVE",
                })
            if request.method == "POST" and "/campaigns" in path and "promotedLinks" not in path:
                return httpx.Response(200, json={"id": "new_camp_001", "name": "NATIVE_MS_2173"})
            if request.method == "POST" and "/images" in path:
                return httpx.Response(200, json={"url": "https://cdn.outbrain.com/img/test.png"})
            if request.method == "POST" and "promotedLinks" in path:
                return httpx.Response(200, json={"id": "creative_001"})
            if request.method == "GET" and "campaigns/new_camp_001" in path:
                return httpx.Response(200, json={
                    "id": "new_camp_001",
                    "budget": {"amount": 100},
                    "targeting": {"geo": ["DE"]},
                    "status": "INACTIVE",
                })
            if request.method == "PUT" and "campaigns/new_camp_001" in path:
                return httpx.Response(200, json={"id": "new_camp_001", "status": "ACTIVE"})

            return httpx.Response(404, json={"error": f"unmatched: {path}"})

        client = OutbrainUploader("k", "acc123", "tpl456")
        client._http = httpx.Client(
            base_url="https://api.outbrain.com/amplify/v0.1",
            transport=httpx.MockTransport(_handler),
        )

        result = client.upload_campaign(
            "NATIVE_MS_2173",
            [png],
            go_live_date=date(2026, 4, 2),
        )
        client.close()

        assert result["campaign_id"] == "new_camp_001"
        assert result["go_live_date"] == "2026-04-02"
        assert len(result["creatives"]) == 1
        assert "GET /amplify/v0.1/marketers/acc123/campaigns/tpl456" in call_log

    def test_validation_fails_on_missing_budget(self, tmp_path: Path) -> None:
        import httpx
        from integrations.outbrain import OutbrainUploader

        png = _png(tmp_path)

        def _handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if request.method == "GET" and "campaigns/tpl456" in path:
                return httpx.Response(200, json={
                    "id": "tpl456",
                    "name": "Template",
                    "budget": {"amount": 100},
                    "targeting": {"geo": ["DE"]},
                    "status": "ACTIVE",
                })
            if request.method == "POST" and "/campaigns" in path:
                return httpx.Response(200, json={"id": "camp_bad"})
            if request.method == "POST" and "/images" in path:
                return httpx.Response(200, json={"url": "https://cdn.outbrain.com/img/test.png"})
            if request.method == "POST" and "promotedLinks" in path:
                return httpx.Response(200, json={"id": "c1"})
            if request.method == "GET" and "campaigns/camp_bad" in path:
                # Budget missing — should trigger validation error
                return httpx.Response(200, json={"id": "camp_bad", "targeting": {"geo": ["DE"]}, "status": "INACTIVE"})
            return httpx.Response(404)

        client = OutbrainUploader("k", "acc123", "tpl456")
        client._http = httpx.Client(
            base_url="https://api.outbrain.com/amplify/v0.1",
            transport=httpx.MockTransport(_handler),
        )
        with pytest.raises(ValueError, match="budget missing"):
            client.upload_campaign("NATIVE_MS_BAD", [png])
        client.close()


# ---------------------------------------------------------------------------
# Taboola tests
# ---------------------------------------------------------------------------

class TestTaboolaUploader:
    def _make_client(self) -> "TaboolaUploader":
        from integrations.taboola import TaboolaUploader
        return TaboolaUploader(
            client_id="cid",
            client_secret="csec",
            account_id="sheko-gmbh",
            template_campaign_id="tpl789",
        )

    def test_from_env_missing_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TABOOLA_CLIENT_ID", raising=False)
        from integrations.taboola import TaboolaUploader
        with pytest.raises(EnvironmentError, match="TABOOLA_CLIENT_ID"):
            TaboolaUploader.from_env()

    def test_from_env_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TABOOLA_CLIENT_ID", "ci")
        monkeypatch.setenv("TABOOLA_CLIENT_SECRET", "cs")
        monkeypatch.setenv("TABOOLA_ACCOUNT_ID", "acc")
        monkeypatch.setenv("TABOOLA_TEMPLATE_CAMPAIGN_ID", "t")
        from integrations.taboola import TaboolaUploader
        client = TaboolaUploader.from_env()
        assert client.account_id == "acc"
        client.close()

    def test_upload_campaign_happy_path(self, tmp_path: Path) -> None:
        import httpx
        from integrations.taboola import TaboolaUploader, TOKEN_URL

        png = _png(tmp_path)
        call_log: list[str] = []

        def _api_handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            call_log.append(f"{request.method} {path}")

            if request.method == "GET" and "tpl789" in path:
                return httpx.Response(200, json={
                    "id": "tpl789",
                    "name": "Template",
                    "cpc": 0.35,
                    "daily_cap": 50.0,
                    "country_targeting": {"type": "INCLUDE", "value": ["DE"]},
                    "platform_targeting": {"type": "INCLUDE", "value": ["DESK"]},
                    "status": "PAUSED",
                })
            if request.method == "POST" and path.endswith("/campaigns/"):
                return httpx.Response(200, json={"id": "new_tb_001", "name": "NATIVE_MS_2173"})
            if request.method == "POST" and "items" in path:
                return httpx.Response(200, json={"id": "item_001", "thumbnail_url": "https://cdn.taboola.com/t.png"})
            if request.method == "GET" and "new_tb_001" in path:
                return httpx.Response(200, json={
                    "id": "new_tb_001",
                    "cpc": 0.35,
                    "daily_cap": 50.0,
                    "country_targeting": {"type": "INCLUDE", "value": ["DE"]},
                    "status": "PAUSED",
                })
            if request.method == "PUT" and "new_tb_001" in path:
                return httpx.Response(200, json={"id": "new_tb_001", "status": "RUNNING"})

            return httpx.Response(404, json={"error": f"unmatched: {path}"})

        client = TaboolaUploader("cid", "csec", "sheko-gmbh", "tpl789")
        client._access_token = "pre_set_token"  # skip OAuth2 exchange
        client._http = httpx.Client(
            base_url="https://backstage.taboola.com/backstage/api/1.0",
            transport=httpx.MockTransport(_api_handler),
            headers={"Authorization": "Bearer pre_set_token"},
        )

        result = client.upload_campaign(
            "NATIVE_MS_2173",
            [png],
            go_live_date=date(2026, 4, 2),
        )
        client.close()

        assert result["campaign_id"] == "new_tb_001"
        assert result["go_live_date"] == "2026-04-02"
        assert len(result["creatives"]) == 1

    def test_validation_fails_on_missing_bid(self, tmp_path: Path) -> None:
        import httpx
        from integrations.taboola import TaboolaUploader

        png = _png(tmp_path)

        def _handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if "tpl789" in path and request.method == "GET":
                return httpx.Response(200, json={
                    "id": "tpl789",
                    "cpc": 0.35,
                    "daily_cap": 50.0,
                    "country_targeting": {"type": "INCLUDE", "value": ["DE"]},
                    "status": "PAUSED",
                })
            if path.endswith("/campaigns/") and request.method == "POST":
                return httpx.Response(200, json={"id": "camp_bad"})
            if "items" in path:
                return httpx.Response(200, json={"id": "i1", "thumbnail_url": "https://cdn.t.com/t.png"})
            if "camp_bad" in path and request.method == "GET":
                # Missing cpc — should fail validation
                return httpx.Response(200, json={
                    "id": "camp_bad",
                    "country_targeting": {"type": "INCLUDE", "value": ["DE"]},
                    "status": "PAUSED",
                })
            return httpx.Response(404)

        client = TaboolaUploader("cid", "csec", "sheko-gmbh", "tpl789")
        client._access_token = "tok"
        client._http = httpx.Client(
            base_url="https://backstage.taboola.com/backstage/api/1.0",
            transport=httpx.MockTransport(_handler),
            headers={"Authorization": "Bearer tok"},
        )
        with pytest.raises(ValueError, match="cpc.*bid.*missing"):
            client.upload_campaign("NATIVE_MS_BAD", [png])
        client.close()

    def test_oauth_token_fetched(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Token endpoint is called exactly once per session."""
        import httpx
        from integrations.taboola import TaboolaUploader

        token_calls = 0

        def _token_post(*args: object, **kwargs: object) -> httpx.Response:
            nonlocal token_calls
            token_calls += 1
            resp = httpx.Response(200, json={"access_token": "fresh_token"})
            # httpx.Response needs a request attached for raise_for_status()
            resp.request = httpx.Request("POST", "https://authentication.taboola.com/calibrate/oauth/token")
            return resp

        def _api_handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if "tpl789" in path:
                return httpx.Response(200, json={
                    "id": "tpl789", "cpc": 0.3, "daily_cap": 50.0,
                    "country_targeting": {"value": ["DE"]}, "status": "PAUSED",
                })
            if path.endswith("/campaigns/"):
                return httpx.Response(200, json={"id": "camp1"})
            if "items" in path:
                return httpx.Response(200, json={"id": "i1", "thumbnail_url": "https://x.com/t.png"})
            if "camp1" in path and request.method == "GET":
                return httpx.Response(200, json={
                    "id": "camp1", "cpc": 0.3, "daily_cap": 50.0,
                    "country_targeting": {"value": ["DE"]}, "status": "PAUSED",
                })
            if "camp1" in path and request.method == "PUT":
                return httpx.Response(200, json={"id": "camp1", "status": "RUNNING"})
            return httpx.Response(404)

        png = _png(tmp_path)
        client = TaboolaUploader("cid", "csec", "sheko-gmbh", "tpl789")
        client._http = httpx.Client(
            base_url="https://backstage.taboola.com/backstage/api/1.0",
            transport=httpx.MockTransport(_api_handler),
        )
        monkeypatch.setattr("integrations.taboola.httpx.post", _token_post)

        client.upload_campaign("NATIVE_MS_OAUTH_TEST", [png])
        client.upload_campaign("NATIVE_MS_OAUTH_TEST_2", [png])
        client.close()

        assert token_calls == 1, "Token should be fetched only once per session"
