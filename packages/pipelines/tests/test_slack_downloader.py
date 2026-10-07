"""Unit tests for the Slack native-ad monitor and Dropbox zip helper.

No real Slack or HTTP calls are made — all external I/O is mocked.
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from integrations.dropbox import download_normal_pngs, parse_dropbox_url
from integrations.slack import SlackNativeAdMonitor, build_from_env


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_monitor(tmp_path: Path) -> SlackNativeAdMonitor:
    return SlackNativeAdMonitor(
        bot_token="xoxb-fake",
        channel_id="CFAKE",
        download_dir=tmp_path / "downloads",
        oldest_ts="1700000000.000000",
    )


def _make_zip(entries: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


def _mock_slack_resp(messages: list) -> MagicMock:
    resp = MagicMock()
    resp.data = {"messages": messages}
    return resp


# ---------------------------------------------------------------------------
# parse_dropbox_url
# ---------------------------------------------------------------------------


def test_parse_dropbox_url_extracts_url() -> None:
    mrkdwn = "<https://www.dropbox.com/scl/fo/abc/def?rlkey=xyz&amp;dl=0|view>"
    assert parse_dropbox_url(mrkdwn) == "https://www.dropbox.com/scl/fo/abc/def?rlkey=xyz&dl=0"


def test_parse_dropbox_url_unescapes_ampersands() -> None:
    mrkdwn = "<https://www.dropbox.com/scl/fo/x?a=1&amp;b=2&amp;dl=0>"
    url = parse_dropbox_url(mrkdwn)
    assert "&amp;" not in url
    assert "a=1&b=2" in url


def test_parse_dropbox_url_returns_none_if_no_dropbox() -> None:
    assert parse_dropbox_url("no url here") is None
    assert parse_dropbox_url("<https://example.com/foo>") is None


# ---------------------------------------------------------------------------
# download_normal_pngs
# ---------------------------------------------------------------------------


def test_download_normal_pngs_extracts_correct_files(tmp_path: Path) -> None:
    fake_png = b"\x89PNG fake"
    fake_zip = _make_zip({
        "normal/creative_v1.png": fake_png,
        "normal/creative_v2.png": fake_png,
        "1_1/creative_v1.png": b"other",
        "landscape/creative_v1.png": b"other",
    })
    with patch("integrations.dropbox.httpx.Client") as mock_cls:
        mock_r = MagicMock()
        mock_r.content = fake_zip
        mock_r.raise_for_status = MagicMock()
        mock_cls.return_value.__enter__.return_value.get.return_value = mock_r
        paths = download_normal_pngs("https://www.dropbox.com/scl/fo/test?dl=0", tmp_path / "out")
    assert len(paths) == 2
    assert all(p.suffix == ".png" for p in paths)
    for p in paths:
        assert p.read_bytes() == fake_png


def test_download_normal_pngs_skips_existing(tmp_path: Path) -> None:
    existing = tmp_path / "out" / "creative_v1.png"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"already here")
    fake_zip = _make_zip({"normal/creative_v1.png": b"new content"})
    with patch("integrations.dropbox.httpx.Client") as mock_cls:
        mock_r = MagicMock()
        mock_r.content = fake_zip
        mock_r.raise_for_status = MagicMock()
        mock_cls.return_value.__enter__.return_value.get.return_value = mock_r
        paths = download_normal_pngs("https://www.dropbox.com/scl/fo/test?dl=0", tmp_path / "out")
    assert len(paths) == 1
    assert existing.read_bytes() == b"already here"


def test_download_normal_pngs_returns_empty_when_no_normal_folder(tmp_path: Path) -> None:
    fake_zip = _make_zip({"1_1/creative.png": b"png", "landscape/creative.png": b"png"})
    with patch("integrations.dropbox.httpx.Client") as mock_cls:
        mock_r = MagicMock()
        mock_r.content = fake_zip
        mock_r.raise_for_status = MagicMock()
        mock_cls.return_value.__enter__.return_value.get.return_value = mock_r
        paths = download_normal_pngs("https://www.dropbox.com/scl/fo/test?dl=0", tmp_path / "out")
    assert paths == []


# ---------------------------------------------------------------------------
# build_from_env
# ---------------------------------------------------------------------------


def test_build_from_env_raises_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    with pytest.raises(EnvironmentError, match="SLACK_BOT_TOKEN"):
        build_from_env()


def test_build_from_env_uses_env_channel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setenv("SLACK_CHANNEL_ID", "CTEST999")
    assert build_from_env().channel_id == "CTEST999"


# ---------------------------------------------------------------------------
# SlackNativeAdMonitor.poll_once
# ---------------------------------------------------------------------------


def test_poll_once_no_messages(tmp_path: Path) -> None:
    monitor = _make_monitor(tmp_path)
    with patch.object(monitor._client, "conversations_history",
                      return_value=_mock_slack_resp([])):
        assert monitor.poll_once() == []


def test_poll_once_ignores_non_native_ms_attachments(tmp_path: Path) -> None:
    monitor = _make_monitor(tmp_path)
    msgs = [{"ts": "1700000100.000000", "text": "x",
             "attachments": [{"title": "OTHER_123", "text": ""}]}]
    with patch.object(monitor._client, "conversations_history",
                      return_value=_mock_slack_resp(msgs)):
        assert monitor.poll_once() == []


def test_poll_once_ignores_native_ms_without_dropbox_url(tmp_path: Path) -> None:
    monitor = _make_monitor(tmp_path)
    msgs = [{"ts": "1700000200.000000", "text": "x",
             "attachments": [{"title": "NATIVE_MS_999", "text": "no url"}]}]
    with patch.object(monitor._client, "conversations_history",
                      return_value=_mock_slack_resp(msgs)):
        assert monitor.poll_once() == []


def test_poll_once_downloads_pngs_for_native_ms(tmp_path: Path) -> None:
    monitor = _make_monitor(tmp_path)
    msgs = [
        {
            "ts": "1774945491.445959",
            "text": "New Ad ist ready to test",
            "attachments": [{
                "title": "NATIVE_MS_2173_STATIC_TESTVERGLEICH_BINGE_EATING",
                "text": "<https://www.dropbox.com/scl/fo/abc/def?rlkey=xyz&amp;dl=0|view>",
            }],
        }
    ]
    fake_zip = _make_zip({
        "normal/NATIVE_MS_2173_V1.png": b"\x89PNG v1",
        "normal/NATIVE_MS_2173_V2.png": b"\x89PNG v2",
        "1_1/NATIVE_MS_2173_V1.png": b"other",
    })
    with (
        patch.object(monitor._client, "conversations_history",
                     return_value=_mock_slack_resp(msgs)),
        patch("integrations.dropbox.httpx.Client") as mock_http_cls,
    ):
        mock_http = MagicMock()
        mock_http_cls.return_value.__enter__.return_value = mock_http
        mock_r = MagicMock()
        mock_r.content = fake_zip
        mock_r.raise_for_status = MagicMock()
        mock_http.get.return_value = mock_r
        result = monitor.poll_once()
    assert len(result) == 2
    assert all(p.suffix == ".png" for p in result)
    assert all("NATIVE_MS_2173_STATIC_TESTVERGLEICH_BINGE_EATING" in str(p) for p in result)


def test_poll_once_advances_cursor(tmp_path: Path) -> None:
    monitor = _make_monitor(tmp_path)
    ts1, ts2 = "1700000300.000000", "1700000400.000000"
    msgs = [
        {"ts": ts2, "text": "x", "attachments": [{"title": "NATIVE_MS_A", "text": "no url"}]},
        {"ts": ts1, "text": "x", "attachments": [{"title": "NATIVE_MS_B", "text": "no url"}]},
    ]
    with patch.object(monitor._client, "conversations_history",
                      return_value=_mock_slack_resp(msgs)):
        monitor.poll_once()
    assert monitor._cursor_ts == ts2
