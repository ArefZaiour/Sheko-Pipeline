"""Unit tests for the Slack native-ad monitor + Dropbox pipeline.

All tests use unittest.mock to avoid real Slack, HTTP, or Dropbox API calls.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from integrations.dropbox import DropboxSharedFolderClient
from integrations.slack import (
    SlackNativeAdMonitor,
    _extract_first_url,
    _is_native_ad_message,
    _safe_filename,
    build_from_env,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_monitor(tmp_path: Path) -> tuple[SlackNativeAdMonitor, MagicMock]:
    """Return a monitor wired to a mock DropboxSharedFolderClient."""
    dropbox = MagicMock(spec=DropboxSharedFolderClient)
    monitor = SlackNativeAdMonitor(
        bot_token="xoxb-fake",
        dropbox_client=dropbox,
        channel_id="CFAKE",
        download_dir=tmp_path / "downloads",
        oldest_ts="1700000000.000000",
    )
    return monitor, dropbox


# ---------------------------------------------------------------------------
# _safe_filename
# ---------------------------------------------------------------------------


def test_safe_filename_strips_spaces() -> None:
    assert _safe_filename("hello world.png") == "hello_world.png"


def test_safe_filename_strips_slashes() -> None:
    assert _safe_filename("path/to/file.png") == "path_to_file.png"


def test_safe_filename_keeps_dots_dashes_underscores() -> None:
    assert _safe_filename("my-file_v1.0.png") == "my-file_v1.0.png"


# ---------------------------------------------------------------------------
# _is_native_ad_message
# ---------------------------------------------------------------------------


def test_is_native_ad_message_matches() -> None:
    assert _is_native_ad_message("New creative package Native_MS uploaded")


def test_is_native_ad_message_case_insensitive() -> None:
    assert _is_native_ad_message("native_fb creative ready")


def test_is_native_ad_message_no_match() -> None:
    assert not _is_native_ad_message("Regular message without the pattern")


# ---------------------------------------------------------------------------
# _extract_first_url
# ---------------------------------------------------------------------------


def test_extract_first_url_returns_url() -> None:
    text = "Native_MS package: https://example.com/abc123 see above"
    assert _extract_first_url(text) == "https://example.com/abc123"


def test_extract_first_url_returns_none_when_missing() -> None:
    assert _extract_first_url("no url here") is None


# ---------------------------------------------------------------------------
# build_from_env
# ---------------------------------------------------------------------------


def test_build_from_env_raises_without_slack_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    with pytest.raises(EnvironmentError, match="SLACK_BOT_TOKEN"):
        build_from_env()


def test_build_from_env_raises_without_dropbox_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.delenv("DROPBOX_ACCESS_TOKEN", raising=False)
    with pytest.raises(EnvironmentError, match="DROPBOX_ACCESS_TOKEN"):
        build_from_env()


# ---------------------------------------------------------------------------
# SlackNativeAdMonitor.poll_once — message filtering
# ---------------------------------------------------------------------------


def test_poll_once_no_messages(tmp_path: Path) -> None:
    monitor, _ = _make_monitor(tmp_path)
    mock_resp = MagicMock()
    mock_resp.get.return_value = []
    with patch.object(monitor._client, "conversations_history", return_value=mock_resp):
        result = monitor.poll_once()
    assert result == []


def test_poll_once_ignores_non_native_messages(tmp_path: Path) -> None:
    monitor, _ = _make_monitor(tmp_path)
    mock_resp = MagicMock()
    mock_resp.get.return_value = [
        {"ts": "1700000001.000000", "text": "Just a regular message"}
    ]
    with patch.object(monitor._client, "conversations_history", return_value=mock_resp):
        result = monitor.poll_once()
    assert result == []


def test_poll_once_ignores_native_message_without_url(tmp_path: Path) -> None:
    monitor, dropbox = _make_monitor(tmp_path)
    mock_resp = MagicMock()
    mock_resp.get.return_value = [
        {"ts": "1700000002.000000", "text": "Native_MS package ready (no url)"}
    ]
    with patch.object(monitor._client, "conversations_history", return_value=mock_resp):
        result = monitor.poll_once()
    assert result == []
    dropbox.list_pngs_in_normal.assert_not_called()


# ---------------------------------------------------------------------------
# SlackNativeAdMonitor._process_message — Dropbox pipeline
# ---------------------------------------------------------------------------


def test_poll_once_downloads_pngs_from_dropbox(tmp_path: Path) -> None:
    monitor, dropbox = _make_monitor(tmp_path)

    ts = "1700000100.000000"
    msg = {
        "ts": ts,
        "text": "Native_MS creative https://track.example.com/abc is ready",
    }
    mock_conv_resp = MagicMock()
    mock_conv_resp.get.return_value = [msg]

    dropbox_url = "https://www.dropbox.com/sh/abc/xyz?dl=0"
    png_names = ["creative_1.png", "creative_2.png"]
    dropbox.list_pngs_in_normal.return_value = png_names

    # Simulate download_png writing the file to disk.
    def _fake_download(folder_url: str, filename: str, dest: Path) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"fake png data")
        return True

    dropbox.download_png.side_effect = _fake_download

    with (
        patch.object(monitor._client, "conversations_history", return_value=mock_conv_resp),
        patch("integrations.slack._resolve_redirect", return_value=dropbox_url),
    ):
        result = monitor.poll_once()

    assert len(result) == 2
    assert all(p.suffix == ".png" for p in result)
    dropbox.list_pngs_in_normal.assert_called_once_with(dropbox_url)
    assert dropbox.download_png.call_count == 2


def test_poll_once_skips_non_dropbox_redirect(tmp_path: Path) -> None:
    monitor, dropbox = _make_monitor(tmp_path)

    msg = {
        "ts": "1700000200.000000",
        "text": "Native_MS creative https://other-host.com/link",
    }
    mock_conv_resp = MagicMock()
    mock_conv_resp.get.return_value = [msg]

    with (
        patch.object(monitor._client, "conversations_history", return_value=mock_conv_resp),
        patch("integrations.slack._resolve_redirect", return_value="https://other-host.com/final"),
    ):
        result = monitor.poll_once()

    assert result == []
    dropbox.list_pngs_in_normal.assert_not_called()


def test_poll_once_advances_cursor(tmp_path: Path) -> None:
    monitor, dropbox = _make_monitor(tmp_path)

    ts1 = "1700000300.000000"
    ts2 = "1700000400.000000"
    messages = [
        {"ts": ts2, "text": "Native_MS https://t.co/a"},  # newest (Slack order)
        {"ts": ts1, "text": "Native_MS https://t.co/b"},  # oldest
    ]
    mock_conv_resp = MagicMock()
    mock_conv_resp.get.return_value = messages
    dropbox.list_pngs_in_normal.return_value = []

    with (
        patch.object(monitor._client, "conversations_history", return_value=mock_conv_resp),
        patch("integrations.slack._resolve_redirect", return_value="https://www.dropbox.com/sh/x/y"),
    ):
        monitor.poll_once()

    # After reversing, ts2 is the newest; cursor must advance to ts2.
    assert monitor._cursor_ts == ts2
