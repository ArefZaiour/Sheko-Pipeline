"""Unit tests for the Slack file downloader.

These tests use unittest.mock to avoid real Slack or HTTP calls, so they run
without a valid SLACK_BOT_TOKEN.
"""
from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from integrations.slack import (
    SlackFileDownloader,
    _safe_filename,
    build_from_env,
)


# ---------------------------------------------------------------------------
# _safe_filename
# ---------------------------------------------------------------------------


def test_safe_filename_strips_spaces() -> None:
    assert _safe_filename("hello world.zip") == "hello_world.zip"


def test_safe_filename_strips_slashes() -> None:
    assert _safe_filename("path/to/file.zip") == "path_to_file.zip"


def test_safe_filename_keeps_dots_dashes_underscores() -> None:
    assert _safe_filename("my-file_v1.0.tar.gz") == "my-file_v1.0.tar.gz"


# ---------------------------------------------------------------------------
# build_from_env
# ---------------------------------------------------------------------------


def test_build_from_env_raises_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    with pytest.raises(EnvironmentError, match="SLACK_BOT_TOKEN"):
        build_from_env()


def test_build_from_env_uses_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test-token")
    monkeypatch.setenv("SLACK_CHANNEL_ID", "CTEST123")
    downloader = build_from_env()
    assert downloader.channel_id == "CTEST123"


# ---------------------------------------------------------------------------
# SlackFileDownloader.poll_once
# ---------------------------------------------------------------------------


def _make_downloader(tmp_path: Path) -> SlackFileDownloader:
    return SlackFileDownloader(
        bot_token="xoxb-fake",
        channel_id="CFAKE",
        download_dir=tmp_path / "downloads",
        oldest_ts="1700000000.000000",
    )


def test_poll_once_no_messages(tmp_path: Path) -> None:
    downloader = _make_downloader(tmp_path)
    mock_resp = MagicMock()
    mock_resp.get.return_value = []

    with patch.object(downloader._client, "conversations_history", return_value=mock_resp):
        result = downloader.poll_once()

    assert result == []


def test_poll_once_messages_without_files(tmp_path: Path) -> None:
    downloader = _make_downloader(tmp_path)
    mock_resp = MagicMock()
    mock_resp.get.return_value = [{"ts": "1700000001.000000", "text": "hello"}]

    with patch.object(downloader._client, "conversations_history", return_value=mock_resp):
        result = downloader.poll_once()

    assert result == []


def test_poll_once_downloads_file(tmp_path: Path) -> None:
    downloader = _make_downloader(tmp_path)

    # Slack API response: one message with one file attachment
    ts = str(time.time())
    msg = {
        "ts": ts,
        "text": "here is the creative package",
        "files": [
            {
                "id": "F001",
                "name": "sheko_stl_package.zip",
                "url_private_download": "https://files.slack.com/files-pri/F001/sheko_stl.zip",
            }
        ],
    }
    mock_conv_resp = MagicMock()
    mock_conv_resp.get.return_value = [msg]

    fake_content = b"PK\x03\x04fake zip content"

    with (
        patch.object(downloader._client, "conversations_history", return_value=mock_conv_resp),
        patch("integrations.slack.httpx.Client") as mock_http_cls,
    ):
        mock_http = MagicMock()
        mock_http_cls.return_value.__enter__.return_value = mock_http
        mock_resp = MagicMock()
        mock_resp.content = fake_content
        mock_http.get.return_value = mock_resp

        result = downloader.poll_once()

    assert len(result) == 1
    downloaded_path = result[0]
    assert downloaded_path.exists()
    assert downloaded_path.read_bytes() == fake_content
    assert downloaded_path.name == "sheko_stl_package.zip"


def test_poll_once_skips_existing_file(tmp_path: Path) -> None:
    downloader = _make_downloader(tmp_path)

    ts = "1700000050.000000"
    msg = {
        "ts": ts,
        "files": [
            {
                "id": "F002",
                "name": "existing.zip",
                "url_private_download": "https://files.slack.com/F002/existing.zip",
            }
        ],
    }
    mock_conv_resp = MagicMock()
    mock_conv_resp.get.return_value = [msg]

    # Pre-create the destination file.
    from datetime import datetime, timezone

    date_str = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")
    dest = tmp_path / "downloads" / date_str / "existing.zip"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"already here")

    with (
        patch.object(downloader._client, "conversations_history", return_value=mock_conv_resp),
        patch("integrations.slack.httpx.Client") as mock_http_cls,
    ):
        result = downloader.poll_once()
        # HTTP client should not have been called.
        mock_http_cls.assert_not_called()

    assert len(result) == 1
    assert result[0] == dest


def test_poll_once_advances_cursor(tmp_path: Path) -> None:
    """Cursor advances to the newest ts when messages with files are present."""
    downloader = _make_downloader(tmp_path)

    ts1 = "1700000100.000000"
    ts2 = "1700000200.000000"
    # Both messages have files so neither is filtered out.
    fake_file = {"id": "FX", "name": "x.zip", "url_private_download": "https://example.com/x.zip"}
    messages = [
        {"ts": ts2, "files": [fake_file]},  # newest
        {"ts": ts1, "files": [fake_file]},  # oldest
    ]
    mock_conv_resp = MagicMock()
    mock_conv_resp.get.return_value = messages

    fake_content = b"data"
    with (
        patch.object(downloader._client, "conversations_history", return_value=mock_conv_resp),
        patch("integrations.slack.httpx.Client") as mock_http_cls,
    ):
        mock_http = MagicMock()
        mock_http_cls.return_value.__enter__.return_value = mock_http
        mock_resp = MagicMock()
        mock_resp.content = fake_content
        mock_http.get.return_value = mock_resp
        downloader.poll_once()

    # After reversing, ts2 is the newest message; cursor should advance to ts2.
    assert downloader._cursor_ts == ts2
