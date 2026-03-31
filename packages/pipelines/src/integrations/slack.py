"""Slack file downloader for native ad creative packages.

Polls the ext-sheko channel (C09FCDHFGCU) for messages with file attachments
and downloads them to a local directory, organized by date.

Required Slack bot scopes:
  - channels:history  (read message history)
  - files:read        (download private files)

Set SLACK_BOT_TOKEN in the environment or .env file.
"""
from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import structlog
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

log = structlog.get_logger(__name__)

# Default channel to monitor — overridable via env.
DEFAULT_CHANNEL_ID = "C09FCDHFGCU"
# Polling interval in seconds when running in loop mode.
DEFAULT_POLL_INTERVAL = 60


def _safe_filename(name: str) -> str:
    """Sanitize a filename, replacing non-alphanumeric characters (except . - _)."""
    return re.sub(r"[^\w.\-]", "_", name)


class SlackFileDownloader:
    """Downloads file attachments from a Slack channel.

    Args:
        bot_token: Slack bot token with channels:history + files:read scopes.
        channel_id: Slack channel ID to monitor.
        download_dir: Root directory where files will be saved.
        oldest_ts: Only fetch messages newer than this Unix timestamp string.
            Pass None to start from 24 hours ago on first run.
    """

    def __init__(
        self,
        bot_token: str,
        channel_id: str = DEFAULT_CHANNEL_ID,
        download_dir: Path | str = Path("downloads/slack"),
        oldest_ts: str | None = None,
    ) -> None:
        self._client = WebClient(token=bot_token)
        self.channel_id = channel_id
        self.download_dir = Path(download_dir)
        # Cursor: track the latest processed message timestamp so we don't
        # re-download on the next poll.
        self._cursor_ts: str | None = oldest_ts

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def poll_once(self) -> list[Path]:
        """Fetch new messages and download any attached files.

        Returns:
            List of paths to files that were downloaded in this call.
        """
        messages = self._fetch_messages()
        if not messages:
            log.info("slack.poll", channel=self.channel_id, new_messages=0)
            return []

        downloaded: list[Path] = []
        for msg in messages:
            files = msg.get("files") or []
            for f in files:
                path = self._download_file(f, msg_ts=msg.get("ts", ""))
                if path:
                    downloaded.append(path)

        # Advance cursor to newest message ts so next poll only sees newer msgs.
        # messages is ordered oldest→newest after the reverse in _fetch_messages.
        newest_ts = messages[-1].get("ts")
        if newest_ts:
            self._cursor_ts = newest_ts

        log.info(
            "slack.poll",
            channel=self.channel_id,
            new_messages=len(messages),
            files_downloaded=len(downloaded),
        )
        return downloaded

    def run_loop(self, interval: int = DEFAULT_POLL_INTERVAL) -> None:
        """Poll the channel in a blocking loop. Ctrl-C to stop."""
        log.info("slack.loop.start", channel=self.channel_id, interval_s=interval)
        while True:
            try:
                self.poll_once()
            except SlackApiError as exc:
                log.error("slack.api_error", error=str(exc))
            except Exception as exc:  # noqa: BLE001
                log.error("slack.unexpected_error", error=str(exc))
            time.sleep(interval)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_messages(self) -> list[dict[str, Any]]:
        """Return messages newer than the current cursor, oldest-first."""
        kwargs: dict[str, Any] = {"channel": self.channel_id, "limit": 200}
        if self._cursor_ts:
            kwargs["oldest"] = self._cursor_ts
        else:
            # On very first run, only look back 24 hours.
            kwargs["oldest"] = str(time.time() - 86400)

        try:
            resp = self._client.conversations_history(**kwargs)
        except SlackApiError as exc:
            log.error("slack.conversations_history.error", error=str(exc))
            raise

        messages: list[dict[str, Any]] = resp.get("messages") or []
        # Slack returns newest-first; reverse so we process oldest first and
        # advance the cursor correctly.
        messages.reverse()

        # Exclude the exact cursor message itself (already processed).
        if self._cursor_ts:
            messages = [m for m in messages if m.get("ts") != self._cursor_ts]

        # Only keep messages that actually have file attachments.
        return [m for m in messages if m.get("files")]

    def _download_file(self, file_info: dict[str, Any], msg_ts: str) -> Path | None:
        """Download a single file described by Slack's file object.

        Files are saved under:
            <download_dir>/<YYYY-MM-DD>/<original_filename>

        Returns the path on success, None if the file was skipped or failed.
        """
        file_id: str = file_info.get("id", "unknown")
        name: str = _safe_filename(file_info.get("name") or f"{file_id}.bin")
        url: str | None = file_info.get("url_private_download") or file_info.get(
            "url_private"
        )

        if not url:
            log.warning("slack.file.no_url", file_id=file_id, name=name)
            return None

        # Organise by the date derived from the message timestamp.
        try:
            ts_float = float(msg_ts) if msg_ts else time.time()
        except ValueError:
            ts_float = time.time()
        date_str = datetime.fromtimestamp(ts_float, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )
        dest_dir = self.download_dir / date_str
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / name

        if dest.exists():
            log.info("slack.file.skip_existing", path=str(dest))
            return dest

        log.info("slack.file.downloading", file_id=file_id, name=name, dest=str(dest))
        try:
            # Slack private-file URLs require the bot token in the Authorization header.
            with httpx.Client() as http:
                resp = http.get(
                    url,
                    headers={"Authorization": f"Bearer {self._client.token}"},
                    follow_redirects=True,
                    timeout=60,
                )
                resp.raise_for_status()
                dest.write_bytes(resp.content)
        except httpx.HTTPError as exc:
            log.error(
                "slack.file.download_error",
                file_id=file_id,
                name=name,
                error=str(exc),
            )
            return None

        size_kb = dest.stat().st_size // 1024
        log.info(
            "slack.file.downloaded",
            path=str(dest),
            size_kb=size_kb,
            msg_ts=msg_ts,
        )
        return dest


def build_from_env(
    download_dir: Path | str = Path("downloads/slack"),
    oldest_ts: str | None = None,
) -> SlackFileDownloader:
    """Construct a :class:`SlackFileDownloader` from environment variables.

    Required env var:
        SLACK_BOT_TOKEN

    Optional env var:
        SLACK_CHANNEL_ID  (default: C09FCDHFGCU / ext-sheko)
    """
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    if not token:
        raise EnvironmentError(
            "SLACK_BOT_TOKEN environment variable is not set. "
            "Create a Slack app with channels:history and files:read scopes "
            "and set the bot token in your .env file."
        )
    channel_id = os.environ.get("SLACK_CHANNEL_ID", DEFAULT_CHANNEL_ID)
    return SlackFileDownloader(
        bot_token=token,
        channel_id=channel_id,
        download_dir=download_dir,
        oldest_ts=oldest_ts,
    )
