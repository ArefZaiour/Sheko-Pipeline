"""Slack monitor for NATIVE_MS native-ad creative packages.

Polls the ext-sheko channel for Asana bot messages whose attachment title
starts with ``NATIVE_MS``.  For each match it extracts the Dropbox
shared-folder URL from the attachment text, downloads the folder as a zip,
and saves PNGs from the ``normal/`` subfolder.

Real message structure (confirmed against live channel 2026-03-31):
  - ``msg.text``: always ``"New Ad ist ready to test"`` — not useful
  - ``msg.attachments[n].title``: ad name, e.g. ``NATIVE_MS_2173_STATIC_...``
  - ``msg.attachments[n].text``: Dropbox folder link in Slack mrkdwn
    ``<url|label>`` format with ``&amp;``-encoded ampersands

Important: do NOT pass ``oldest`` to ``conversations.history``.  Slack
restricts that endpoint to post-join messages for recently-added bots.
We fetch the latest 200 messages and apply cursor filtering in Python
via ``resp.data["messages"]``.

Required env vars:
    SLACK_BOT_TOKEN    — Bot token with ``channels:history`` scope.

Optional env vars:
    SLACK_CHANNEL_ID   — Channel to monitor (default: C09FCDHFGCU / ext-sheko).
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from integrations.dropbox import download_normal_pngs, parse_dropbox_url

log = structlog.get_logger(__name__)

DEFAULT_CHANNEL_ID = "C09FCDHFGCU"
DEFAULT_POLL_INTERVAL = 60


class SlackNativeAdMonitor:
    """Polls a Slack channel for NATIVE_MS ad packages and downloads PNGs.

    For each ``NATIVE_MS`` attachment:
    1. Parses the Dropbox shared-folder URL from ``attachment.text``.
    2. Downloads the folder as a zip (no Dropbox API token needed).
    3. Saves PNGs from ``normal/`` to
       ``<download_dir>/<YYYY-MM-DD>/<ad_title>/<filename>``.

    Args:
        bot_token: Slack bot token with ``channels:history`` scope.
        channel_id: Slack channel ID to monitor.
        download_dir: Root directory for downloaded PNGs.
        oldest_ts: Only process messages newer than this Unix timestamp string.
            Defaults to 24 h ago on first run.
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
        self._cursor_ts: str | None = oldest_ts

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def poll_once(self) -> list[Path]:
        """Fetch new NATIVE_MS messages and download their PNGs.

        Returns:
            All local PNG paths saved (or already present) during this call.
        """
        messages = self._fetch_native_ms_messages()
        if not messages:
            log.info("slack.poll", channel=self.channel_id, native_ms_messages=0)
            return []

        all_paths: list[Path] = []
        for msg in messages:
            all_paths.extend(self._process_message(msg))

        newest_ts = messages[-1].get("ts")
        if newest_ts:
            self._cursor_ts = newest_ts

        log.info(
            "slack.poll",
            channel=self.channel_id,
            native_ms_messages=len(messages),
            pngs_downloaded=len(all_paths),
        )
        return all_paths

    def run_loop(self, interval: int = DEFAULT_POLL_INTERVAL) -> None:
        """Poll continuously until interrupted (Ctrl-C)."""
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

    def _fetch_native_ms_messages(self) -> list[dict[str, Any]]:
        """Return messages newer than cursor that have a NATIVE_MS attachment.

        We intentionally omit ``oldest`` from the API call because Slack
        restricts ``conversations.history`` to post-join messages when that
        parameter is supplied to a recently-joined bot.  Cursor filtering is
        applied in Python on ``resp.data["messages"]``.
        """
        try:
            resp = self._client.conversations_history(
                channel=self.channel_id, limit=200
            )
        except SlackApiError as exc:
            log.error("slack.conversations_history.error", error=str(exc))
            raise

        messages: list[dict[str, Any]] = resp.data.get("messages") or []
        messages.reverse()  # Slack returns newest-first; process oldest first

        if self._cursor_ts:
            cursor = float(self._cursor_ts)
            messages = [m for m in messages if float(m.get("ts") or 0) > cursor]
        else:
            cutoff = time.time() - 86400
            messages = [m for m in messages if float(m.get("ts") or 0) > cutoff]

        return [m for m in messages if self._has_native_ms_attachment(m)]

    @staticmethod
    def _has_native_ms_attachment(msg: dict[str, Any]) -> bool:
        return any(
            att.get("title", "").startswith("NATIVE_MS")
            for att in msg.get("attachments") or []
        )

    def _process_message(self, msg: dict[str, Any]) -> list[Path]:
        """Download PNGs for all NATIVE_MS attachments in *msg*."""
        msg_ts: str = msg.get("ts") or ""
        try:
            ts_float = float(msg_ts)
        except (ValueError, TypeError):
            ts_float = time.time()
        date_str = datetime.fromtimestamp(ts_float, tz=timezone.utc).strftime("%Y-%m-%d")

        result: list[Path] = []
        for att in msg.get("attachments") or []:
            title: str = att.get("title", "")
            if not title.startswith("NATIVE_MS"):
                continue

            dropbox_url = parse_dropbox_url(att.get("text", ""))
            if not dropbox_url:
                log.warning("slack.attachment.no_dropbox_url", title=title)
                continue

            dest_dir = self.download_dir / date_str / title
            log.info("slack.attachment.processing", title=title, dest=str(dest_dir))

            try:
                paths = download_normal_pngs(dropbox_url, dest_dir)
            except Exception as exc:  # noqa: BLE001
                log.error("slack.attachment.download_failed", title=title, error=str(exc))
                continue

            result.extend(paths)

        return result


# Backwards-compatibility alias.
SlackFileDownloader = SlackNativeAdMonitor


def build_from_env(
    download_dir: Path | str = Path("downloads/slack"),
    oldest_ts: str | None = None,
) -> SlackNativeAdMonitor:
    """Construct a :class:`SlackNativeAdMonitor` from environment variables.

    Required:
        SLACK_BOT_TOKEN

    Optional:
        SLACK_CHANNEL_ID  (default: C09FCDHFGCU)
    """
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    if not token:
        raise EnvironmentError(
            "SLACK_BOT_TOKEN is not set. "
            "Create a Slack app with channels:history scope and set the bot token."
        )
    channel_id = os.environ.get("SLACK_CHANNEL_ID", DEFAULT_CHANNEL_ID)
    return SlackNativeAdMonitor(
        bot_token=token,
        channel_id=channel_id,
        download_dir=download_dir,
        oldest_ts=oldest_ts,
    )
