"""Slack monitor for native ad creative packages.

Polls the ext-sheko channel (C09FCDHFGCU) for messages matching the
``Native_`` pattern, extracts the external URL, follows redirects to the
Dropbox shared folder, and downloads all PNGs from the ``Normal`` subfolder.

Pipeline:
    Slack message (Native_MS) → extract URL → follow redirect → Dropbox
    shared folder → /Normal/ folder → download PNGs

Required env vars:
    SLACK_BOT_TOKEN        — Bot token with channels:history scope.
    DROPBOX_ACCESS_TOKEN   — Dropbox app token with shared_link.metadata.

Optional env vars:
    SLACK_CHANNEL_ID   — Override default channel (C09FCDHFGCU / ext-sheko).
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

from integrations.dropbox import DropboxSharedFolderClient

log = structlog.get_logger(__name__)

# Default channel to monitor — overridable via env.
DEFAULT_CHANNEL_ID = "C09FCDHFGCU"
# Polling interval in seconds when running in loop mode.
DEFAULT_POLL_INTERVAL = 60

# Matches messages relevant to native ad packages (e.g. Native_MS, Native_FB).
NATIVE_AD_PATTERN = re.compile(r"Native_", re.IGNORECASE)
# Extracts the first URL found in a string.
URL_PATTERN = re.compile(r"https?://\S+")


def _safe_filename(name: str) -> str:
    """Sanitize a filename, replacing non-alphanumeric characters (except . - _)."""
    return re.sub(r"[^\w.\-]", "_", name)


def _is_native_ad_message(text: str) -> bool:
    """Return True if *text* matches the Native_ ad package pattern."""
    return bool(NATIVE_AD_PATTERN.search(text))


def _extract_first_url(text: str) -> str | None:
    """Extract the first URL from *text*, or None if none found."""
    match = URL_PATTERN.search(text)
    return match.group(0) if match else None


def _resolve_redirect(url: str, timeout: int = 30) -> str:
    """Follow HTTP redirects and return the final URL."""
    with httpx.Client(follow_redirects=True, timeout=timeout) as http:
        resp = http.get(url)
        return str(resp.url)


class SlackNativeAdMonitor:
    """Monitors a Slack channel for native ad messages and downloads the PNGs.

    For each message matching ``Native_``, the monitor:
    1. Extracts the external URL from the message text.
    2. Follows HTTP redirects to arrive at the Dropbox shared folder URL.
    3. Lists PNG files inside the ``/Normal`` subfolder via the Dropbox API.
    4. Downloads each PNG to ``<download_dir>/<YYYY-MM-DD>/<filename>``.

    Args:
        bot_token: Slack bot token with ``channels:history`` scope.
        dropbox_client: Configured :class:`~integrations.dropbox.DropboxSharedFolderClient`.
        channel_id: Slack channel ID to monitor.
        download_dir: Root directory where PNGs will be saved.
        oldest_ts: Only fetch messages newer than this Unix timestamp string.
            Pass None to default to the last 24 hours on first run.
    """

    def __init__(
        self,
        bot_token: str,
        dropbox_client: DropboxSharedFolderClient,
        channel_id: str = DEFAULT_CHANNEL_ID,
        download_dir: Path | str = Path("downloads/slack"),
        oldest_ts: str | None = None,
    ) -> None:
        self._client = WebClient(token=bot_token)
        self._dropbox = dropbox_client
        self.channel_id = channel_id
        self.download_dir = Path(download_dir)
        self._cursor_ts: str | None = oldest_ts

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def poll_once(self) -> list[Path]:
        """Fetch new messages and download PNGs from Dropbox for each match.

        Returns:
            List of local paths to PNG files that were downloaded.
        """
        messages = self._fetch_native_ad_messages()
        if not messages:
            log.info("slack.poll", channel=self.channel_id, native_ad_messages=0)
            return []

        downloaded: list[Path] = []
        for msg in messages:
            paths = self._process_message(msg)
            downloaded.extend(paths)

        # Advance cursor to newest message so next poll only sees newer msgs.
        newest_ts = messages[-1].get("ts")
        if newest_ts:
            self._cursor_ts = newest_ts

        log.info(
            "slack.poll",
            channel=self.channel_id,
            native_ad_messages=len(messages),
            pngs_downloaded=len(downloaded),
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

    def _fetch_native_ad_messages(self) -> list[dict[str, Any]]:
        """Return messages newer than the cursor that match the Native_ pattern."""
        kwargs: dict[str, Any] = {"channel": self.channel_id, "limit": 200}
        if self._cursor_ts:
            kwargs["oldest"] = self._cursor_ts
        else:
            kwargs["oldest"] = str(time.time() - 86400)

        try:
            resp = self._client.conversations_history(**kwargs)
        except SlackApiError as exc:
            log.error("slack.conversations_history.error", error=str(exc))
            raise

        messages: list[dict[str, Any]] = resp.get("messages") or []
        # Slack returns newest-first; reverse so we process oldest first.
        messages.reverse()

        # Exclude the exact cursor message (already processed).
        if self._cursor_ts:
            messages = [m for m in messages if m.get("ts") != self._cursor_ts]

        # Keep only messages whose text matches the Native_ pattern.
        return [m for m in messages if _is_native_ad_message(m.get("text") or "")]

    def _process_message(self, msg: dict[str, Any]) -> list[Path]:
        """Handle a single Native_ message: resolve URL, download PNGs.

        Returns list of paths to downloaded PNG files.
        """
        text: str = msg.get("text") or ""
        msg_ts: str = msg.get("ts") or ""

        url = _extract_first_url(text)
        if not url:
            log.warning("slack.message.no_url", ts=msg_ts, text=text[:120])
            return []

        log.info("slack.message.processing", ts=msg_ts, url=url)

        try:
            dropbox_url = _resolve_redirect(url)
        except httpx.HTTPError as exc:
            log.error("slack.message.redirect_error", url=url, error=str(exc))
            return []

        if "dropbox.com" not in dropbox_url:
            log.warning(
                "slack.message.not_dropbox",
                original_url=url,
                resolved_url=dropbox_url,
            )
            return []

        try:
            png_filenames = self._dropbox.list_pngs_in_normal(dropbox_url)
        except httpx.HTTPError as exc:
            log.error("dropbox.list_error", dropbox_url=dropbox_url, error=str(exc))
            return []

        if not png_filenames:
            log.info("dropbox.normal.no_pngs", dropbox_url=dropbox_url)
            return []

        # Organise downloads by date derived from Slack message timestamp.
        try:
            ts_float = float(msg_ts) if msg_ts else time.time()
        except ValueError:
            ts_float = time.time()
        date_str = datetime.fromtimestamp(ts_float, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )
        dest_dir = self.download_dir / date_str

        result: list[Path] = []
        for filename in png_filenames:
            dest = dest_dir / _safe_filename(filename)
            try:
                self._dropbox.download_png(dropbox_url, filename, dest)
            except httpx.HTTPError as exc:
                log.error("dropbox.download_error", filename=filename, error=str(exc))
                continue
            if dest.exists():
                result.append(dest)

        return result


# ---------------------------------------------------------------------------
# Backward-compat alias — existing imports of SlackFileDownloader still work.
# ---------------------------------------------------------------------------
SlackFileDownloader = SlackNativeAdMonitor


def build_from_env(
    download_dir: Path | str = Path("downloads/slack"),
    oldest_ts: str | None = None,
) -> SlackNativeAdMonitor:
    """Construct a :class:`SlackNativeAdMonitor` from environment variables.

    Required env vars:
        SLACK_BOT_TOKEN
        DROPBOX_ACCESS_TOKEN

    Optional env vars:
        SLACK_CHANNEL_ID  (default: C09FCDHFGCU / ext-sheko)
    """
    slack_token = os.environ.get("SLACK_BOT_TOKEN", "")
    if not slack_token:
        raise EnvironmentError(
            "SLACK_BOT_TOKEN environment variable is not set. "
            "Create a Slack app with channels:history scope and set the bot token."
        )

    from integrations.dropbox import build_from_env as dropbox_from_env

    dropbox_client = dropbox_from_env()
    channel_id = os.environ.get("SLACK_CHANNEL_ID", DEFAULT_CHANNEL_ID)
    return SlackNativeAdMonitor(
        bot_token=slack_token,
        dropbox_client=dropbox_client,
        channel_id=channel_id,
        download_dir=download_dir,
        oldest_ts=oldest_ts,
    )
