"""Dropbox integration — download PNGs from a shared folder via zip.

No Dropbox API token required.  Appending ``dl=1`` to any public Dropbox
shared-folder URL triggers a zip download of the entire folder.  We extract
PNGs from the ``normal/`` subfolder (case-insensitive) in-memory and write
them to the local filesystem.
"""
from __future__ import annotations

import html
import io
import re
import zipfile
from pathlib import Path

import httpx
import structlog

log = structlog.get_logger(__name__)

_SLACK_URL_RE = re.compile(r"<(https?://[^|>]+)[|>]")
_HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 perf-marketing-pipelines/1.0"}


def parse_dropbox_url(slack_mrkdwn: str) -> str | None:
    """Extract and HTML-unescape a Dropbox URL from Slack mrkdwn text.

    Slack encodes ``&`` as ``&amp;`` inside attachment text fields.

    Returns:
        Unescaped Dropbox URL, or ``None`` if none found.
    """
    for raw in _SLACK_URL_RE.findall(slack_mrkdwn):
        unescaped = html.unescape(raw)
        if "dropbox.com" in unescaped:
            return unescaped
    return None


def _make_zip_url(folder_url: str) -> str:
    """Return the zip-download variant of a Dropbox shared-folder URL."""
    if "dl=0" in folder_url:
        return folder_url.replace("dl=0", "dl=1")
    if "dl=1" not in folder_url:
        sep = "&" if "?" in folder_url else "?"
        return folder_url + sep + "dl=1"
    return folder_url


def download_normal_pngs(
    folder_url: str,
    dest_dir: Path,
    timeout: int = 120,
) -> list[Path]:
    """Download all PNGs from the ``normal/`` subfolder of a Dropbox shared folder.

    Fetches the folder as a zip (no API token needed), extracts PNGs whose
    path starts with ``normal/`` (case-insensitive), writes them to *dest_dir*,
    and skips already-existing files.

    Returns:
        List of paths present after this call (downloaded + pre-existing).
    """
    zip_url = _make_zip_url(folder_url)
    log.info("dropbox.zip.downloading", url=zip_url[:80])

    with httpx.Client(follow_redirects=True, timeout=timeout, headers=_HTTP_HEADERS) as http:
        resp = http.get(zip_url)
        resp.raise_for_status()

    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile as exc:
        log.error("dropbox.zip.bad", error=str(exc), size=len(resp.content))
        raise

    normal_entries = [
        name for name in zf.namelist()
        if name.lower().startswith("normal/") and name.lower().endswith(".png")
    ]
    log.info("dropbox.zip.normal_pngs", count=len(normal_entries))

    if not normal_entries:
        return []

    dest_dir.mkdir(parents=True, exist_ok=True)
    result: list[Path] = []
    for entry in normal_entries:
        filename = Path(entry).name
        dest = dest_dir / filename
        if dest.exists():
            log.info("dropbox.png.skip_existing", path=str(dest))
        else:
            dest.write_bytes(zf.read(entry))
            log.info("dropbox.png.saved", path=str(dest), size=dest.stat().st_size)
        result.append(dest)

    return result
