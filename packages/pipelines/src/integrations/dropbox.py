"""Dropbox integration for downloading PNG files from shared folder links.

Required env var:
    DROPBOX_ACCESS_TOKEN — Dropbox app access token with shared_link.metadata permissions.

API reference:
    POST https://api.dropboxapi.com/2/files/list_folder  (list shared folder)
    POST https://content.dropboxapi.com/2/sharing/get_shared_link_file  (download)
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import httpx
import structlog

log = structlog.get_logger(__name__)

_DROPBOX_API = "https://api.dropboxapi.com/2"
_DROPBOX_CONTENT = "https://content.dropboxapi.com/2"


class DropboxSharedFolderClient:
    """Downloads files from a Dropbox shared folder link.

    Args:
        access_token: Dropbox app access token.
    """

    def __init__(self, access_token: str) -> None:
        self._token = access_token

    def list_pngs_in_normal(self, folder_url: str) -> list[str]:
        """Return PNG filenames found in the /Normal subfolder of *folder_url*.

        Args:
            folder_url: Dropbox shared folder URL (https://www.dropbox.com/sh/...).

        Returns:
            List of filenames (e.g. ``["creative_1.png", "creative_2.png"]``).
        """
        resp = httpx.post(
            f"{_DROPBOX_API}/files/list_folder",
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            },
            json={"path": "/Normal", "shared_link": {"url": folder_url}},
            timeout=30,
        )
        resp.raise_for_status()
        entries = resp.json().get("entries", [])
        return [
            e["name"]
            for e in entries
            if e.get(".tag") == "file" and e["name"].lower().endswith(".png")
        ]

    def download_png(self, folder_url: str, filename: str, dest: Path) -> bool:
        """Download a single PNG from the /Normal subfolder to *dest*.

        Skips the download if *dest* already exists.

        Args:
            folder_url: Dropbox shared folder URL.
            filename: PNG filename within /Normal (e.g. ``"creative_1.png"``).
            dest: Full local path to write the file.

        Returns:
            True if the file was downloaded, False if it was skipped.
        """
        if dest.exists():
            log.info("dropbox.file.skip_existing", path=str(dest))
            return False

        dest.parent.mkdir(parents=True, exist_ok=True)
        api_arg = json.dumps({"url": folder_url, "path": f"/Normal/{filename}"})
        resp = httpx.post(
            f"{_DROPBOX_CONTENT}/sharing/get_shared_link_file",
            headers={
                "Authorization": f"Bearer {self._token}",
                "Dropbox-API-Arg": api_arg,
            },
            timeout=120,
        )
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        size_kb = dest.stat().st_size // 1024
        log.info(
            "dropbox.file.downloaded",
            filename=filename,
            path=str(dest),
            size_kb=size_kb,
        )
        return True


def build_from_env() -> DropboxSharedFolderClient:
    """Construct a :class:`DropboxSharedFolderClient` from environment variables.

    Required env var:
        DROPBOX_ACCESS_TOKEN
    """
    token = os.environ.get("DROPBOX_ACCESS_TOKEN", "")
    if not token:
        raise EnvironmentError(
            "DROPBOX_ACCESS_TOKEN environment variable is not set. "
            "Create a Dropbox app with shared_link.metadata permission and "
            "generate an access token, then set it in your .env file."
        )
    return DropboxSharedFolderClient(access_token=token)
