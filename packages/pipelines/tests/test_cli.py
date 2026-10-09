"""Unit tests for native_ads.cli — argument parsing and main() exit codes.

All tests use unittest.mock; no live credentials or HTTP calls required.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from native_ads.cli import _parse_args, main
from native_ads.uploader import UploadResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _result(platform: str, success: bool = True) -> UploadResult:
    return UploadResult(
        platform=platform,
        campaign_id="camp-1",
        campaign_name="TEST",
        creatives_uploaded=3,
        errors=[] if success else ["upload failed"],
    )


# ---------------------------------------------------------------------------
# _parse_args
# ---------------------------------------------------------------------------


def test_parse_args_required_fields() -> None:
    args = _parse_args(["--folder", "/tmp/imgs", "--name", "MY_CAMP", "--url", "https://x.com"])
    assert args.folder == Path("/tmp/imgs")
    assert args.name == "MY_CAMP"
    assert args.url == "https://x.com"


def test_parse_args_defaults() -> None:
    args = _parse_args(["--folder", "/tmp", "--name", "X", "--url", "https://x.com"])
    assert args.platform == "all"
    assert args.start_date is None
    assert args.outbrain_template is None
    assert args.taboola_template is None


def test_parse_args_platform_choices() -> None:
    for platform in ("outbrain", "taboola", "all"):
        args = _parse_args(
            ["--folder", "/tmp", "--name", "X", "--url", "https://x.com", "--platform", platform]
        )
        assert args.platform == platform


def test_parse_args_start_date_parses_iso() -> None:
    args = _parse_args(
        ["--folder", "/tmp", "--name", "X", "--url", "https://x.com", "--start-date", "2026-10-11"]
    )
    assert args.start_date == date(2026, 10, 11)


def test_parse_args_template_overrides() -> None:
    args = _parse_args(
        [
            "--folder",
            "/tmp",
            "--name",
            "X",
            "--url",
            "https://x.com",
            "--outbrain-template",
            "ob-tpl-99",
            "--taboola-template",
            "tb-tpl-88",
        ]
    )
    assert args.outbrain_template == "ob-tpl-99"
    assert args.taboola_template == "tb-tpl-88"


def test_parse_args_invalid_platform_exits(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        _parse_args(["--folder", "/tmp", "--name", "X", "--url", "https://x.com", "--platform", "facebook"])  # noqa: E501


# ---------------------------------------------------------------------------
# main() — non-existent folder
# ---------------------------------------------------------------------------


def test_main_returns_1_when_folder_missing(tmp_path: Path) -> None:
    nonexistent = tmp_path / "no_such_folder"
    exit_code = main(
        ["--folder", str(nonexistent), "--name", "X", "--url", "https://x.com"]
    )
    assert exit_code == 1


# ---------------------------------------------------------------------------
# main() — platform routing (skip flags)
# ---------------------------------------------------------------------------


def test_main_platform_all_passes_both_platforms(tmp_path: Path) -> None:
    folder = tmp_path / "imgs"
    folder.mkdir()
    (folder / "ad.png").write_bytes(b"\x89PNG")

    captured: dict[str, object] = {}

    def fake_run_upload(**kwargs: object) -> list[UploadResult]:
        captured.update(kwargs)
        return [_result("outbrain"), _result("taboola")]

    with (
        patch("native_ads.cli.load_dotenv"),
        patch("native_ads.uploader.run_upload", side_effect=fake_run_upload),
    ):
        exit_code = main(["--folder", str(folder), "--name", "X", "--url", "https://x.com"])

    assert exit_code == 0
    assert captured.get("skip_outbrain") is False
    assert captured.get("skip_taboola") is False


def test_main_platform_outbrain_skips_taboola(tmp_path: Path) -> None:
    folder = tmp_path / "imgs"
    folder.mkdir()
    (folder / "ad.png").write_bytes(b"\x89PNG")

    captured: dict[str, object] = {}

    def fake_run_upload(**kwargs: object) -> list[UploadResult]:
        captured.update(kwargs)
        return [_result("outbrain")]

    with (
        patch("native_ads.cli.load_dotenv"),
        patch("native_ads.uploader.run_upload", side_effect=fake_run_upload),
    ):
        exit_code = main(
            ["--folder", str(folder), "--name", "X", "--url", "https://x.com", "--platform", "outbrain"]  # noqa: E501
        )

    assert exit_code == 0
    assert captured.get("skip_outbrain") is False
    assert captured.get("skip_taboola") is True


def test_main_platform_taboola_skips_outbrain(tmp_path: Path) -> None:
    folder = tmp_path / "imgs"
    folder.mkdir()
    (folder / "ad.png").write_bytes(b"\x89PNG")

    captured: dict[str, object] = {}

    def fake_run_upload(**kwargs: object) -> list[UploadResult]:
        captured.update(kwargs)
        return [_result("taboola")]

    with (
        patch("native_ads.cli.load_dotenv"),
        patch("native_ads.uploader.run_upload", side_effect=fake_run_upload),
    ):
        exit_code = main(
            ["--folder", str(folder), "--name", "X", "--url", "https://x.com", "--platform", "taboola"]  # noqa: E501
        )

    assert exit_code == 0
    assert captured.get("skip_outbrain") is True
    assert captured.get("skip_taboola") is False


# ---------------------------------------------------------------------------
# main() — exit codes based on upload success/failure
# ---------------------------------------------------------------------------


def test_main_returns_0_when_all_succeed(tmp_path: Path) -> None:
    folder = tmp_path / "imgs"
    folder.mkdir()
    (folder / "ad.png").write_bytes(b"\x89PNG")

    with (
        patch("native_ads.cli.load_dotenv"),
        patch(
            "native_ads.uploader.run_upload",
            return_value=[_result("outbrain", success=True), _result("taboola", success=True)],
        ),
    ):
        exit_code = main(["--folder", str(folder), "--name", "X", "--url", "https://x.com"])

    assert exit_code == 0


def test_main_returns_1_when_any_platform_fails(tmp_path: Path) -> None:
    folder = tmp_path / "imgs"
    folder.mkdir()
    (folder / "ad.png").write_bytes(b"\x89PNG")

    with (
        patch("native_ads.cli.load_dotenv"),
        patch(
            "native_ads.uploader.run_upload",
            return_value=[_result("outbrain", success=True), _result("taboola", success=False)],
        ),
    ):
        exit_code = main(["--folder", str(folder), "--name", "X", "--url", "https://x.com"])

    assert exit_code == 1


def test_main_passes_template_ids_to_run_upload(tmp_path: Path) -> None:
    folder = tmp_path / "imgs"
    folder.mkdir()
    (folder / "ad.png").write_bytes(b"\x89PNG")

    captured: dict[str, object] = {}

    def fake_run_upload(**kwargs: object) -> list[UploadResult]:
        captured.update(kwargs)
        return [_result("outbrain"), _result("taboola")]

    with (
        patch("native_ads.cli.load_dotenv"),
        patch("native_ads.uploader.run_upload", side_effect=fake_run_upload),
    ):
        main(
            [
                "--folder",
                str(folder),
                "--name",
                "X",
                "--url",
                "https://x.com",
                "--outbrain-template",
                "ob-tpl",
                "--taboola-template",
                "tb-tpl",
            ]
        )

    assert captured.get("outbrain_template_id") == "ob-tpl"
    assert captured.get("taboola_template_id") == "tb-tpl"
