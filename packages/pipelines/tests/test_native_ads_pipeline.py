"""Unit tests for the end-to-end Native Ads pipeline (Slack → Dropbox → Outbrain/Taboola).

All tests use unittest.mock — no live credentials, Slack, or ad platform calls.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from loaders.native_ads_pipeline import PackageResult, _print_summary, run_pipeline
from native_ads.uploader import UploadResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_upload_result(platform: str, success: bool = True) -> UploadResult:
    errors = [] if success else [f"{platform} upload failed"]
    return UploadResult(
        platform=platform,
        campaign_id=f"camp-{platform}-001",
        campaign_name="NATIVE_MS_TEST",
        creatives_uploaded=5,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# run_pipeline — missing env var
# ---------------------------------------------------------------------------


def test_run_pipeline_raises_without_landing_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NATIVE_ADS_LANDING_URL", raising=False)
    with pytest.raises(EnvironmentError, match="NATIVE_ADS_LANDING_URL"):
        run_pipeline(dry_run=False)


# ---------------------------------------------------------------------------
# run_pipeline — no new packages
# ---------------------------------------------------------------------------


def test_run_pipeline_no_packages_returns_empty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-fake")
    mock_monitor = MagicMock()
    mock_monitor.poll_once.return_value = []

    with patch("loaders.native_ads_pipeline.build_slack_monitor", return_value=mock_monitor):
        results = run_pipeline(landing_url="https://example.com", download_dir=tmp_path)

    assert results == []
    mock_monitor.poll_once.assert_called_once()


# ---------------------------------------------------------------------------
# run_pipeline — dry run skips upload
# ---------------------------------------------------------------------------


def test_run_pipeline_dry_run_skips_upload(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-fake")

    package_dir = tmp_path / "NATIVE_MS_TEST_001"
    package_dir.mkdir()
    fake_img = package_dir / "img1.png"
    fake_img.touch()

    mock_monitor = MagicMock()
    mock_monitor.poll_once.return_value = [fake_img]

    with (
        patch("loaders.native_ads_pipeline.build_slack_monitor", return_value=mock_monitor),
        patch("loaders.native_ads_pipeline.run_upload") as mock_upload,
    ):
        results = run_pipeline(landing_url="https://example.com", download_dir=tmp_path, dry_run=True)

    assert len(results) == 1
    assert results[0].package_name == "NATIVE_MS_TEST_001"
    assert results[0].images_downloaded == 1
    assert results[0].upload_results == []
    mock_upload.assert_not_called()


# ---------------------------------------------------------------------------
# run_pipeline — successful upload (all platforms)
# ---------------------------------------------------------------------------


def test_run_pipeline_uploads_on_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-fake")

    package_dir = tmp_path / "NATIVE_MS_TEST_002"
    package_dir.mkdir()
    imgs = [package_dir / f"img{i}.png" for i in range(3)]
    for img in imgs:
        img.touch()

    mock_monitor = MagicMock()
    mock_monitor.poll_once.return_value = imgs

    ob_result = _mock_upload_result("outbrain", success=True)
    tb_result = _mock_upload_result("taboola", success=True)

    with (
        patch("loaders.native_ads_pipeline.build_slack_monitor", return_value=mock_monitor),
        patch("loaders.native_ads_pipeline.run_upload", return_value=[ob_result, tb_result]),
    ):
        results = run_pipeline(
            landing_url="https://example.com",
            download_dir=tmp_path,
            platform="all",
        )

    assert len(results) == 1
    pkg = results[0]
    assert pkg.package_name == "NATIVE_MS_TEST_002"
    assert pkg.images_downloaded == 3
    assert pkg.success is True
    assert len(pkg.upload_results) == 2


# ---------------------------------------------------------------------------
# run_pipeline — multiple packages from one poll
# ---------------------------------------------------------------------------


def test_run_pipeline_groups_images_by_folder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-fake")

    dir_a = tmp_path / "NATIVE_MS_PKG_A"
    dir_b = tmp_path / "NATIVE_MS_PKG_B"
    dir_a.mkdir()
    dir_b.mkdir()
    imgs_a = [(dir_a / f"a{i}.png") for i in range(2)]
    imgs_b = [(dir_b / f"b{i}.png") for i in range(4)]
    for img in imgs_a + imgs_b:
        img.touch()

    mock_monitor = MagicMock()
    mock_monitor.poll_once.return_value = imgs_a + imgs_b

    with (
        patch("loaders.native_ads_pipeline.build_slack_monitor", return_value=mock_monitor),
        patch("loaders.native_ads_pipeline.run_upload", return_value=[]) as mock_upload,
    ):
        results = run_pipeline(landing_url="https://example.com", download_dir=tmp_path)

    assert len(results) == 2
    assert mock_upload.call_count == 2
    names = {r.package_name for r in results}
    assert names == {"NATIVE_MS_PKG_A", "NATIVE_MS_PKG_B"}


# ---------------------------------------------------------------------------
# run_pipeline — platform flag is passed through
# ---------------------------------------------------------------------------


def test_run_pipeline_outbrain_only_skips_taboola(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-fake")

    pkg_dir = tmp_path / "NATIVE_MS_TEST_OB"
    pkg_dir.mkdir()
    img = pkg_dir / "img.png"
    img.touch()

    mock_monitor = MagicMock()
    mock_monitor.poll_once.return_value = [img]
    ob_result = _mock_upload_result("outbrain")

    with (
        patch("loaders.native_ads_pipeline.build_slack_monitor", return_value=mock_monitor),
        patch("loaders.native_ads_pipeline.run_upload", return_value=[ob_result]) as mock_upload,
    ):
        run_pipeline(landing_url="https://example.com", download_dir=tmp_path, platform="outbrain")

    call_kwargs = mock_upload.call_args[1]
    assert call_kwargs["skip_outbrain"] is False
    assert call_kwargs["skip_taboola"] is True


def test_run_pipeline_taboola_only_skips_outbrain(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-fake")

    pkg_dir = tmp_path / "NATIVE_MS_TEST_TB"
    pkg_dir.mkdir()
    img = pkg_dir / "img.png"
    img.touch()

    mock_monitor = MagicMock()
    mock_monitor.poll_once.return_value = [img]
    tb_result = _mock_upload_result("taboola")

    with (
        patch("loaders.native_ads_pipeline.build_slack_monitor", return_value=mock_monitor),
        patch("loaders.native_ads_pipeline.run_upload", return_value=[tb_result]) as mock_upload,
    ):
        run_pipeline(landing_url="https://example.com", download_dir=tmp_path, platform="taboola")

    call_kwargs = mock_upload.call_args[1]
    assert call_kwargs["skip_outbrain"] is True
    assert call_kwargs["skip_taboola"] is False


# ---------------------------------------------------------------------------
# PackageResult — success / failure helpers
# ---------------------------------------------------------------------------


def test_package_result_success_when_all_uploads_ok() -> None:
    pkg = PackageResult(
        package_name="NATIVE_MS_X",
        folder=Path("/tmp/x"),
        images_downloaded=5,
        upload_results=[
            _mock_upload_result("outbrain", success=True),
            _mock_upload_result("taboola", success=True),
        ],
    )
    assert pkg.success is True
    assert pkg.errors == []


def test_package_result_failure_when_any_upload_fails() -> None:
    pkg = PackageResult(
        package_name="NATIVE_MS_Y",
        folder=Path("/tmp/y"),
        images_downloaded=3,
        upload_results=[
            _mock_upload_result("outbrain", success=True),
            _mock_upload_result("taboola", success=False),
        ],
    )
    assert pkg.success is False
    assert len(pkg.errors) == 1


def test_package_result_no_uploads_is_not_success() -> None:
    pkg = PackageResult(
        package_name="NATIVE_MS_Z",
        folder=Path("/tmp/z"),
        images_downloaded=2,
    )
    assert pkg.success is False


# ---------------------------------------------------------------------------
# _print_summary — smoke test (no crash, no assertion)
# ---------------------------------------------------------------------------


def test_print_summary_no_results(capsys: pytest.CaptureFixture) -> None:
    _print_summary([])
    captured = capsys.readouterr()
    assert "No new NATIVE_MS packages found" in captured.out


def test_print_summary_with_results(capsys: pytest.CaptureFixture) -> None:
    pkg = PackageResult(
        package_name="NATIVE_MS_SMOKE",
        folder=Path("/tmp/smoke"),
        images_downloaded=10,
        upload_results=[_mock_upload_result("outbrain", success=True)],
    )
    _print_summary([pkg])
    captured = capsys.readouterr()
    assert "NATIVE_MS_SMOKE" in captured.out
    assert "10" in captured.out
