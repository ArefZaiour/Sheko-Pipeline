"""Unit tests for native_ads.uploader — _collect_images and platform upload functions.

Tests cover file collection, title derivation, and missing-template-ID error paths.
All tests use unittest.mock; no live credentials or HTTP calls required.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from native_ads.uploader import UploadResult, _collect_images, upload_to_taboola

# ---------------------------------------------------------------------------
# _collect_images
# ---------------------------------------------------------------------------


def test_collect_images_returns_pngs(tmp_path: Path) -> None:
    (tmp_path / "a.png").write_bytes(b"")
    (tmp_path / "b.png").write_bytes(b"")
    images = _collect_images(tmp_path)
    assert len(images) == 2
    assert all(p.suffix == ".png" for p in images)


def test_collect_images_includes_jpeg(tmp_path: Path) -> None:
    (tmp_path / "creative.jpg").write_bytes(b"")
    (tmp_path / "creative2.jpeg").write_bytes(b"")
    images = _collect_images(tmp_path)
    assert len(images) == 2


def test_collect_images_sorted_by_name(tmp_path: Path) -> None:
    (tmp_path / "c_third.png").write_bytes(b"")
    (tmp_path / "a_first.png").write_bytes(b"")
    (tmp_path / "b_second.png").write_bytes(b"")
    images = _collect_images(tmp_path)
    assert [p.name for p in images] == ["a_first.png", "b_second.png", "c_third.png"]


def test_collect_images_ignores_non_images(tmp_path: Path) -> None:
    (tmp_path / "ad.png").write_bytes(b"")
    (tmp_path / "brief.docx").write_bytes(b"")
    (tmp_path / "notes.txt").write_bytes(b"")
    images = _collect_images(tmp_path)
    assert len(images) == 1
    assert images[0].name == "ad.png"


def test_collect_images_raises_when_no_images(tmp_path: Path) -> None:
    (tmp_path / "readme.txt").write_bytes(b"")
    with pytest.raises(FileNotFoundError, match=str(tmp_path)):
        _collect_images(tmp_path)


def test_collect_images_empty_dir_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _collect_images(tmp_path)


# ---------------------------------------------------------------------------
# UploadResult.success
# ---------------------------------------------------------------------------


def test_upload_result_success_with_no_errors() -> None:
    r = UploadResult(platform="taboola", campaign_id="c1", campaign_name="X", creatives_uploaded=3)
    assert r.success is True


def test_upload_result_not_success_with_errors() -> None:
    r = UploadResult(
        platform="taboola",
        campaign_id="",
        campaign_name="X",
        creatives_uploaded=0,
        errors=["upload failed"],
    )
    assert r.success is False


# ---------------------------------------------------------------------------
# upload_to_taboola — title derivation from filename
# ---------------------------------------------------------------------------


def _make_mock_taboola_client(campaign_id: str = "camp-1") -> MagicMock:
    mock_client = MagicMock()
    mock_campaign = MagicMock()
    mock_campaign.id = campaign_id
    mock_client.duplicate_campaign.return_value = mock_campaign
    mock_client.validate_campaign.return_value = []
    mock_client.create_creative.return_value = MagicMock()
    return mock_client


def test_upload_to_taboola_derives_title_from_snake_case_filename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    img = tmp_path / "weight_loss_before_after.png"
    img.write_bytes(b"\x89PNG")
    mock_client = _make_mock_taboola_client()

    import sys
    import types

    fake_module = types.ModuleType("native_ads.taboola")
    fake_module.build_from_env = lambda: mock_client  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "native_ads.taboola", fake_module)

    result = upload_to_taboola(
        images=[img],
        campaign_name="Weight Loss Test",
        landing_url="https://example.com/lp",
        template_campaign_id="tpl-1",
    )

    assert result.creatives_uploaded == 1
    assert mock_client.create_creative.call_args.kwargs["title"] == "Weight Loss Before After"


def test_upload_to_taboola_derives_title_from_hyphen_filename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    img = tmp_path / "keto-diet-ad.png"
    img.write_bytes(b"\x89PNG")
    mock_client = _make_mock_taboola_client("camp-2")

    import sys
    import types

    fake_module = types.ModuleType("native_ads.taboola")
    fake_module.build_from_env = lambda: mock_client  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "native_ads.taboola", fake_module)

    result = upload_to_taboola(
        images=[img],
        campaign_name="Keto Diet",
        landing_url="https://example.com/keto",
        template_campaign_id="tpl-2",
    )

    assert result.creatives_uploaded == 1
    assert mock_client.create_creative.call_args.kwargs["title"] == "Keto Diet Ad"


# ---------------------------------------------------------------------------
# upload_to_taboola — missing template ID
# ---------------------------------------------------------------------------


def test_upload_to_taboola_missing_template_id_returns_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys
    import types

    img = tmp_path / "ad.png"
    img.write_bytes(b"\x89PNG")
    monkeypatch.delenv("TABOOLA_TEMPLATE_CAMPAIGN_ID", raising=False)

    mock_client = MagicMock()
    fake_module = types.ModuleType("native_ads.taboola")
    fake_module.build_from_env = lambda: mock_client  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "native_ads.taboola", fake_module)

    result = upload_to_taboola(
        images=[img],
        campaign_name="Test",
        landing_url="https://example.com",
        template_campaign_id=None,
    )

    assert result.success is False
    assert any("TABOOLA_TEMPLATE_CAMPAIGN_ID" in e for e in result.errors)
    mock_client.duplicate_campaign.assert_not_called()


# ---------------------------------------------------------------------------
# upload_to_taboola — individual creative failure doesn't abort batch
# ---------------------------------------------------------------------------


def test_upload_to_taboola_creative_error_continues_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys
    import types

    img1 = tmp_path / "good.png"
    img2 = tmp_path / "bad.png"
    img1.write_bytes(b"\x89PNG")
    img2.write_bytes(b"\x89PNG")

    mock_client = MagicMock()
    mock_campaign = MagicMock()
    mock_campaign.id = "camp-3"
    mock_client.duplicate_campaign.return_value = mock_campaign
    mock_client.validate_campaign.return_value = []

    call_count = 0

    def create_creative_side_effect(**kwargs: object) -> MagicMock:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("upload API error")
        return MagicMock()

    mock_client.create_creative.side_effect = create_creative_side_effect

    fake_module = types.ModuleType("native_ads.taboola")
    fake_module.build_from_env = lambda: mock_client  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "native_ads.taboola", fake_module)

    result = upload_to_taboola(
        images=[img1, img2],
        campaign_name="Batch Test",
        landing_url="https://example.com",
        template_campaign_id="tpl-3",
    )

    assert result.creatives_uploaded == 1
    assert len(result.errors) == 1
    assert "bad.png" in result.errors[0]
