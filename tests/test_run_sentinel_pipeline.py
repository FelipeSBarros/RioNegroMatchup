"""
Tests for run_sentinel_pipeline() and the run_download() return-value fix.

Network boundary
----------------
build_catalog() calls SentinelHubCatalog.search() and pystac_client.Client.search().
run_download() calls boto3 S3 and requests.get().

All external I/O is patched at the call-site level so no real credentials or
network access are needed.  The conftest already patches Client.open() at
collection time; these tests patch the individual search/download calls.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from aquamatch.sentinel_data import run_download, run_sentinel_pipeline
from aquamatch.pipeline_config import SentinelSection, DownloadSection

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_SCENE_ID = "S2A_MSIL1C_20240315T135111_N0500_R024_T21HUD_20240315T160000"
_CATALOG_ENTRY = [
    {
        "field_date": "2024-03-15",
        "images_found": {
            "same_day": [
                {
                    "id": _SCENE_ID,
                    "datetime": "2024-03-15T13:51:11Z",
                    "cloud_cover": 5.0,
                    "href": (
                        "s3://eodata/Sentinel-2/MSI/L1C/2024/03/15/"
                        + _SCENE_ID
                        + ".SAFE"
                    ),
                    "delta_days": 0,
                    "l2a_scl": "https://example.com/SCL.tif",
                }
            ],
            "previous": [],
            "posterior": [],
        },
    }
]


def _write_unique_csv(path: Path) -> None:
    pd.DataFrame(
        {
            "date": ["2024-03-15"],
            "latitud": [-32.85],
            "longitud": [-56.57],
            "s2_tile": ["21HUD"],
        }
    ).to_csv(path, index=False)


def _write_catalog_json(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(_CATALOG_ENTRY, f)


def _make_safe_dir(output_dir: Path, scene_id: str) -> Path:
    """Create an already-downloaded SAFE folder to trigger skip logic."""
    safe_dir = output_dir / scene_id
    safe_dir.mkdir(parents=True, exist_ok=True)
    (safe_dir / "dummy.txt").write_text("x")
    return safe_dir


# ---------------------------------------------------------------------------
# Tests for run_download return value
# ---------------------------------------------------------------------------


class TestRunDownloadReturnsStats:
    """run_download() must return its stats dict."""

    def test_returns_dict(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        result = run_download(
            catalog_json=catalog_file,
            output_dir=tmp_path,
            strategy="best",
            max_per_date=1,
            download_scl=True,
        )

        assert isinstance(result, dict)

    def test_stats_keys_present(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        result = run_download(
            catalog_json=catalog_file,
            output_dir=tmp_path,
            strategy="best",
            max_per_date=1,
            download_scl=True,
        )

        for key in (
            "total_processed",
            "already_downloaded",
            "safe_downloaded",
            "scl_downloaded",
            "errors",
        ):
            assert key in result

    def test_already_downloaded_counted(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        result = run_download(
            catalog_json=catalog_file,
            output_dir=tmp_path,
            strategy="best",
            max_per_date=1,
            download_scl=True,
        )

        assert result["already_downloaded"] == 1
        assert result["safe_downloaded"] == 0
        assert result["errors"] == 0


# ---------------------------------------------------------------------------
# Tests for run_sentinel_pipeline
# ---------------------------------------------------------------------------


class TestRunSentinelPipelineValidation:
    """Input validation before any I/O."""

    def test_invalid_steps_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid mode"):
            run_sentinel_pipeline(mode="invalid")

    def test_valid_steps_do_not_raise(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        for step in ("download",):
            result = run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode=step,
            )
            assert "step" in result


class TestRunSentinelPipelineDefaults:
    """None arguments must resolve to pipeline_config dataclass defaults."""

    def test_none_strategy_resolves_to_download_section_default(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        captured = {}

        def fake_run_download(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch(
            "aquamatch.sentinel_data.run_download", side_effect=fake_run_download
        ):
            run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="download",
                strategy=None,
            )

        assert captured["strategy"] == DownloadSection().strategy

    def test_none_max_per_date_resolves_to_default(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)

        captured = {}

        def fake_run_download(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch(
            "aquamatch.sentinel_data.run_download", side_effect=fake_run_download
        ):
            run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="download",
                max_per_date=None,
            )

        assert captured["max_per_date"] == DownloadSection().max_per_date

    def test_none_max_cloud_cover_resolves_to_none(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)

        captured = {}

        def fake_run_download(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch(
            "aquamatch.sentinel_data.run_download", side_effect=fake_run_download
        ):
            run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="download",
                max_cloud_cover=None,
            )

        assert captured["max_cloud_cover"] is None

    def test_none_unique_csv_resolves_to_sentinel_section_default(self):
        with patch("aquamatch.pipeline_config.SentinelSection") as mock_section:
            mock_section.return_value.csv = "/tmp/nonexistent_default.csv"
            mock_section.return_value.catalog_json = "/tmp/catalog.json"

            result = run_sentinel_pipeline(
                csv=None,
                mode="catalog",
            )

        assert result["status"] == "error"
        assert "nonexistent_default.csv" in result["error"]

    def test_none_output_dir_resolves_to_download_section_default(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        with patch("aquamatch.pipeline_config.DownloadSection") as mock_section:
            mock_section.return_value.output_dir = tmp_path
            mock_section.return_value.strategy = "best"
            mock_section.return_value.max_per_date = 1
            mock_section.return_value.max_cloud_cover = None
            mock_section.return_value.download_scl = True

            result = run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=None,
                mode="download",
            )

        assert result["status"] == "success"
        assert result["outputs"]["output_dir"] == tmp_path


class TestRunSentinelPipelineDownloadStep:
    """Tests for mode='download' — build_catalog is skipped."""

    def test_returns_success_when_all_already_downloaded(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        result = run_sentinel_pipeline(
            catalog_json=catalog_file,
            output_dir=tmp_path,
            mode="download",
        )

        assert result["status"] == "success"
        assert result["error"] is None

    def test_outputs_contains_output_dir(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        result = run_sentinel_pipeline(
            catalog_json=catalog_file,
            output_dir=tmp_path,
            mode="download",
        )

        assert result["outputs"]["output_dir"] == tmp_path

    def test_outputs_contains_download_stats(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        result = run_sentinel_pipeline(
            catalog_json=catalog_file,
            output_dir=tmp_path,
            mode="download",
        )

        stats = result["outputs"]["download_stats"]
        assert isinstance(stats, dict)
        assert "total_processed" in stats

    def test_strategy_forwarded_to_run_download(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)

        captured = {}

        def fake_run_download(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch(
            "aquamatch.sentinel_data.run_download", side_effect=fake_run_download
        ):
            run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="download",
                strategy="same_day",
            )

        assert captured["strategy"] == "same_day"

    def test_max_per_date_forwarded_to_run_download(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)

        captured = {}

        def fake_run_download(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch(
            "aquamatch.sentinel_data.run_download", side_effect=fake_run_download
        ):
            run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="download",
                max_per_date=3,
            )

        assert captured["max_per_date"] == 3

    def test_max_cloud_cover_forwarded_to_run_download(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)

        captured = {}

        def fake_run_download(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch(
            "aquamatch.sentinel_data.run_download", side_effect=fake_run_download
        ):
            run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="download",
                max_cloud_cover=15,
            )

        assert captured["max_cloud_cover"] == 15

    def test_only_first_not_a_parameter(self):
        """run_sentinel_pipeline must not accept only_first."""
        import inspect

        sig = inspect.signature(run_sentinel_pipeline)
        assert "only_first" not in sig.parameters

    def test_elapsed_seconds_present_on_success(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        result = run_sentinel_pipeline(
            catalog_json=catalog_file,
            output_dir=tmp_path,
            mode="download",
        )

        assert isinstance(result["elapsed_seconds"], (float, int))
        assert result["elapsed_seconds"] >= 0.0

    def test_missing_catalog_returns_error(self, tmp_path):
        result = run_sentinel_pipeline(
            catalog_json=tmp_path / "nonexistent.json",
            output_dir=tmp_path,
            mode="download",
        )

        assert result["status"] == "error"
        assert result["error"] is not None

    def test_error_result_still_has_elapsed_seconds(self, tmp_path):
        result = run_sentinel_pipeline(
            catalog_json=tmp_path / "nonexistent.json",
            output_dir=tmp_path,
            mode="download",
        )

        assert isinstance(result["elapsed_seconds"], float)
        assert result["elapsed_seconds"] >= 0.0

    def test_accepts_string_paths(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        result = run_sentinel_pipeline(
            catalog_json=str(catalog_file),
            output_dir=str(tmp_path),
            mode="download",
        )

        assert result["status"] == "success"

    def test_download_step_does_not_write_catalog(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        original_mtime = catalog_file.stat().st_mtime
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        run_sentinel_pipeline(
            catalog_json=catalog_file,
            output_dir=tmp_path,
            mode="download",
        )

        assert catalog_file.stat().st_mtime == original_mtime


class TestRunSentinelPipelineCatalogStep:
    """Tests for mode='catalog' — download is skipped."""

    def _mock_search_images_result(self):
        return [
            {
                "id": _SCENE_ID,
                "datetime": "2024-03-15T13:51:11Z",
                "cloud_cover": 5.0,
                "href": "s3://eodata/dummy/path/" + _SCENE_ID + ".SAFE",
                "delta_days": 0,
                "l2a_scl": "https://example.com/SCL.tif",
            }
        ]

    def test_catalog_json_is_written(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=self._mock_search_images_result(),
        ):
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
            )

        assert catalog_file.exists()

    def test_catalog_json_is_valid_json(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=self._mock_search_images_result(),
        ):
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
            )

        with open(catalog_file) as f:
            data = json.load(f)
        assert isinstance(data, list)

    def test_catalog_uses_bucketed_schema(self, tmp_path):
        """Catalog output must use the new images_found dict schema."""
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=self._mock_search_images_result(),
        ):
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
            )

        with open(catalog_file) as f:
            data = json.load(f)
        assert set(data[0]["images_found"].keys()) == {
            "same_day",
            "previous",
            "posterior",
        }

    def test_outputs_contains_catalog_json_path(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=self._mock_search_images_result(),
        ):
            result = run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
            )

        assert result["outputs"]["catalog_json"] == catalog_file

    def test_catalog_step_does_not_trigger_download(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=self._mock_search_images_result(),
        ), patch("aquamatch.sentinel_data.run_download") as mock_run_download:
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
            )

        mock_run_download.assert_not_called()

    def test_missing_unique_csv_returns_error(self, tmp_path):
        result = run_sentinel_pipeline(
            csv=tmp_path / "nonexistent.csv",
            catalog_json=tmp_path / "catalog.json",
            mode="catalog",
        )

        assert result["status"] == "error"
        assert result["error"] is not None


class TestRunSentinelPipelineStatusDict:
    """Status dict contract is consistent across all modes and outcomes."""

    def test_step_field_is_sentinel(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        result = run_sentinel_pipeline(
            catalog_json=catalog_file,
            output_dir=tmp_path,
            mode="download",
        )

        assert result["step"] == "sentinel"

    def test_error_result_has_empty_or_partial_outputs(self, tmp_path):
        result = run_sentinel_pipeline(
            catalog_json=tmp_path / "nonexistent.json",
            output_dir=tmp_path,
            mode="download",
        )

        assert isinstance(result["outputs"], dict)

    def test_partial_outputs_preserved_on_error_after_catalog(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[
                {
                    "id": _SCENE_ID,
                    "datetime": "2024-03-15T13:51:11Z",
                    "cloud_cover": 5.0,
                    "href": "s3://eodata/dummy/path/" + _SCENE_ID + ".SAFE",
                    "delta_days": 0,
                    "l2a_scl": "https://example.com/SCL.tif",
                }
            ],
        ), patch(
            "aquamatch.sentinel_data.run_download",
            side_effect=RuntimeError("S3 unavailable"),
        ):
            result = run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="all",
            )

        assert result["status"] == "error"
        assert "catalog_json" in result["outputs"]
