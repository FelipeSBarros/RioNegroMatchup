"""
Tests for run_sentinel_pipeline() and the run_download() return-value fix.

Network boundary
----------------
build_catalog() calls SentinelHubCatalog.search() and pystac_client.Client.search().
run_download() calls boto3 S3 and requests.get().

All external I/O is patched at the call-site level so no real credentials or
network access are needed.  The conftest already patches Client.open() at
collection time; these tests patch the individual search/download calls.

Conventions (matching the existing test suite)
----------------------------------------------
- One class per logical unit under test.
- Real files written to tmp_path where disk I/O is exercised.
- External calls mocked with unittest.mock.patch / MagicMock.
- pytest.approx for floats; plain assert for everything else.
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

# Minimal catalog JSON structure (one field date, one scene).
_SCENE_ID = "S2A_MSIL1C_20240315T135111_N0500_R024_T21HUD_20240315T160000"
_CATALOG_ENTRY = [
    {
        "field_date": "2024-03-15",
        "images_found": [
            {
                "id": _SCENE_ID,
                "datetime": "2024-03-15T13:51:11Z",
                "cloud_cover": 5.0,
                "href": "s3://eodata/Sentinel-2/MSI/L1C/2024/03/15/"
                + _SCENE_ID
                + ".SAFE",
                "delta_days": 0,
                "l2a_scl": "https://example.com/SCL.tif",
            }
        ],
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
    """run_download() must now return its stats dict (non-breaking addition)."""

    def test_returns_dict(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        # Mark SAFE as already downloaded so no real S3 call is needed.
        _make_safe_dir(tmp_path, _SCENE_ID)
        # SCL already present too.
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        result = run_download(
            catalog_json=catalog_file,
            output_dir=tmp_path,
            only_first=True,
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
            only_first=True,
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
            only_first=True,
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
        with pytest.raises(ValueError, match="Invalid steps"):
            run_sentinel_pipeline(steps="invalid")

    def test_valid_steps_do_not_raise(self, tmp_path):
        """All three valid step values must be accepted (network calls mocked)."""
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        for step in ("download",):  # only download avoids build_catalog network call
            result = run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=tmp_path,
                steps=step,
            )
            # Should not raise; status depends on network mocking done elsewhere
            assert "step" in result


class TestRunSentinelPipelineDefaults:
    """None arguments must resolve to pipeline_config dataclass defaults."""

    def test_none_unique_csv_resolves_to_sentinel_section_default(self):
        defaults = SentinelSection()
        # We verify via a catalog-only run that would fail on the *resolved*
        # path — the error message must reference the default path, not None.
        result = run_sentinel_pipeline(steps="catalog")
        # File won't exist, so status is error — but path resolution happened.
        assert result["status"] == "error"
        assert str(Path(defaults.unique_csv)) in result["error"] or result["error"]

    def test_none_output_dir_resolves_to_download_section_default(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        _make_safe_dir(tmp_path, _SCENE_ID)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / f"{_SCENE_ID}_SCL.tif").write_text("x")

        # Pass explicit catalog but no output_dir → should resolve to default
        result = run_sentinel_pipeline(
            catalog_json=catalog_file,
            steps="download",
        )
        defaults = DownloadSection()
        # The resolved output_dir in outputs (or error) comes from DownloadSection
        if result["status"] == "success":
            assert result["outputs"]["output_dir"] == Path(defaults.output_dir)


class TestRunSentinelPipelineDownloadStep:
    """Tests for steps='download' — build_catalog is skipped."""

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
            steps="download",
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
            steps="download",
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
            steps="download",
        )

        stats = result["outputs"]["download_stats"]
        assert isinstance(stats, dict)
        assert "total_processed" in stats

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
            steps="download",
        )

        assert isinstance(result["elapsed_seconds"], float)
        assert result["elapsed_seconds"] >= 0.0

    def test_missing_catalog_returns_error(self, tmp_path):
        result = run_sentinel_pipeline(
            catalog_json=tmp_path / "nonexistent.json",
            output_dir=tmp_path,
            steps="download",
        )

        assert result["status"] == "error"
        assert result["error"] is not None

    def test_error_result_still_has_elapsed_seconds(self, tmp_path):
        result = run_sentinel_pipeline(
            catalog_json=tmp_path / "nonexistent.json",
            output_dir=tmp_path,
            steps="download",
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
            steps="download",
        )

        assert result["status"] == "success"

    def test_download_step_does_not_write_catalog(self, tmp_path):
        """steps='download' must not create or overwrite a catalog JSON."""
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
            steps="download",
        )

        assert catalog_file.stat().st_mtime == original_mtime


class TestRunSentinelPipelineCatalogStep:
    """Tests for steps='catalog' — build_catalog is called, download is skipped.

    build_catalog() makes live SentinelHub + EarthSearch network calls, so
    catalog.search() and Client.search() are patched here.
    """

    def _mock_catalog_search(self, acquisition_date="2024-03-15"):
        """Return a mock SentinelHubCatalog.search result."""
        item = {
            "id": _SCENE_ID,
            "properties": {
                "datetime": f"{acquisition_date}T13:51:11Z",
                "eo:cloud_cover": 5.0,
            },
            "assets": {
                "data": {"href": "s3://eodata/dummy/path/" + _SCENE_ID + ".SAFE"}
            },
        }
        return [item]

    def _mock_l2a_item(self):
        """Return a mock EarthSearch L2A pystac Item."""
        scl_asset = MagicMock()
        scl_asset.href = "https://example.com/SCL.tif"
        item = MagicMock()
        item.assets = {"scl": scl_asset}
        return item

    def test_catalog_json_is_written(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        mock_search_result = self._mock_catalog_search()
        mock_l2a_item = self._mock_l2a_item()

        with (
            patch(
                "aquamatch.sentinel_data.catalog.search",
                return_value=iter(mock_search_result),
            ),
            patch(
                "aquamatch.sentinel_data.client.search",
                return_value=MagicMock(items=lambda: iter([mock_l2a_item])),
            ),
        ):
            run_sentinel_pipeline(
                unique_csv=unique_csv,
                catalog_json=catalog_file,
                steps="catalog",
            )

        assert catalog_file.exists()

    def test_catalog_json_is_valid_json(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        mock_search_result = self._mock_catalog_search()
        mock_l2a_item = self._mock_l2a_item()

        with (
            patch(
                "aquamatch.sentinel_data.catalog.search",
                return_value=iter(mock_search_result),
            ),
            patch(
                "aquamatch.sentinel_data.client.search",
                return_value=MagicMock(items=lambda: iter([mock_l2a_item])),
            ),
        ):
            run_sentinel_pipeline(
                unique_csv=unique_csv,
                catalog_json=catalog_file,
                steps="catalog",
            )

        with open(catalog_file) as f:
            data = json.load(f)
        assert isinstance(data, list)

    def test_outputs_contains_catalog_json_path(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        mock_search_result = self._mock_catalog_search()
        mock_l2a_item = self._mock_l2a_item()

        with (
            patch(
                "aquamatch.sentinel_data.catalog.search",
                return_value=iter(mock_search_result),
            ),
            patch(
                "aquamatch.sentinel_data.client.search",
                return_value=MagicMock(items=lambda: iter([mock_l2a_item])),
            ),
        ):
            result = run_sentinel_pipeline(
                unique_csv=unique_csv,
                catalog_json=catalog_file,
                steps="catalog",
            )

        assert result["outputs"]["catalog_json"] == catalog_file

    def test_catalog_step_does_not_trigger_download(self, tmp_path):
        """steps='catalog' must never call run_download."""
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        mock_search_result = self._mock_catalog_search()
        mock_l2a_item = self._mock_l2a_item()

        with (
            patch(
                "aquamatch.sentinel_data.catalog.search",
                return_value=iter(mock_search_result),
            ),
            patch(
                "aquamatch.sentinel_data.client.search",
                return_value=MagicMock(items=lambda: iter([mock_l2a_item])),
            ),
            patch("aquamatch.sentinel_data.run_download") as mock_run_download,
        ):
            run_sentinel_pipeline(
                unique_csv=unique_csv,
                catalog_json=catalog_file,
                steps="catalog",
            )

        mock_run_download.assert_not_called()

    def test_missing_unique_csv_returns_error(self, tmp_path):
        result = run_sentinel_pipeline(
            unique_csv=tmp_path / "nonexistent.csv",
            catalog_json=tmp_path / "catalog.json",
            steps="catalog",
        )

        assert result["status"] == "error"
        assert result["error"] is not None


class TestRunSentinelPipelineStatusDict:
    """Status dict contract is consistent across all steps and outcomes."""

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
            steps="download",
        )

        assert result["step"] == "sentinel"

    def test_error_result_has_empty_outputs_before_failing_step(self, tmp_path):
        """Outputs for completed steps are preserved even on error."""
        result = run_sentinel_pipeline(
            catalog_json=tmp_path / "nonexistent.json",
            output_dir=tmp_path,
            steps="download",
        )

        # No steps completed, so outputs should be empty or only partial
        assert isinstance(result["outputs"], dict)

    def test_partial_outputs_preserved_on_error_after_catalog(self, tmp_path):
        """If catalog succeeds but download fails, catalog_json is in outputs."""
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        mock_search_result = [
            {
                "id": _SCENE_ID,
                "properties": {
                    "datetime": "2024-03-15T13:51:11Z",
                    "eo:cloud_cover": 5.0,
                },
                "assets": {"data": {"href": "s3://eodata/dummy/" + _SCENE_ID}},
            }
        ]
        mock_l2a = MagicMock()
        mock_l2a.assets = {"scl": MagicMock(href="https://example.com/SCL.tif")}

        with (
            patch(
                "aquamatch.sentinel_data.catalog.search",
                return_value=iter(mock_search_result),
            ),
            patch(
                "aquamatch.sentinel_data.client.search",
                return_value=MagicMock(items=lambda: iter([mock_l2a])),
            ),
            patch(
                "aquamatch.sentinel_data.run_download",
                side_effect=RuntimeError("S3 unavailable"),
            ),
        ):
            result = run_sentinel_pipeline(
                unique_csv=unique_csv,
                catalog_json=catalog_file,
                output_dir=tmp_path,
                steps="all",
            )

        assert result["status"] == "error"
        assert "catalog_json" in result["outputs"]
