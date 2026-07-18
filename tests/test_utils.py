"""
Unit tests for aquamatch/utils.py — analyze_temporal_opportunity and helpers.

Tests use synthetic catalog data written to tmp_path so there is no
dependency on real satellite data or network access.

Conventions (matching the existing test suite):
  - One class per logical unit under test.
  - Real JSON files written to tmp_path — no mocking of json.load.
  - pytest.approx for floats; plain assert for everything else.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import rasterio.warp
import rioxarray  # noqa: F401 - registers the .rio accessor
import xarray as xr
from rasterio.crs import CRS

from aquamatch.utils import (
    _flatten_images,
    _iter_bucketed_images,
    _get_download_status,
    _rio_prepare,
    _best_image_within_tolerance,
    _compute_metrics_for_tolerance,
    analyze_temporal_opportunity,
    audit_downloads,
    extract_l2w_pixel_values,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _img(delta_days: int, cloud_cover: float, id_: str = "S2A") -> dict:
    """Build a minimal image dict."""
    return {"id": id_, "delta_days": delta_days, "cloud_cover": cloud_cover}


def _entry(field_date: str, same_day=None, previous=None, posterior=None) -> dict:
    """Build a bucketed catalog entry."""
    return {
        "field_date": field_date,
        "images_found": {
            "same_day": same_day or [],
            "previous": previous or [],
            "posterior": posterior or [],
        },
    }


def _write_catalog(tmp_path: Path, entries: list[dict]) -> Path:
    p = tmp_path / "catalog.json"
    p.write_text(json.dumps(entries))
    return p


# ---------------------------------------------------------------------------
# _flatten_images
# ---------------------------------------------------------------------------


class TestFlattenImages:

    def test_bucketed_dict_flattened(self):
        images_found = {
            "same_day": [_img(0, 5, "SD")],
            "previous": [_img(1, 3, "PV")],
            "posterior": [_img(2, 8, "PT")],
        }
        result = _flatten_images(images_found)
        assert len(result) == 3
        ids = {r["id"] for r in result}
        assert ids == {"SD", "PV", "PT"}

    def test_empty_buckets_return_empty_list(self):
        images_found = {"same_day": [], "previous": [], "posterior": []}
        assert _flatten_images(images_found) == []

    def test_legacy_flat_list_returned_as_is(self):
        images = [_img(0, 5), _img(1, 3)]
        assert _flatten_images(images) == images

    def test_partial_buckets_flattened(self):
        images_found = {"same_day": [_img(0, 5)], "previous": [], "posterior": []}
        result = _flatten_images(images_found)
        assert len(result) == 1

    def test_order_same_day_previous_posterior(self):
        images_found = {
            "same_day": [_img(0, 5, "SD")],
            "previous": [_img(1, 3, "PV")],
            "posterior": [_img(1, 8, "PT")],
        }
        result = _flatten_images(images_found)
        assert result[0]["id"] == "SD"
        assert result[1]["id"] == "PV"
        assert result[2]["id"] == "PT"


# ---------------------------------------------------------------------------
# _best_image_within_tolerance
# ---------------------------------------------------------------------------


class TestBestImageWithinTolerance:

    def test_returns_none_when_no_candidates(self):
        assert _best_image_within_tolerance([], max_delta=3) is None

    def test_returns_none_when_all_exceed_tolerance(self):
        images = [_img(2, 5), _img(3, 3)]
        assert _best_image_within_tolerance(images, max_delta=1) is None

    def test_returns_only_candidate(self):
        images = [_img(0, 5)]
        result = _best_image_within_tolerance(images, max_delta=2)
        assert result["delta_days"] == 0

    def test_prefers_minimum_delta_days(self):
        images = [_img(2, 3, "FAR"), _img(0, 8, "CLOSE")]
        result = _best_image_within_tolerance(images, max_delta=3)
        assert result["id"] == "CLOSE"

    def test_breaks_ties_by_cloud_cover(self):
        images = [_img(1, 20, "HIGH_CLOUD"), _img(1, 5, "LOW_CLOUD")]
        result = _best_image_within_tolerance(images, max_delta=2)
        assert result["id"] == "LOW_CLOUD"

    def test_tolerance_boundary_inclusive(self):
        images = [_img(3, 5)]
        result = _best_image_within_tolerance(images, max_delta=3)
        assert result is not None

    def test_image_just_outside_tolerance_excluded(self):
        images = [_img(4, 5)]
        assert _best_image_within_tolerance(images, max_delta=3) is None

    def test_zero_tolerance_only_same_day(self):
        images = [_img(0, 5, "SD"), _img(1, 3, "PV")]
        result = _best_image_within_tolerance(images, max_delta=0)
        assert result["id"] == "SD"


# ---------------------------------------------------------------------------
# _compute_metrics_for_tolerance
# ---------------------------------------------------------------------------


class TestComputeMetricsForTolerance:

    def test_returns_dict_with_expected_keys(self):
        entries = [_entry("2025-08-01", same_day=[_img(0, 5)])]
        result = _compute_metrics_for_tolerance(entries, max_delta=0)
        expected_keys = {
            "delta_days",
            "n_dates",
            "n_available",
            "availability",
            "opportunity_cost",
            "mean_cloud_cover",
            "median_cloud_cover",
        }
        assert set(result.keys()) == expected_keys

    def test_full_availability_when_all_dates_have_image(self):
        entries = [
            _entry("2025-08-01", same_day=[_img(0, 5)]),
            _entry("2025-08-02", same_day=[_img(0, 3)]),
        ]
        result = _compute_metrics_for_tolerance(entries, max_delta=0)
        assert result["availability"] == pytest.approx(100.0)
        assert result["opportunity_cost"] == pytest.approx(0.0)

    def test_zero_availability_when_no_images(self):
        entries = [_entry("2025-08-01"), _entry("2025-08-02")]
        result = _compute_metrics_for_tolerance(entries, max_delta=0)
        assert result["availability"] == pytest.approx(0.0)
        assert result["opportunity_cost"] == pytest.approx(100.0)

    def test_partial_availability(self):
        entries = [
            _entry("2025-08-01", same_day=[_img(0, 5)]),
            _entry("2025-08-02"),  # no image
        ]
        result = _compute_metrics_for_tolerance(entries, max_delta=0)
        assert result["availability"] == pytest.approx(50.0)
        assert result["opportunity_cost"] == pytest.approx(50.0)

    def test_availability_increases_with_tolerance(self):
        entries = [
            _entry("2025-08-01", same_day=[_img(0, 5)]),
            _entry("2025-08-02", previous=[_img(2, 3)]),  # only available at d>=2
        ]
        result_d0 = _compute_metrics_for_tolerance(entries, max_delta=0)
        result_d2 = _compute_metrics_for_tolerance(entries, max_delta=2)
        assert result_d2["availability"] > result_d0["availability"]

    def test_mean_cloud_cover_computed_correctly(self):
        entries = [
            _entry("2025-08-01", same_day=[_img(0, 10)]),
            _entry("2025-08-02", same_day=[_img(0, 20)]),
        ]
        result = _compute_metrics_for_tolerance(entries, max_delta=0)
        assert result["mean_cloud_cover"] == pytest.approx(15.0)

    def test_median_cloud_cover_odd_count(self):
        entries = [
            _entry("2025-08-01", same_day=[_img(0, 5)]),
            _entry("2025-08-02", same_day=[_img(0, 10)]),
            _entry("2025-08-03", same_day=[_img(0, 15)]),
        ]
        result = _compute_metrics_for_tolerance(entries, max_delta=0)
        assert result["median_cloud_cover"] == pytest.approx(10.0)

    def test_median_cloud_cover_even_count(self):
        entries = [
            _entry("2025-08-01", same_day=[_img(0, 10)]),
            _entry("2025-08-02", same_day=[_img(0, 20)]),
        ]
        result = _compute_metrics_for_tolerance(entries, max_delta=0)
        assert result["median_cloud_cover"] == pytest.approx(15.0)

    def test_mean_cloud_cover_none_when_no_images_available(self):
        entries = [_entry("2025-08-01")]
        result = _compute_metrics_for_tolerance(entries, max_delta=0)
        assert result["mean_cloud_cover"] is None
        assert result["median_cloud_cover"] is None

    def test_delta_days_field_matches_input(self):
        entries = [_entry("2025-08-01", same_day=[_img(0, 5)])]
        assert _compute_metrics_for_tolerance(entries, max_delta=3)["delta_days"] == 3

    def test_n_dates_and_n_available_correct(self):
        entries = [
            _entry("2025-08-01", same_day=[_img(0, 5)]),
            _entry("2025-08-02"),
            _entry("2025-08-03", previous=[_img(1, 8)]),
        ]
        result = _compute_metrics_for_tolerance(entries, max_delta=1)
        assert result["n_dates"] == 3
        assert result["n_available"] == 2


# ---------------------------------------------------------------------------
# analyze_temporal_opportunity
# ---------------------------------------------------------------------------


class TestAnalyzeTemporalOpportunity:

    def _make_catalog(self, tmp_path: Path, n_dates: int = 3) -> Path:
        entries = [
            _entry(
                f"2025-08-0{i+1}",
                same_day=[_img(0, i * 5)] if i % 2 == 0 else [],
                previous=[_img(1, 8)] if i % 2 == 1 else [],
            )
            for i in range(n_dates)
        ]
        return _write_catalog(tmp_path, entries)

    # --- Return value ---

    def test_returns_dataframe_by_default(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        result = analyze_temporal_opportunity(catalog, out, max_delta_days=3)
        assert isinstance(result, pd.DataFrame)

    def test_returns_none_when_return_dataframe_false(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        result = analyze_temporal_opportunity(
            catalog, out, max_delta_days=3, return_dataframe=False
        )
        assert result is None

    # --- DataFrame shape and columns ---

    def test_dataframe_has_correct_number_of_rows(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        df = analyze_temporal_opportunity(catalog, out, max_delta_days=5)
        assert len(df) == 6  # 0..5 inclusive

    def test_dataframe_has_expected_columns(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        df = analyze_temporal_opportunity(catalog, out, max_delta_days=2)
        expected = {
            "delta_days",
            "n_dates",
            "n_available",
            "availability",
            "opportunity_cost",
            "mean_cloud_cover",
            "median_cloud_cover",
        }
        assert expected.issubset(set(df.columns))

    def test_delta_days_column_is_0_to_max(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        df = analyze_temporal_opportunity(catalog, out, max_delta_days=4)
        assert list(df["delta_days"]) == [0, 1, 2, 3, 4]

    # --- Metric correctness ---

    def test_availability_plus_opportunity_cost_equals_100(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        df = analyze_temporal_opportunity(catalog, out, max_delta_days=3)
        for _, row in df.iterrows():
            assert row["availability"] + row["opportunity_cost"] == pytest.approx(100.0)

    def test_availability_monotonically_non_decreasing(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        df = analyze_temporal_opportunity(catalog, out, max_delta_days=5)
        avail = list(df["availability"])
        assert all(avail[i] <= avail[i + 1] for i in range(len(avail) - 1))

    def test_opportunity_cost_monotonically_non_increasing(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        df = analyze_temporal_opportunity(catalog, out, max_delta_days=5)
        opp = list(df["opportunity_cost"])
        assert all(opp[i] >= opp[i + 1] for i in range(len(opp) - 1))

    def test_full_availability_at_max_delta_when_all_have_images(self, tmp_path):
        entries = [_entry(f"2025-08-0{i+1}", same_day=[_img(0, 5)]) for i in range(3)]
        catalog = _write_catalog(tmp_path, entries)
        out = tmp_path / "fig.png"
        df = analyze_temporal_opportunity(catalog, out, max_delta_days=2)
        assert df.iloc[0]["availability"] == pytest.approx(100.0)

    # --- Figure output ---

    def test_figure_file_created(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        analyze_temporal_opportunity(catalog, out, max_delta_days=2)
        assert out.exists()

    def test_figure_parent_directory_created(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "reports" / "subdir" / "fig.png"
        analyze_temporal_opportunity(catalog, out, max_delta_days=2)
        assert out.exists()

    def test_figure_is_non_empty(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        analyze_temporal_opportunity(catalog, out, max_delta_days=2)
        assert out.stat().st_size > 0

    # --- Input validation ---

    def test_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            analyze_temporal_opportunity(
                tmp_path / "nonexistent.json",
                tmp_path / "fig.png",
            )

    def test_raises_value_error_for_negative_max_delta(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        with pytest.raises(ValueError, match="max_delta_days"):
            analyze_temporal_opportunity(
                catalog, tmp_path / "fig.png", max_delta_days=-1
            )

    def test_raises_value_error_for_empty_catalog(self, tmp_path):
        catalog = _write_catalog(tmp_path, [])
        with pytest.raises(ValueError, match="empty"):
            analyze_temporal_opportunity(catalog, tmp_path / "fig.png")

    # --- Edge cases ---

    def test_max_delta_days_zero(self, tmp_path):
        entries = [
            _entry("2025-08-01", same_day=[_img(0, 5)]),
            _entry("2025-08-02"),
        ]
        catalog = _write_catalog(tmp_path, entries)
        out = tmp_path / "fig.png"
        df = analyze_temporal_opportunity(catalog, out, max_delta_days=0)
        assert len(df) == 1
        assert df.iloc[0]["availability"] == pytest.approx(50.0)

    def test_accepts_string_paths(self, tmp_path):
        catalog = self._make_catalog(tmp_path)
        out = tmp_path / "fig.png"
        result = analyze_temporal_opportunity(str(catalog), str(out), max_delta_days=2)
        assert isinstance(result, pd.DataFrame)

    def test_legacy_flat_list_catalog_handled(self, tmp_path):
        """Old-style flat-list images_found must be processed without error."""
        entries = [
            {
                "field_date": "2025-08-01",
                "images_found": [_img(0, 5), _img(1, 3)],
            }
        ]
        catalog = _write_catalog(tmp_path, entries)
        out = tmp_path / "fig.png"
        df = analyze_temporal_opportunity(catalog, out, max_delta_days=2)
        assert df.iloc[0]["availability"] == pytest.approx(100.0)

    def test_single_field_date_catalog(self, tmp_path):
        entries = [_entry("2025-08-01", same_day=[_img(0, 10)])]
        catalog = _write_catalog(tmp_path, entries)
        out = tmp_path / "fig.png"
        df = analyze_temporal_opportunity(catalog, out, max_delta_days=2)
        assert df.iloc[0]["n_dates"] == 1
        assert df.iloc[0]["n_available"] == 1


# ---------------------------------------------------------------------------
# _iter_bucketed_images
# ---------------------------------------------------------------------------


class TestIterBucketedImages:
    def test_yields_bucket_tagged_pairs_in_order(self):
        images_found = {
            "same_day": [_img(0, 5, "SD1")],
            "previous": [_img(1, 3, "PV1")],
            "posterior": [_img(2, 8, "PT1")],
        }
        result = list(_iter_bucketed_images(images_found))
        assert result == [
            ("same_day", images_found["same_day"][0]),
            ("previous", images_found["previous"][0]),
            ("posterior", images_found["posterior"][0]),
        ]

    def test_missing_bucket_keys_default_to_empty(self):
        result = list(_iter_bucketed_images({"same_day": [_img(0, 5, "SD1")]}))
        assert result == [("same_day", {"id": "SD1", "delta_days": 0, "cloud_cover": 5})]

    def test_legacy_flat_list_yields_none_bucket(self):
        images = [_img(0, 5, "L1"), _img(1, 3, "L2")]
        result = list(_iter_bucketed_images(images))
        assert result == [(None, images[0]), (None, images[1])]

    def test_empty_dict_yields_nothing(self):
        assert list(_iter_bucketed_images({})) == []

    def test_empty_list_yields_nothing(self):
        assert list(_iter_bucketed_images([])) == []


# ---------------------------------------------------------------------------
# _get_download_status
# ---------------------------------------------------------------------------


class TestGetDownloadStatus:
    def test_safe_exists_true_for_populated_folder(self, tmp_path):
        safe_folder = tmp_path / "SCENE1"
        safe_folder.mkdir()
        (safe_folder / "dummy.xml").write_text("x")
        status = _get_download_status("SCENE1", tmp_path, download_scl=False)
        assert status["safe_exists"] is True

    def test_safe_exists_true_for_dot_safe_file(self, tmp_path):
        (tmp_path / "SCENE1.SAFE").write_text("x")
        status = _get_download_status("SCENE1", tmp_path, download_scl=False)
        assert status["safe_exists"] is True

    def test_safe_exists_false_when_folder_empty(self, tmp_path):
        (tmp_path / "SCENE1").mkdir()
        status = _get_download_status("SCENE1", tmp_path, download_scl=False)
        assert status["safe_exists"] is False

    def test_safe_exists_false_when_absent(self, tmp_path):
        status = _get_download_status("SCENE1", tmp_path, download_scl=False)
        assert status["safe_exists"] is False

    def test_scl_exists_none_when_download_scl_false(self, tmp_path):
        status = _get_download_status("SCENE1", tmp_path, download_scl=False)
        assert status["scl_exists"] is None

    def test_scl_exists_true_when_file_present(self, tmp_path):
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / "SCENE1_SCL.tif").write_bytes(b"x")
        status = _get_download_status("SCENE1", tmp_path, download_scl=True)
        assert status["scl_exists"] is True

    def test_scl_exists_false_when_file_absent(self, tmp_path):
        status = _get_download_status("SCENE1", tmp_path, download_scl=True)
        assert status["scl_exists"] is False

    def test_all_downloaded_requires_both_when_download_scl_true(self, tmp_path):
        safe_folder = tmp_path / "SCENE1"
        safe_folder.mkdir()
        (safe_folder / "dummy.xml").write_text("x")
        status = _get_download_status("SCENE1", tmp_path, download_scl=True)
        assert status["all_downloaded"] is False

        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / "SCENE1_SCL.tif").write_bytes(b"x")
        status = _get_download_status("SCENE1", tmp_path, download_scl=True)
        assert status["all_downloaded"] is True

    def test_all_downloaded_ignores_scl_when_download_scl_false(self, tmp_path):
        safe_folder = tmp_path / "SCENE1"
        safe_folder.mkdir()
        (safe_folder / "dummy.xml").write_text("x")
        status = _get_download_status("SCENE1", tmp_path, download_scl=False)
        assert status["all_downloaded"] is True


# ---------------------------------------------------------------------------
# audit_downloads
# ---------------------------------------------------------------------------


class TestAuditDownloads:
    def _touch_safe(self, output_dir: Path, scene_id: str) -> None:
        safe_folder = output_dir / scene_id
        safe_folder.mkdir(parents=True, exist_ok=True)
        (safe_folder / "dummy.xml").write_text("x")

    def _touch_scl(self, output_dir: Path, scene_id: str) -> None:
        scl_dir = output_dir / "scl"
        scl_dir.mkdir(parents=True, exist_ok=True)
        core_id = scene_id.split(".")[0]
        (scl_dir / f"{core_id}_SCL.tif").write_bytes(b"x")

    def test_returns_dataframe_with_expected_columns(self, tmp_path):
        entries = [_entry("2025-08-01", same_day=[_img(0, 5, "SD1")])]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        df = audit_downloads(catalog, out_dir)
        expected = {
            "field_date",
            "scene_id",
            "delta_days",
            "cloud_cover",
            "safe_exists",
            "scl_exists",
            "all_downloaded",
            "bucket",
        }
        assert set(df.columns) == expected

    def test_row_count_matches_total_images_across_buckets(self, tmp_path):
        entries = [
            _entry(
                "2025-08-01",
                same_day=[_img(0, 5, "SD1")],
                previous=[_img(1, 3, "PV1")],
                posterior=[_img(2, 8, "PT1")],
            )
        ]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        df = audit_downloads(catalog, out_dir)
        assert len(df) == 3

    def test_row_count_across_multiple_field_dates(self, tmp_path):
        entries = [
            _entry("2025-08-01", same_day=[_img(0, 5, "SD1")]),
            _entry(
                "2025-08-02", same_day=[_img(0, 3, "SD2")], previous=[_img(1, 4, "PV2")]
            ),
        ]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        df = audit_downloads(catalog, out_dir)
        assert len(df) == 3

    def test_scalar_columns_match_source_image(self, tmp_path):
        entries = [_entry("2025-08-01", same_day=[_img(3, 12.5, "SD1")])]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        df = audit_downloads(catalog, out_dir)
        row = df.iloc[0]
        assert row["field_date"] == "2025-08-01"
        assert row["scene_id"] == "SD1"
        assert row["delta_days"] == 3
        assert row["cloud_cover"] == pytest.approx(12.5)

    def test_bucket_column_tags_correctly(self, tmp_path):
        entries = [
            _entry(
                "2025-08-01",
                same_day=[_img(0, 5, "SD1")],
                previous=[_img(1, 3, "PV1")],
                posterior=[_img(2, 8, "PT1")],
            )
        ]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        df = audit_downloads(catalog, out_dir)
        by_id = df.set_index("scene_id")["bucket"]
        assert by_id["SD1"] == "same_day"
        assert by_id["PV1"] == "previous"
        assert by_id["PT1"] == "posterior"

    def test_safe_exists_true_when_folder_present(self, tmp_path):
        entries = [_entry("2025-08-01", same_day=[_img(0, 5, "S2A_TEST")])]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        self._touch_safe(out_dir, "S2A_TEST")
        df = audit_downloads(catalog, out_dir, download_scl=False)
        assert bool(df.iloc[0]["safe_exists"]) is True

    def test_safe_exists_false_when_absent(self, tmp_path):
        entries = [_entry("2025-08-01", same_day=[_img(0, 5, "S2A_TEST")])]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        df = audit_downloads(catalog, out_dir, download_scl=False)
        assert bool(df.iloc[0]["safe_exists"]) is False

    def test_all_downloaded_true_when_safe_and_scl_present(self, tmp_path):
        entries = [_entry("2025-08-01", same_day=[_img(0, 5, "S2A_TEST")])]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        self._touch_safe(out_dir, "S2A_TEST")
        self._touch_scl(out_dir, "S2A_TEST")
        df = audit_downloads(catalog, out_dir, download_scl=True)
        row = df.iloc[0]
        assert bool(row["safe_exists"]) is True
        assert bool(row["scl_exists"]) is True
        assert bool(row["all_downloaded"]) is True

    def test_all_downloaded_false_when_scl_missing(self, tmp_path):
        entries = [_entry("2025-08-01", same_day=[_img(0, 5, "S2A_TEST")])]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        self._touch_safe(out_dir, "S2A_TEST")
        df = audit_downloads(catalog, out_dir, download_scl=True)
        row = df.iloc[0]
        assert bool(row["safe_exists"]) is True
        assert bool(row["scl_exists"]) is False
        assert bool(row["all_downloaded"]) is False

    def test_scl_exists_none_when_download_scl_false(self, tmp_path):
        entries = [_entry("2025-08-01", same_day=[_img(0, 5, "S2A_TEST")])]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        self._touch_safe(out_dir, "S2A_TEST")
        df = audit_downloads(catalog, out_dir, download_scl=False)
        assert df.iloc[0]["scl_exists"] is None
        assert bool(df.iloc[0]["all_downloaded"]) is True

    def test_mixed_downloaded_and_missing_scenes(self, tmp_path):
        entries = [
            _entry(
                "2025-08-01",
                same_day=[_img(0, 5, "DOWNLOADED")],
                previous=[_img(1, 3, "MISSING")],
            )
        ]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        self._touch_safe(out_dir, "DOWNLOADED")
        df = audit_downloads(catalog, out_dir, download_scl=False)
        by_id = df.set_index("scene_id")["all_downloaded"]
        assert bool(by_id["DOWNLOADED"]) is True
        assert bool(by_id["MISSING"]) is False

    def test_raises_value_error_for_empty_catalog(self, tmp_path):
        catalog = _write_catalog(tmp_path, [])
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        with pytest.raises(ValueError, match="empty"):
            audit_downloads(catalog, out_dir)

    def test_entries_with_no_images_produce_empty_dataframe_with_columns(self, tmp_path):
        entries = [_entry("2025-08-01")]  # all buckets empty
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        df = audit_downloads(catalog, out_dir)
        assert len(df) == 0
        expected = {
            "field_date",
            "scene_id",
            "delta_days",
            "cloud_cover",
            "safe_exists",
            "scl_exists",
            "all_downloaded",
            "bucket",
        }
        assert set(df.columns) == expected

    def test_legacy_flat_list_catalog_handled(self, tmp_path):
        entries = [
            {
                "field_date": "2025-08-01",
                "images_found": [_img(0, 5, "LEGACY1"), _img(1, 3, "LEGACY2")],
            }
        ]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        df = audit_downloads(catalog, out_dir, download_scl=False)
        assert len(df) == 2
        assert df["bucket"].isna().all()

    def test_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            audit_downloads(tmp_path / "nonexistent.json", tmp_path)

    def test_accepts_string_paths(self, tmp_path):
        entries = [_entry("2025-08-01", same_day=[_img(0, 5, "SD1")])]
        catalog = _write_catalog(tmp_path, entries)
        out_dir = tmp_path / "downloads"
        out_dir.mkdir()
        df = audit_downloads(str(catalog), str(out_dir))
        assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# _rio_prepare
# ---------------------------------------------------------------------------


class TestRioPrepare:
    def test_returns_none_triple_when_no_spatial_dims(self):
        da = xr.DataArray(np.zeros((3, 3)), dims=("a", "b"))
        result = _rio_prepare(da)
        assert result == (None, None, None)

    def test_returns_none_triple_when_no_crs(self):
        da = xr.DataArray(
            np.zeros((3, 3)),
            dims=("y", "x"),
            coords={"y": [0, 1, 2], "x": [0, 1, 2]},
        )
        result = _rio_prepare(da)
        assert result == (None, None, None)

    def test_returns_prepared_array_when_valid(self):
        da = xr.DataArray(
            np.zeros((3, 3)),
            dims=("y", "x"),
            coords={"y": [0, 1, 2], "x": [0, 1, 2]},
        )
        da = da.rio.write_crs(CRS.from_epsg(32721))
        result_da, x_dim, y_dim = _rio_prepare(da)
        assert result_da is not None
        assert x_dim == "x"
        assert y_dim == "y"
        assert result_da.rio.crs is not None


# ---------------------------------------------------------------------------
# extract_l2w_pixel_values fixtures
# ---------------------------------------------------------------------------

L2W_TEST_CRS = CRS.from_epsg(32721)
L2W_Y0 = 6_300_000.0
L2W_X0 = 500_000.0
L2W_RES = 10.0


def _l2w_coords(ny: int, nx: int):
    y = np.linspace(L2W_Y0, L2W_Y0 - (ny - 1) * L2W_RES, ny)
    x = np.linspace(L2W_X0, L2W_X0 + (nx - 1) * L2W_RES, nx)
    return y, x


def _make_l2w_nc(path: Path, variables: dict) -> Path:
    """Write a synthetic multi-variable NetCDF shaped like an ACOLITE L2W
    scene. ``variables`` maps variable name -> 2D numpy array; all arrays
    must share the same shape."""
    names = list(variables)
    ny, nx = variables[names[0]].shape
    y, x = _l2w_coords(ny, nx)

    data_arrays = {}
    for name, arr in variables.items():
        assert arr.shape == (ny, nx)
        da = xr.DataArray(
            arr.astype("float32"), dims=("y", "x"), coords={"y": y, "x": x}, name=name
        )
        data_arrays[name] = da.rio.write_crs(L2W_TEST_CRS)

    ds = xr.Dataset(data_arrays)
    ds.to_netcdf(path)
    return path


def _lonlat_for_pixel(row: int, col: int, ny: int, nx: int):
    """Convert a pixel's exact center to EPSG:4326 lon/lat using the same
    rasterio.warp.transform the function under test relies on — this
    guarantees the resulting station coordinate round-trips to exactly
    this pixel under nearest-neighbor matching."""
    y, x = _l2w_coords(ny, nx)
    lons, lats = rasterio.warp.transform(
        L2W_TEST_CRS, "EPSG:4326", [x[col]], [y[row]]
    )
    return lons[0], lats[0]


def _stations_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# extract_l2w_pixel_values
# ---------------------------------------------------------------------------


class TestExtractL2wPixelValues:
    def test_exact_mean_known_neighborhood(self, tmp_path):
        arr = np.arange(49, dtype="float64").reshape(7, 7)
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})

        lon, lat = _lonlat_for_pixel(3, 3, 7, 7)
        stations = _stations_df(
            [{"id": "A", "date": "2024-01-01", "latitud": lat, "longitud": lon}]
        )

        result = extract_l2w_pixel_values(
            nc, stations, variables=["chl_oc3"], window_size=3
        )

        expected_mean = arr[2:5, 2:5].mean()
        assert result.loc[0, "chl_oc3_mean"] == pytest.approx(expected_mean)
        assert result.loc[0, "chl_oc3_n_valid_px"] == 9
        assert bool(result.loc[0, "in_bounds"]) is True
        assert result.loc[0, "id"] == "A"
        assert result.loc[0, "date"] == "2024-01-01"

    def test_window_clipped_at_raster_edge(self, tmp_path):
        arr = np.arange(49, dtype="float64").reshape(7, 7)
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})

        # Top-left corner pixel (0, 0) — a full 3x3 window would go out of range.
        lon, lat = _lonlat_for_pixel(0, 0, 7, 7)
        stations = _stations_df([{"latitud": lat, "longitud": lon}])

        result = extract_l2w_pixel_values(
            nc, stations, variables=["chl_oc3"], window_size=3
        )

        expected = arr[0:2, 0:2]
        assert result.loc[0, "chl_oc3_n_valid_px"] == expected.size
        assert result.loc[0, "chl_oc3_mean"] == pytest.approx(expected.mean())
        assert bool(result.loc[0, "in_bounds"]) is True

    def test_nan_pixels_excluded_from_mean_and_count(self, tmp_path):
        arr = np.arange(25, dtype="float64").reshape(5, 5)
        arr[1, 1] = np.nan
        arr[2, 2] = np.nan
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})

        lon, lat = _lonlat_for_pixel(2, 2, 5, 5)
        stations = _stations_df([{"latitud": lat, "longitud": lon}])

        result = extract_l2w_pixel_values(
            nc, stations, variables=["chl_oc3"], window_size=3
        )

        window = arr[1:4, 1:4]
        valid = window[~np.isnan(window)]
        assert result.loc[0, "chl_oc3_n_valid_px"] == valid.size
        assert result.loc[0, "chl_oc3_mean"] == pytest.approx(valid.mean())

    def test_fully_masked_window_gives_nan_mean_and_zero_count(self, tmp_path):
        arr = np.full((5, 5), np.nan, dtype="float64")
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})

        lon, lat = _lonlat_for_pixel(2, 2, 5, 5)
        stations = _stations_df([{"latitud": lat, "longitud": lon}])

        result = extract_l2w_pixel_values(
            nc, stations, variables=["chl_oc3"], window_size=3
        )

        assert result.loc[0, "chl_oc3_n_valid_px"] == 0
        assert np.isnan(result.loc[0, "chl_oc3_mean"])
        # Still in bounds — this is "cloud-masked", not "wrong tile".
        assert bool(result.loc[0, "in_bounds"]) is True

    def test_out_of_bounds_station_gets_nan_and_flag(self, tmp_path):
        arr = np.arange(49, dtype="float64").reshape(7, 7)
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})

        # (0, 0) lon/lat is nowhere near UTM zone 21S — far outside bounds.
        stations = _stations_df([{"latitud": 0.0, "longitud": 0.0}])

        result = extract_l2w_pixel_values(
            nc, stations, variables=["chl_oc3"], window_size=3
        )

        assert bool(result.loc[0, "in_bounds"]) is False
        assert np.isnan(result.loc[0, "chl_oc3_mean"])
        assert result.loc[0, "chl_oc3_n_valid_px"] == 0

    def test_multiple_stations_each_get_independent_correct_rows(self, tmp_path):
        arr = np.arange(49, dtype="float64").reshape(7, 7)
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})

        lon_in, lat_in = _lonlat_for_pixel(3, 3, 7, 7)
        rows = [
            {
                "id": "in_bounds_station",
                "date": "2024-01-01",
                "s2_tile": "T21HWD",
                "latitud": lat_in,
                "longitud": lon_in,
            },
            {
                "id": "oob_station",
                "date": "2024-01-02",
                "s2_tile": "T21HWD",
                "latitud": 0.0,
                "longitud": 0.0,
            },
        ]
        stations = _stations_df(rows)

        result = extract_l2w_pixel_values(
            nc, stations, variables=["chl_oc3"], window_size=3
        )

        assert len(result) == 2
        assert list(result["id"]) == ["in_bounds_station", "oob_station"]
        assert list(result["date"]) == ["2024-01-01", "2024-01-02"]
        assert list(result["s2_tile"]) == ["T21HWD", "T21HWD"]

        expected_mean = arr[2:5, 2:5].mean()
        assert result.loc[0, "chl_oc3_mean"] == pytest.approx(expected_mean)
        assert bool(result.loc[0, "in_bounds"]) is True

        assert np.isnan(result.loc[1, "chl_oc3_mean"])
        assert bool(result.loc[1, "in_bounds"]) is False

    def test_variables_none_picks_up_all_variables(self, tmp_path):
        arr1 = np.arange(49, dtype="float64").reshape(7, 7)
        arr2 = arr1 * 2
        nc = _make_l2w_nc(
            tmp_path / "scene_L2W.nc",
            {"chl_oc3": arr1, "tur_dogliotti": arr2},
        )

        lon, lat = _lonlat_for_pixel(3, 3, 7, 7)
        stations = _stations_df([{"latitud": lat, "longitud": lon}])

        result = extract_l2w_pixel_values(nc, stations, variables=None, window_size=3)

        for var in ("chl_oc3", "tur_dogliotti"):
            assert f"{var}_mean" in result.columns
            assert f"{var}_n_valid_px" in result.columns
        assert result.loc[0, "tur_dogliotti_mean"] == pytest.approx(
            2 * result.loc[0, "chl_oc3_mean"]
        )

    def test_variables_filters_to_requested_subset(self, tmp_path):
        arr1 = np.arange(49, dtype="float64").reshape(7, 7)
        arr2 = arr1 * 2
        nc = _make_l2w_nc(
            tmp_path / "scene_L2W.nc",
            {"chl_oc3": arr1, "tur_dogliotti": arr2},
        )

        lon, lat = _lonlat_for_pixel(3, 3, 7, 7)
        stations = _stations_df([{"latitud": lat, "longitud": lon}])

        result = extract_l2w_pixel_values(
            nc, stations, variables=["chl_oc3"], window_size=3
        )

        assert "chl_oc3_mean" in result.columns
        assert "tur_dogliotti_mean" not in result.columns

    def test_nonexistent_variable_raises_value_error(self, tmp_path):
        arr = np.arange(49, dtype="float64").reshape(7, 7)
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})
        stations = _stations_df([{"latitud": -33.0, "longitud": -58.0}])

        with pytest.raises(ValueError, match="No exportable variables found"):
            extract_l2w_pixel_values(nc, stations, variables=["does_not_exist"])

    @pytest.mark.parametrize("window_size", [0, -1, 2, 4])
    def test_invalid_window_size_raises_value_error(self, tmp_path, window_size):
        arr = np.arange(49, dtype="float64").reshape(7, 7)
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})
        stations = _stations_df([{"latitud": -33.0, "longitud": -58.0}])

        with pytest.raises(ValueError, match="window_size"):
            extract_l2w_pixel_values(nc, stations, window_size=window_size)

    def test_missing_l2w_nc_raises_file_not_found(self, tmp_path):
        stations = _stations_df([{"latitud": -33.0, "longitud": -58.0}])

        with pytest.raises(FileNotFoundError):
            extract_l2w_pixel_values(tmp_path / "does_not_exist_L2W.nc", stations)

    def test_empty_stations_returns_empty_but_correctly_columned(self, tmp_path):
        arr = np.arange(49, dtype="float64").reshape(7, 7)
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})
        stations = pd.DataFrame(columns=["id", "date", "latitud", "longitud"])

        result = extract_l2w_pixel_values(nc, stations, variables=["chl_oc3"])

        assert len(result) == 0
        assert "chl_oc3_mean" in result.columns
        assert "chl_oc3_n_valid_px" in result.columns
        assert "in_bounds" in result.columns

    def test_custom_lat_lon_column_names(self, tmp_path):
        arr = np.arange(49, dtype="float64").reshape(7, 7)
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})

        lon, lat = _lonlat_for_pixel(3, 3, 7, 7)
        stations = pd.DataFrame([{"lat": lat, "lon": lon}])

        result = extract_l2w_pixel_values(
            nc, stations, variables=["chl_oc3"], lat_col="lat", lon_col="lon"
        )

        expected_mean = arr[2:5, 2:5].mean()
        assert result.loc[0, "chl_oc3_mean"] == pytest.approx(expected_mean)

    def test_missing_lat_lon_column_raises_value_error(self, tmp_path):
        arr = np.arange(49, dtype="float64").reshape(7, 7)
        nc = _make_l2w_nc(tmp_path / "scene_L2W.nc", {"chl_oc3": arr})
        stations = pd.DataFrame([{"latitud": -33.0}])  # missing longitud

        with pytest.raises(ValueError, match="lon_col"):
            extract_l2w_pixel_values(nc, stations, variables=["chl_oc3"])
