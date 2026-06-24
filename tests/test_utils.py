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

import pandas as pd
import pytest

from aquamatch.utils import (
    _flatten_images,
    _best_image_within_tolerance,
    _compute_metrics_for_tolerance,
    analyze_temporal_opportunity,
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
