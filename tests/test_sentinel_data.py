import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest
from sentinelhub import BBox, CRS

from aquamatch.sentinel_data import (
    create_bbox_from_point,
    search_images,
    build_catalog,
    run_download,
    get_download_status,
    get_scl_path,
    SCL_SUBDIR,
    _temporal_bucket,
    _empty_buckets,
    _select_scenes,
)

# ---------------------------------------------------------------------------
# _temporal_bucket
# ---------------------------------------------------------------------------


class TestTemporalBucket:
    """Tests for the _temporal_bucket helper."""

    def test_same_day(self):
        assert _temporal_bucket("2025-08-01", "2025-08-01") == "same_day"

    def test_previous(self):
        assert _temporal_bucket("2025-07-30", "2025-08-01") == "previous"

    def test_posterior(self):
        assert _temporal_bucket("2025-08-03", "2025-08-01") == "posterior"

    def test_previous_two_days(self):
        assert _temporal_bucket("2025-07-30", "2025-08-01") == "previous"

    def test_posterior_one_day(self):
        assert _temporal_bucket("2025-08-02", "2025-08-01") == "posterior"


# ---------------------------------------------------------------------------
# _empty_buckets
# ---------------------------------------------------------------------------


class TestEmptyBuckets:

    def test_returns_dict_with_three_keys(self):
        b = _empty_buckets()
        assert set(b.keys()) == {"same_day", "previous", "posterior"}

    def test_all_buckets_are_empty_lists(self):
        b = _empty_buckets()
        for key in b:
            assert b[key] == []


# ---------------------------------------------------------------------------
# _select_scenes
# ---------------------------------------------------------------------------


def _make_buckets(same_day=None, previous=None, posterior=None):
    """Build a minimal images_found dict for _select_scenes tests."""

    def _imgs(ids, delta, cloud=5):
        return [
            {
                "id": i,
                "delta_days": delta,
                "cloud_cover": cloud,
                "datetime": "2025-08-01T10:00:00Z",
            }
            for i in ids
        ]

    return {
        "same_day": _imgs(same_day or [], 0),
        "previous": [
            {
                "id": i,
                "delta_days": d,
                "cloud_cover": 5,
                "datetime": "2025-07-31T10:00:00Z",
            }
            for i, d in (previous or [])
        ],
        "posterior": [
            {
                "id": i,
                "delta_days": d,
                "cloud_cover": 5,
                "datetime": "2025-08-02T10:00:00Z",
            }
            for i, d in (posterior or [])
        ],
    }


class TestSelectScenes:
    """Tests for _select_scenes — the Step 4 seam."""

    # --- strategy="best", max_per_date=1 (mirrors strategy="best", max_per_date=1) ---

    def test_best_prefers_same_day(self):
        buckets = _make_buckets(
            same_day=["SD"], previous=[("PV", 1)], posterior=[("PT", 1)]
        )
        result = _select_scenes(buckets, strategy="best", max_per_date=1)
        assert len(result) == 1
        assert result[0]["id"] == "SD"

    def test_best_falls_back_to_previous_when_no_same_day(self):
        buckets = _make_buckets(previous=[("PV", 1)], posterior=[("PT", 1)])
        result = _select_scenes(buckets, strategy="best", max_per_date=1)
        assert len(result) == 1
        assert result[0]["id"] == "PV"

    def test_best_falls_back_to_posterior_when_no_same_day_or_previous(self):
        buckets = _make_buckets(posterior=[("PT", 1)])
        result = _select_scenes(buckets, strategy="best", max_per_date=1)
        assert len(result) == 1
        assert result[0]["id"] == "PT"

    def test_best_returns_empty_when_all_buckets_empty(self):
        result = _select_scenes(_make_buckets(), strategy="best", max_per_date=1)
        assert result == []

    # --- max_per_date > 1 ---

    def test_best_respects_max_per_date(self):
        buckets = _make_buckets(same_day=["SD1", "SD2"], previous=[("PV", 1)])
        result = _select_scenes(buckets, strategy="best", max_per_date=2)
        assert len(result) == 2
        assert result[0]["id"] == "SD1"
        assert result[1]["id"] == "SD2"

    def test_best_fills_quota_across_buckets(self):
        """If same_day has 1 and quota is 2, take 1 from same_day + 1 from previous."""
        buckets = _make_buckets(same_day=["SD"], previous=[("PV", 1)])
        result = _select_scenes(buckets, strategy="best", max_per_date=2)
        assert len(result) == 2
        assert result[0]["id"] == "SD"
        assert result[1]["id"] == "PV"

    # --- strategy="all" ---

    def test_all_returns_every_image(self):
        buckets = _make_buckets(
            same_day=["SD"], previous=[("PV", 1)], posterior=[("PT", 1)]
        )
        result = _select_scenes(buckets, strategy="all")
        assert len(result) == 3
        ids = {r["id"] for r in result}
        assert ids == {"SD", "PV", "PT"}

    def test_all_ignores_max_per_date(self):
        buckets = _make_buckets(same_day=["SD1", "SD2"], previous=[("PV", 1)])
        result = _select_scenes(buckets, strategy="all", max_per_date=1)
        assert len(result) == 3

    def test_all_returns_empty_when_all_buckets_empty(self):
        result = _select_scenes(_make_buckets(), strategy="all")
        assert result == []

    # --- max_cloud_cover filter ---

    def test_max_cloud_cover_filters_images(self):
        buckets = {
            "same_day": [
                {"id": "HIGH", "delta_days": 0, "cloud_cover": 25, "datetime": ""}
            ],
            "previous": [
                {"id": "LOW", "delta_days": 1, "cloud_cover": 5, "datetime": ""}
            ],
            "posterior": [],
        }
        result = _select_scenes(
            buckets, strategy="best", max_per_date=1, max_cloud_cover=10
        )
        assert len(result) == 1
        assert result[0]["id"] == "LOW"

    def test_max_cloud_cover_none_applies_no_filter(self):
        buckets = {
            "same_day": [
                {"id": "HIGH", "delta_days": 0, "cloud_cover": 80, "datetime": ""}
            ],
            "previous": [],
            "posterior": [],
        }
        result = _select_scenes(
            buckets, strategy="best", max_per_date=1, max_cloud_cover=None
        )
        assert result[0]["id"] == "HIGH"

    # --- strategy="same_day" ---

    def test_same_day_returns_only_same_day_bucket(self):
        buckets = _make_buckets(
            same_day=["SD"], previous=[("PV", 1)], posterior=[("PT", 1)]
        )
        result = _select_scenes(buckets, strategy="same_day", max_per_date=1)
        assert len(result) == 1
        assert result[0]["id"] == "SD"

    def test_same_day_returns_empty_when_no_same_day(self):
        buckets = _make_buckets(previous=[("PV", 1)], posterior=[("PT", 1)])
        result = _select_scenes(buckets, strategy="same_day", max_per_date=1)
        assert result == []

    def test_same_day_never_returns_previous_or_posterior(self):
        buckets = _make_buckets(previous=[("PV", 1)], posterior=[("PT", 1)])
        result = _select_scenes(buckets, strategy="same_day")
        ids = {r["id"] for r in result}
        assert "PV" not in ids
        assert "PT" not in ids

    def test_same_day_respects_max_per_date(self):
        buckets = _make_buckets(same_day=["SD1", "SD2", "SD3"])
        result = _select_scenes(buckets, strategy="same_day", max_per_date=2)
        assert len(result) == 2

    # --- strategy="previous" ---

    def test_previous_returns_same_day_when_available(self):
        buckets = _make_buckets(
            same_day=["SD"], previous=[("PV", 1)], posterior=[("PT", 1)]
        )
        result = _select_scenes(buckets, strategy="previous", max_per_date=1)
        assert result[0]["id"] == "SD"

    def test_previous_falls_back_to_previous_when_no_same_day(self):
        buckets = _make_buckets(previous=[("PV", 1)], posterior=[("PT", 1)])
        result = _select_scenes(buckets, strategy="previous", max_per_date=1)
        assert len(result) == 1
        assert result[0]["id"] == "PV"

    def test_previous_never_returns_posterior(self):
        buckets = _make_buckets(posterior=[("PT", 1)])
        result = _select_scenes(buckets, strategy="previous", max_per_date=1)
        assert result == []

    def test_previous_fills_quota_from_same_day_and_previous(self):
        buckets = _make_buckets(
            same_day=["SD"], previous=[("PV", 1)], posterior=[("PT", 1)]
        )
        result = _select_scenes(buckets, strategy="previous", max_per_date=2)
        assert len(result) == 2
        ids = [r["id"] for r in result]
        assert "SD" in ids
        assert "PV" in ids
        assert "PT" not in ids

    # --- strategy="posterior" ---

    def test_posterior_returns_same_day_when_available(self):
        buckets = _make_buckets(
            same_day=["SD"], previous=[("PV", 1)], posterior=[("PT", 1)]
        )
        result = _select_scenes(buckets, strategy="posterior", max_per_date=1)
        assert result[0]["id"] == "SD"

    def test_posterior_falls_back_to_posterior_when_no_same_day(self):
        buckets = _make_buckets(previous=[("PV", 1)], posterior=[("PT", 1)])
        result = _select_scenes(buckets, strategy="posterior", max_per_date=1)
        assert len(result) == 1
        assert result[0]["id"] == "PT"

    def test_posterior_never_returns_previous(self):
        buckets = _make_buckets(previous=[("PV", 1)])
        result = _select_scenes(buckets, strategy="posterior", max_per_date=1)
        assert result == []

    def test_posterior_fills_quota_from_same_day_and_posterior(self):
        buckets = _make_buckets(
            same_day=["SD"], previous=[("PV", 1)], posterior=[("PT", 1)]
        )
        result = _select_scenes(buckets, strategy="posterior", max_per_date=2)
        assert len(result) == 2
        ids = [r["id"] for r in result]
        assert "SD" in ids
        assert "PT" in ids
        assert "PV" not in ids

    # --- cloud cover filter interacts correctly with all strategies ---

    def test_same_day_cloud_filter_applied(self):
        buckets = {
            "same_day": [
                {"id": "HIGH", "delta_days": 0, "cloud_cover": 30, "datetime": ""}
            ],
            "previous": [],
            "posterior": [],
        }
        result = _select_scenes(
            buckets, strategy="same_day", max_per_date=1, max_cloud_cover=10
        )
        assert result == []

    def test_previous_cloud_filter_applied_to_fallback(self):
        buckets = {
            "same_day": [],
            "previous": [
                {"id": "HIGH", "delta_days": 1, "cloud_cover": 30, "datetime": ""}
            ],
            "posterior": [],
        }
        result = _select_scenes(
            buckets, strategy="previous", max_per_date=1, max_cloud_cover=10
        )
        assert result == []

    def test_posterior_cloud_filter_applied_to_fallback(self):
        buckets = {
            "same_day": [],
            "previous": [],
            "posterior": [
                {"id": "HIGH", "delta_days": 1, "cloud_cover": 30, "datetime": ""}
            ],
        }
        result = _select_scenes(
            buckets, strategy="posterior", max_per_date=1, max_cloud_cover=10
        )
        assert result == []

    def test_flat_list_best_returns_first(self):
        images = [
            {"id": "A", "delta_days": 0, "cloud_cover": 5},
            {"id": "B", "delta_days": 1, "cloud_cover": 3},
        ]
        result = _select_scenes(images, strategy="best", max_per_date=1)
        assert len(result) == 1
        assert result[0]["id"] == "A"

    def test_flat_list_all_returns_everything(self):
        images = [{"id": "A"}, {"id": "B"}, {"id": "C"}]
        result = _select_scenes(images, strategy="all")
        assert len(result) == 3

    def test_flat_list_cloud_filter_applied(self):
        images = [
            {"id": "HIGH", "cloud_cover": 30},
            {"id": "LOW", "cloud_cover": 5},
        ]
        result = _select_scenes(images, strategy="all", max_cloud_cover=10)
        assert len(result) == 1
        assert result[0]["id"] == "LOW"


# ---------------------------------------------------------------------------
# create_bbox_from_point
# ---------------------------------------------------------------------------


class TestCreateBboxFromPoint:
    """Tests for create_bbox_from_point."""

    def test_returns_bbox_instance(self):
        bbox = create_bbox_from_point(lon=-56.5, lat=-32.85)
        assert isinstance(bbox, BBox)

    def test_crs_is_wgs84(self):
        bbox = create_bbox_from_point(lon=-56.5, lat=-32.85)
        assert bbox.crs == CRS.WGS84

    def test_default_buffer_expands_correctly(self):
        lon, lat, buffer = -56.5, -32.85, 0.01
        bbox = create_bbox_from_point(lon=lon, lat=lat, buffer_degrees=buffer)
        min_lon, min_lat, max_lon, max_lat = list(bbox)
        assert min_lon == pytest.approx(lon - buffer)
        assert min_lat == pytest.approx(lat - buffer)
        assert max_lon == pytest.approx(lon + buffer)
        assert max_lat == pytest.approx(lat + buffer)

    def test_custom_buffer(self):
        lon, lat, buffer = -56.5, -32.85, 0.05
        bbox = create_bbox_from_point(lon=lon, lat=lat, buffer_degrees=buffer)
        min_lon, min_lat, max_lon, max_lat = list(bbox)
        assert min_lon == pytest.approx(lon - buffer)
        assert max_lon == pytest.approx(lon + buffer)


# ---------------------------------------------------------------------------
# search_images
# ---------------------------------------------------------------------------


class TestSearchImages:
    """Tests for search_images."""

    def _make_fake_l1c_item(self, date="2025-08-01", cloud=5):
        return {
            "id": f"S2A_{date.replace('-', '')}T101031",
            "properties": {
                "datetime": f"{date}T10:10:31.000Z",
                "eo:cloud_cover": cloud,
            },
            "assets": {
                "data": {
                    "href": "https://eodata.dataspace.copernicus.eu/eodata/fake/path"
                }
            },
        }

    def _make_fake_l2a_item(self):
        mock_item = MagicMock()
        mock_scl = MagicMock()
        mock_scl.href = "https://fake-l2a-link.com/SCL.tif"
        mock_item.assets = {"scl": mock_scl}
        return mock_item

    def test_returns_list(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        with patch("aquamatch.sentinel_data.catalog") as mock_catalog, patch(
            "aquamatch.sentinel_data.client"
        ) as mock_client:
            mock_catalog.search.return_value = iter([self._make_fake_l1c_item()])
            mock_search = MagicMock()
            mock_search.items.return_value = [self._make_fake_l2a_item()]
            mock_client.search.return_value = mock_search

            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert isinstance(result, list)

    def test_returns_correct_keys(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        with patch("aquamatch.sentinel_data.catalog") as mock_catalog, patch(
            "aquamatch.sentinel_data.client"
        ) as mock_client:
            mock_catalog.search.return_value = iter([self._make_fake_l1c_item()])
            mock_search = MagicMock()
            mock_search.items.return_value = [self._make_fake_l2a_item()]
            mock_client.search.return_value = mock_search

            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert len(result) == 1
            for key in [
                "id",
                "datetime",
                "cloud_cover",
                "href",
                "delta_days",
                "l2a_scl",
            ]:
                assert key in result[0]

    def test_delta_days_computed_correctly(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        field_date = "2025-08-01"
        acquisition_date = "2025-08-02"

        with patch("aquamatch.sentinel_data.catalog") as mock_catalog, patch(
            "aquamatch.sentinel_data.client"
        ) as mock_client:
            mock_catalog.search.return_value = iter(
                [self._make_fake_l1c_item(date=acquisition_date)]
            )
            mock_search = MagicMock()
            mock_search.items.return_value = [self._make_fake_l2a_item()]
            mock_client.search.return_value = mock_search

            result = search_images(bbox, field_date, time_delta=2, cloud_cover=10)
            assert result[0]["delta_days"] == 1

    def test_returns_empty_when_no_l1c_found(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        with patch("aquamatch.sentinel_data.catalog") as mock_catalog:
            mock_catalog.search.return_value = iter([])
            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert result == []

    def test_l2a_scl_is_none_when_no_l2a_found(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        with patch("aquamatch.sentinel_data.catalog") as mock_catalog, patch(
            "aquamatch.sentinel_data.client"
        ) as mock_client:
            mock_catalog.search.return_value = iter([self._make_fake_l1c_item()])
            mock_search = MagicMock()
            mock_search.items.return_value = []
            mock_client.search.return_value = mock_search

            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert result[0]["l2a_scl"] == []


# ---------------------------------------------------------------------------
# build_catalog
# ---------------------------------------------------------------------------


class TestBuildCatalog:
    """Tests for build_catalog — bucketed images_found schema."""

    def _make_csv(self, tmp_path) -> Path:
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {
                "date": ["2025-08-01", "2025-08-02"],
                "longitud": [-56.5, -56.5],
                "latitud": [-32.85, -32.85],
            }
        ).to_csv(csv_file, index=False)
        return csv_file

    def _fake_image(self, date="2025-08-01", cloud=5):
        return {
            "id": f"S2A_{date.replace('-','')}T101031",
            "datetime": f"{date}T10:10:31.000Z",
            "cloud_cover": cloud,
            "href": "https://fake-link.com/product",
            "delta_days": 0,
            "l2a_scl": "https://fake-link.com/SCL.tif",
        }

    # --- Output file ---

    def test_creates_json_output(self, tmp_path):
        csv_file = self._make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[self._fake_image()],
        ):
            build_catalog(csv_file, output_json)
        assert output_json.exists()

    # --- Top-level schema ---

    def test_output_is_list(self, tmp_path):
        csv_file = self._make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[self._fake_image()],
        ):
            build_catalog(csv_file, output_json)
        data = json.loads(output_json.read_text())
        assert isinstance(data, list)

    def test_each_entry_has_field_date_and_images_found(self, tmp_path):
        csv_file = self._make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[self._fake_image()],
        ):
            build_catalog(csv_file, output_json)
        data = json.loads(output_json.read_text())
        for entry in data:
            assert "field_date" in entry
            assert "images_found" in entry

    # --- images_found is now a dict with three buckets ---

    def test_images_found_is_dict(self, tmp_path):
        csv_file = self._make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[self._fake_image()],
        ):
            build_catalog(csv_file, output_json)
        data = json.loads(output_json.read_text())
        assert isinstance(data[0]["images_found"], dict)

    def test_images_found_has_three_bucket_keys(self, tmp_path):
        csv_file = self._make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[self._fake_image()],
        ):
            build_catalog(csv_file, output_json)
        data = json.loads(output_json.read_text())
        assert set(data[0]["images_found"].keys()) == {
            "same_day",
            "previous",
            "posterior",
        }

    # --- Correct bucketing ---

    def test_same_day_image_goes_into_same_day_bucket(self, tmp_path):
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {"date": ["2025-08-01"], "longitud": [-56.5], "latitud": [-32.85]}
        ).to_csv(csv_file, index=False)
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[self._fake_image(date="2025-08-01")],
        ):
            build_catalog(csv_file, output_json)

        data = json.loads(output_json.read_text())
        assert len(data[0]["images_found"]["same_day"]) == 1
        assert len(data[0]["images_found"]["previous"]) == 0
        assert len(data[0]["images_found"]["posterior"]) == 0

    def test_earlier_image_goes_into_previous_bucket(self, tmp_path):
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {"date": ["2025-08-01"], "longitud": [-56.5], "latitud": [-32.85]}
        ).to_csv(csv_file, index=False)
        output_json = tmp_path / "catalog.json"

        img = {**self._fake_image(date="2025-07-30"), "delta_days": 2}
        with patch("aquamatch.sentinel_data.search_images", return_value=[img]):
            build_catalog(csv_file, output_json)

        data = json.loads(output_json.read_text())
        assert len(data[0]["images_found"]["previous"]) == 1
        assert len(data[0]["images_found"]["same_day"]) == 0
        assert len(data[0]["images_found"]["posterior"]) == 0

    def test_later_image_goes_into_posterior_bucket(self, tmp_path):
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {"date": ["2025-08-01"], "longitud": [-56.5], "latitud": [-32.85]}
        ).to_csv(csv_file, index=False)
        output_json = tmp_path / "catalog.json"

        img = {**self._fake_image(date="2025-08-03"), "delta_days": 2}
        with patch("aquamatch.sentinel_data.search_images", return_value=[img]):
            build_catalog(csv_file, output_json)

        data = json.loads(output_json.read_text())
        assert len(data[0]["images_found"]["posterior"]) == 1
        assert len(data[0]["images_found"]["same_day"]) == 0
        assert len(data[0]["images_found"]["previous"]) == 0

    def test_mixed_images_distributed_across_buckets(self, tmp_path):
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {"date": ["2025-08-01"], "longitud": [-56.5], "latitud": [-32.85]}
        ).to_csv(csv_file, index=False)
        output_json = tmp_path / "catalog.json"

        images = [
            {**self._fake_image(date="2025-08-01"), "id": "SAME", "delta_days": 0},
            {**self._fake_image(date="2025-07-31"), "id": "PREV", "delta_days": 1},
            {**self._fake_image(date="2025-08-02"), "id": "POST", "delta_days": 1},
        ]
        with patch("aquamatch.sentinel_data.search_images", return_value=images):
            build_catalog(csv_file, output_json)

        data = json.loads(output_json.read_text())
        buckets = data[0]["images_found"]
        assert len(buckets["same_day"]) == 1
        assert len(buckets["previous"]) == 1
        assert len(buckets["posterior"]) == 1

    # --- Sorting within buckets ---

    def test_bucket_sorted_by_delta_days_then_cloud_cover(self, tmp_path):
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {"date": ["2025-08-01"], "longitud": [-56.5], "latitud": [-32.85]}
        ).to_csv(csv_file, index=False)
        output_json = tmp_path / "catalog.json"

        images = [
            {
                **self._fake_image(date="2025-07-30"),
                "id": "P2_HIGH",
                "delta_days": 2,
                "cloud_cover": 20,
            },
            {
                **self._fake_image(date="2025-07-31"),
                "id": "P1_HIGH",
                "delta_days": 1,
                "cloud_cover": 15,
            },
            {
                **self._fake_image(date="2025-07-31"),
                "id": "P1_LOW",
                "delta_days": 1,
                "cloud_cover": 5,
            },
        ]
        with patch("aquamatch.sentinel_data.search_images", return_value=images):
            build_catalog(csv_file, output_json)

        data = json.loads(output_json.read_text())
        previous = data[0]["images_found"]["previous"]
        assert len(previous) == 3
        assert previous[0]["id"] == "P1_LOW"  # delta=1, cloud=5
        assert previous[1]["id"] == "P1_HIGH"  # delta=1, cloud=15
        assert previous[2]["id"] == "P2_HIGH"  # delta=2, cloud=20

    # --- Empty results ---

    def test_no_images_found_produces_empty_buckets(self, tmp_path):
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {"date": ["2025-08-01"], "longitud": [-56.5], "latitud": [-32.85]}
        ).to_csv(csv_file, index=False)
        output_json = tmp_path / "catalog.json"

        with patch("aquamatch.sentinel_data.search_images", return_value=[]):
            build_catalog(csv_file, output_json)

        data = json.loads(output_json.read_text())
        assert data == []

    # --- Deduplication ---

    def test_deduplicates_same_scene_across_stations(self, tmp_path):
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {
                "date": ["2025-08-01", "2025-08-01"],
                "longitud": [-56.5, -56.6],
                "latitud": [-32.85, -32.90],
            }
        ).to_csv(csv_file, index=False, sep=";")
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[self._fake_image()],
        ):
            build_catalog(csv_file, output_json)

        data = json.loads(output_json.read_text())
        assert len(data) == 1
        total = sum(len(v) for v in data[0]["images_found"].values())
        assert total == 1

    # --- Validation ---

    def test_raises_on_missing_date_column(self, tmp_path):
        csv_file = tmp_path / "bad.csv"
        pd.DataFrame({"longitud": [-56.5], "latitud": [-32.85]}).to_csv(
            csv_file, index=False, sep=";"
        )
        with pytest.raises(ValueError, match="date"):
            build_catalog(csv_file, tmp_path / "out.json")

    def test_raises_on_missing_coordinate_columns(self, tmp_path):
        csv_file = tmp_path / "bad.csv"
        pd.DataFrame({"date": ["2025-08-01"], "code": 42}).to_csv(
            csv_file, index=False, sep=","
        )
        with pytest.raises(ValueError, match="longitud"):
            build_catalog(csv_file, tmp_path / "out.json")

    def test_reads_comma_separated_csv(self, tmp_path):
        csv_file = tmp_path / "campaigns_comma.csv"
        pd.DataFrame(
            {"date": ["2025-08-01"], "longitud": [-56.5], "latitud": [-32.85]}
        ).to_csv(csv_file, index=False)
        output_json = tmp_path / "catalog.json"
        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[self._fake_image()],
        ):
            build_catalog(csv_file, output_json)
        assert output_json.exists()

    # --- Image keys preserved ---

    def test_image_keys_preserved_in_bucket(self, tmp_path):
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {"date": ["2025-08-01"], "longitud": [-56.5], "latitud": [-32.85]}
        ).to_csv(csv_file, index=False)
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[self._fake_image()],
        ):
            build_catalog(csv_file, output_json)

        data = json.loads(output_json.read_text())
        img = data[0]["images_found"]["same_day"][0]
        for key in ("id", "datetime", "cloud_cover", "href", "delta_days", "l2a_scl"):
            assert key in img

    def test_internal_field_date_key_not_in_output(self, tmp_path):
        """The internal _field_date helper key must not appear in the JSON output."""
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {"date": ["2025-08-01"], "longitud": [-56.5], "latitud": [-32.85]}
        ).to_csv(csv_file, index=False)
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[self._fake_image()],
        ):
            build_catalog(csv_file, output_json)

        data = json.loads(output_json.read_text())
        img = data[0]["images_found"]["same_day"][0]
        assert "_field_date" not in img


# ---------------------------------------------------------------------------
# run_download — backward compatibility with bucketed schema
# ---------------------------------------------------------------------------


class TestRunDownloadBucketedSchema:
    """run_download must handle the new bucketed images_found dict."""

    def _make_bucketed_catalog(self, tmp_path, bucket="same_day") -> Path:
        img = {
            "id": "IMG1",
            "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG1/path",
            "l2a_scl": "https://fake.com/IMG1_SCL.tif",
            "delta_days": 0,
            "cloud_cover": 5,
            "datetime": "2024-03-15T13:51:11Z",
        }
        catalog_data = [
            {
                "field_date": "2024-03-15",
                "images_found": {
                    "same_day": [img] if bucket == "same_day" else [],
                    "previous": [img] if bucket == "previous" else [],
                    "posterior": [img] if bucket == "posterior" else [],
                },
            }
        ]
        path = tmp_path / "catalog.json"
        path.write_text(json.dumps(catalog_data))
        return path

    def test_processes_same_day_image(self, tmp_path):
        catalog = self._make_bucketed_catalog(tmp_path, bucket="same_day")
        with patch("aquamatch.sentinel_data.download_product"), patch(
            "aquamatch.sentinel_data.download_scl_asset"
        ), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):
            stats = run_download(
                catalog, tmp_path, strategy="best", max_per_date=1, download_scl=False
            )
        assert stats["total_processed"] == 1

    def test_processes_previous_image_when_no_same_day(self, tmp_path):
        catalog = self._make_bucketed_catalog(tmp_path, bucket="previous")
        with patch("aquamatch.sentinel_data.download_product"), patch(
            "aquamatch.sentinel_data.download_scl_asset"
        ), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):
            stats = run_download(
                catalog, tmp_path, strategy="best", max_per_date=1, download_scl=False
            )
        assert stats["total_processed"] == 1

    def test_empty_buckets_skips_date(self, tmp_path):
        catalog_data = [
            {
                "field_date": "2024-03-15",
                "images_found": {"same_day": [], "previous": [], "posterior": []},
            }
        ]
        catalog = tmp_path / "catalog.json"
        catalog.write_text(json.dumps(catalog_data))
        with patch("aquamatch.sentinel_data.download_product") as mock_dl:
            run_download(
                catalog, tmp_path, strategy="best", max_per_date=1, download_scl=False
            )
        mock_dl.assert_not_called()

    def test_strategy_best_downloads_one_image_across_all_buckets(self, tmp_path):
        img = {
            "id": "IMG1",
            "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG1/path",
            "l2a_scl": "https://fake.com/IMG1_SCL.tif",
            "delta_days": 0,
            "cloud_cover": 5,
            "datetime": "2024-03-15T13:51:11Z",
        }
        catalog_data = [
            {
                "field_date": "2024-03-15",
                "images_found": {
                    "same_day": [img],
                    "previous": [{**img, "id": "IMG2"}],
                    "posterior": [{**img, "id": "IMG3"}],
                },
            }
        ]
        catalog = tmp_path / "catalog.json"
        catalog.write_text(json.dumps(catalog_data))

        with patch("aquamatch.sentinel_data.download_product") as mock_dl, patch(
            "aquamatch.sentinel_data.download_scl_asset"
        ), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):
            run_download(
                catalog, tmp_path, strategy="best", max_per_date=1, download_scl=False
            )
        assert mock_dl.call_count == 1

    def test_strategy_all_downloads_all_images(self, tmp_path):
        img = {
            "id": "IMG1",
            "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG1/path",
            "l2a_scl": None,
            "delta_days": 0,
            "cloud_cover": 5,
            "datetime": "2024-03-15T13:51:11Z",
        }
        catalog_data = [
            {
                "field_date": "2024-03-15",
                "images_found": {
                    "same_day": [img],
                    "previous": [{**img, "id": "IMG2"}],
                    "posterior": [{**img, "id": "IMG3"}],
                },
            }
        ]
        catalog = tmp_path / "catalog.json"
        catalog.write_text(json.dumps(catalog_data))

        with patch("aquamatch.sentinel_data.download_product") as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):
            run_download(catalog, tmp_path, strategy="all", download_scl=False)
        assert mock_dl.call_count == 3

    def test_backward_compat_with_flat_list_catalog(self, tmp_path):
        """Old-style flat list catalogs must still be processed without error."""
        catalog_data = [
            {
                "field_date": "2024-03-15",
                "images_found": [
                    {
                        "id": "IMG1",
                        "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG1/path",
                        "l2a_scl": None,
                        "delta_days": 0,
                        "cloud_cover": 5,
                        "datetime": "2024-03-15T13:51:11Z",
                    }
                ],
            }
        ]
        catalog = tmp_path / "catalog.json"
        catalog.write_text(json.dumps(catalog_data))

        with patch("aquamatch.sentinel_data.download_product") as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):
            stats = run_download(
                catalog, tmp_path, strategy="best", max_per_date=1, download_scl=False
            )
        assert mock_dl.call_count == 1


# ---------------------------------------------------------------------------
# get_scl_path
# ---------------------------------------------------------------------------


class TestGetSclPath:
    """Tests for the get_scl_path helper."""

    def test_returns_path_under_scl_subdir(self, tmp_path):
        path = get_scl_path("S2A_MSIL1C_20250801", tmp_path)
        assert path.parent == tmp_path / SCL_SUBDIR
        assert path.name == "S2A_MSIL1C_20250801_SCL.tif"

    def test_strips_safe_extension(self, tmp_path):
        path = get_scl_path("S2A_MSIL1C_20250801.SAFE", tmp_path)
        assert path.name == "S2A_MSIL1C_20250801_SCL.tif"

    def test_consistent_with_download_scl_asset(self, tmp_path):
        """get_scl_path and download_scl_asset must agree on the file location."""
        product_id = "S2A_MSIL1C_20250801.SAFE"
        product_core_id = product_id.split(".")[0]
        expected = get_scl_path(product_id, tmp_path)

        scl_dir = tmp_path / SCL_SUBDIR
        scl_dir.mkdir()
        actual = scl_dir / f"{product_core_id}_SCL.tif"
        actual.write_bytes(b"fake")

        assert expected == actual
        assert expected.exists()


# ---------------------------------------------------------------------------
# get_download_status
# ---------------------------------------------------------------------------


class TestGetDownloadStatus:
    """Tests for get_download_status."""

    def test_safe_folder_exists_and_not_empty(self, tmp_path):
        product_id = "S2A_MSIL1C_20250801"
        safe_folder = tmp_path / product_id
        safe_folder.mkdir()
        (safe_folder / "dummy.xml").write_text("x")

        status = get_download_status(product_id, tmp_path, download_scl=False)
        assert status["safe_exists"] is True
        assert status["all_downloaded"] is True

    def test_safe_not_downloaded(self, tmp_path):
        status = get_download_status(
            "S2A_MSIL1C_20250801", tmp_path, download_scl=False
        )
        assert status["safe_exists"] is False
        assert status["all_downloaded"] is False

    def test_scl_check_when_required(self, tmp_path):
        product_id = "S2A_MSIL1C_20250801.SAFE"
        product_core_id = product_id.split(".")[0]

        safe_file = tmp_path / product_id
        safe_file.mkdir()
        (safe_file / "dummy.xml").write_text("x")

        scl_dir = tmp_path / SCL_SUBDIR
        scl_dir.mkdir()
        (scl_dir / f"{product_core_id}_SCL.tif").write_bytes(b"fake")

        status = get_download_status(product_id, tmp_path, download_scl=True)
        assert status["scl_exists"] is True
        assert status["all_downloaded"] is True

    def test_all_downloaded_false_when_scl_missing(self, tmp_path):
        product_id = "S2A_MSIL1C_20250801"
        safe_folder = tmp_path / product_id
        safe_folder.mkdir()
        (safe_folder / "dummy.xml").write_text("x")

        status = get_download_status(product_id, tmp_path, download_scl=True)
        assert status["safe_exists"] is True
        assert status["scl_exists"] is False
        assert status["all_downloaded"] is False

    def test_scl_not_found_in_old_flat_location(self, tmp_path):
        product_id = "S2A_MSIL1C_20250801"
        safe_folder = tmp_path / product_id
        safe_folder.mkdir()
        (safe_folder / "dummy.xml").write_text("x")

        (tmp_path / f"{product_id}_SCL.tif").write_bytes(b"old")

        status = get_download_status(product_id, tmp_path, download_scl=True)
        assert status["scl_exists"] is False


# ---------------------------------------------------------------------------
# run_download (original tests, adapted for bucketed schema)
# ---------------------------------------------------------------------------


class TestRunDownload:
    """Tests for run_download."""

    def _make_catalog(self, tmp_path) -> Path:
        img1 = {
            "id": "IMG1",
            "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG1/path",
            "l2a_scl": "https://fake.com/IMG1_SCL.tif",
            "delta_days": 0,
            "cloud_cover": 5,
            "datetime": "2024-03-15T13:51:11Z",
        }
        img2 = {
            "id": "IMG2",
            "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG2/path",
            "l2a_scl": "https://fake.com/IMG2_SCL.tif",
            "delta_days": 0,
            "cloud_cover": 3,
            "datetime": "2024-03-16T13:51:11Z",
        }
        img3 = {
            "id": "IMG3",
            "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG3/path",
            "l2a_scl": "https://fake.com/IMG3_SCL.tif",
            "delta_days": 1,
            "cloud_cover": 8,
            "datetime": "2024-03-17T13:51:11Z",
        }
        catalog_data = [
            {
                "field_date": "2024-03-15",
                "images_found": {
                    "same_day": [img1],
                    "previous": [],
                    "posterior": [],
                },
            },
            {
                "field_date": "2024-03-16",
                "images_found": {
                    "same_day": [img2],
                    "previous": [],
                    "posterior": [img3],
                },
            },
        ]
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text(json.dumps(catalog_data))
        return catalog_json

    def test_strategy_best_downloads_one_per_date(self, tmp_path):
        catalog_json = self._make_catalog(tmp_path)
        with patch("aquamatch.sentinel_data.download_product") as mock_dl, patch(
            "aquamatch.sentinel_data.download_scl_asset"
        ), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):
            run_download(
                catalog_json,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
            )
            assert mock_dl.call_count == 2

    def test_strategy_all_downloads_all_images_in_catalog(self, tmp_path):
        catalog_json = self._make_catalog(tmp_path)
        with patch("aquamatch.sentinel_data.download_product") as mock_dl, patch(
            "aquamatch.sentinel_data.download_scl_asset"
        ), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):
            run_download(catalog_json, tmp_path, strategy="all", download_scl=False)
            assert mock_dl.call_count == 3

    def test_skips_already_downloaded(self, tmp_path):
        catalog_json = self._make_catalog(tmp_path)
        with patch("aquamatch.sentinel_data.download_product") as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value={
                "safe_exists": True,
                "scl_exists": True,
                "all_downloaded": True,
            },
        ):
            run_download(
                catalog_json,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=True,
            )
            mock_dl.assert_not_called()
