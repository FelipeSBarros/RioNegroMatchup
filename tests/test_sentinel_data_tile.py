"""
Tests for _tile_from_scene_id and the tile-filtering logic in build_catalog.
Updated to reflect the bucketed images_found schema.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from aquamatch.sentinel_data import _tile_from_scene_id, build_catalog

# ---------------------------------------------------------------------------
# _tile_from_scene_id
# ---------------------------------------------------------------------------


class TestTileFromSceneId:

    def test_extracts_tile_from_l1c_scene_id(self):
        scene_id = "S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD_20230919T094731"
        assert _tile_from_scene_id(scene_id) == "21HUD"

    def test_extracts_tile_from_scene_id_with_safe_suffix(self):
        scene_id = "S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD.SAFE"
        assert _tile_from_scene_id(scene_id) == "21HUD"

    def test_extracts_tile_from_scl_href(self):
        href = (
            "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a"
            "/items/S2A_MSIL2A_20170713T135111_N0500_R024_T21HUD_20230919T094731"
        )
        assert _tile_from_scene_id(href) == "21HUD"

    def test_returns_none_for_unrecognised_string(self):
        assert _tile_from_scene_id("unexpected_filename.tif") is None

    def test_returns_none_for_empty_string(self):
        assert _tile_from_scene_id("") is None

    def test_different_tile_codes(self):
        assert (
            _tile_from_scene_id("S2B_MSIL1C_20200101T000000_N0400_R000_T33UXP_x")
            == "33UXP"
        )


# ---------------------------------------------------------------------------
# Helpers shared across tile filter tests
# ---------------------------------------------------------------------------


def _make_unique_csv(tmp_path, s2_tile="21HUD") -> Path:
    csv_file = tmp_path / "campaigns_unique_data.csv"
    pd.DataFrame(
        {
            "date": ["2025-08-01"],
            "longitud": [-56.5],
            "latitud": [-32.85],
            "s2_tile": [s2_tile],
        }
    ).to_csv(csv_file, index=False)
    return csv_file


def _make_csv_without_tile(tmp_path) -> Path:
    csv_file = tmp_path / "campaigns_no_tile.csv"
    pd.DataFrame(
        {
            "date": ["2025-08-01"],
            "longitud": [-56.5],
            "latitud": [-32.85],
        }
    ).to_csv(csv_file, index=False)
    return csv_file


def _fake_image(tile="21HUD", scl_tile="21HUD"):
    scene_id = f"S2A_MSIL1C_20250801T101031_N0500_R024_T{tile}_20230919T094731"
    zone, band, square = scl_tile[:2], scl_tile[2], scl_tile[3:]
    scl_href = (
        f"https://sentinel-cogs.s3.us-west-2.amazonaws.com/"
        f"sentinel-s2-l2a-cogs/{zone}/{band}/{square}/2025/8/fake/SCL.tif"
    )
    return {
        "id": scene_id,
        "datetime": "2025-08-01T10:10:31.000Z",
        "cloud_cover": 5,
        "href": "https://eodata.dataspace.copernicus.eu/eodata/fake/path",
        "delta_days": 0,
        "l2a_scl": [scl_href],
    }


def _all_images(data: list[dict]) -> list[dict]:
    """Flatten all buckets from a catalog entry into a single list."""
    images = []
    for entry in data:
        found = entry["images_found"]
        images.extend(found.get("same_day", []))
        images.extend(found.get("previous", []))
        images.extend(found.get("posterior", []))
    return images


# ---------------------------------------------------------------------------
# build_catalog — tile filtering
# ---------------------------------------------------------------------------


class TestBuildCatalogTileFilter:

    def test_matching_tile_is_included(self, tmp_path):
        csv = _make_unique_csv(tmp_path, s2_tile="21HUD")
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[_fake_image(tile="21HUD")],
        ):
            build_catalog(csv, output_json)

        data = json.loads(output_json.read_text())
        assert len(data) == 1
        assert len(_all_images(data)) == 1

    def test_mismatched_tile_is_discarded(self, tmp_path):
        csv = _make_unique_csv(tmp_path, s2_tile="21HUD")
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[_fake_image(tile="21HVD")],  # wrong tile
        ):
            build_catalog(csv, output_json)

        data = json.loads(output_json.read_text())
        assert len(data) == 0

    def test_mixed_tiles_only_matching_kept(self, tmp_path):
        csv = _make_unique_csv(tmp_path, s2_tile="21HUD")
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[
                _fake_image(tile="21HUD"),  # keep
                _fake_image(tile="21HVD"),  # discard
            ],
        ):
            build_catalog(csv, output_json)

        data = json.loads(output_json.read_text())
        images = _all_images(data)
        assert len(images) == 1
        assert "T21HUD" in images[0]["id"]

    def test_no_s2_tile_column_skips_filter(self, tmp_path):
        """When s2_tile is absent all scenes are kept and a warning is logged."""
        csv = _make_csv_without_tile(tmp_path)
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[_fake_image(tile="21HVD")],
        ):
            build_catalog(csv, output_json)

        data = json.loads(output_json.read_text())
        assert len(_all_images(data)) == 1

    def test_no_s2_tile_column_emits_warning(self, tmp_path, caplog):
        import logging

        csv = _make_csv_without_tile(tmp_path)
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[],
        ), caplog.at_level(logging.WARNING, logger="aquamatch.sentinel_data"):
            build_catalog(csv, output_json)

        assert any("s2_tile" in msg for msg in caplog.messages)

    def test_mismatched_scl_tile_set_to_none(self, tmp_path):
        """SCL href from a different tile must be nulled out."""
        csv = _make_unique_csv(tmp_path, s2_tile="21HUD")
        output_json = tmp_path / "catalog.json"

        img = _fake_image(tile="21HUD", scl_tile="21HVD")  # SCL tile mismatch
        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[img],
        ):
            build_catalog(csv, output_json)

        data = json.loads(output_json.read_text())
        images = _all_images(data)
        assert images[0]["l2a_scl"] is None

    def test_matching_scl_tile_preserved(self, tmp_path):
        """SCL href from the correct tile must be kept as-is."""
        csv = _make_unique_csv(tmp_path, s2_tile="21HUD")
        output_json = tmp_path / "catalog.json"

        img = _fake_image(tile="21HUD", scl_tile="21HUD")
        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[img],
        ):
            build_catalog(csv, output_json)

        data = json.loads(output_json.read_text())
        images = _all_images(data)
        assert images[0]["l2a_scl"] is not None

    # --- Bucket placement with tile filtering active ---

    def test_same_day_image_placed_in_same_day_bucket(self, tmp_path):
        csv = _make_unique_csv(tmp_path, s2_tile="21HUD")
        output_json = tmp_path / "catalog.json"

        img = _fake_image(
            tile="21HUD"
        )  # datetime is 2025-08-01, field_date is 2025-08-01
        with patch("aquamatch.sentinel_data.search_images", return_value=[img]):
            build_catalog(csv, output_json)

        data = json.loads(output_json.read_text())
        assert len(data[0]["images_found"]["same_day"]) == 1
        assert len(data[0]["images_found"]["previous"]) == 0
        assert len(data[0]["images_found"]["posterior"]) == 0
