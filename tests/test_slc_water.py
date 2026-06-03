"""
Unit tests for scl_water.py — Feature 1: scl_water_to_geojson.

Synthetic SCL rasters are written to tmp_path using rasterio so tests
have no dependency on real satellite data.  All rasters use a small
UTM-projected CRS (EPSG:32721, Uruguay/Argentina zone) so that area
calculations are in metres and behave predictably.
"""

import json
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds
import geopandas as gpd

from rionegromatchup.scl_water import (
    scl_water_to_geojson,
    _parse_date_from_filename,
    SCL_WATER_CLASS,
    GEOJSON_SUBDIR,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# UTM zone 21S — covers Uruguay / NE Argentina where the project operates
TEST_CRS = "EPSG:32721"

# A 100 × 100 m bounding box in UTM 21S (roughly near Río Negro basin)
WEST, SOUTH, EAST, NORTH = 500_000.0, 6_350_000.0, 500_300.0, 6_350_300.0


def _make_scl_raster(
    tmp_path: Path,
    water_rows: slice,
    water_cols: slice,
    name: str = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif",
    rows: int = 30,
    cols: int = 30,
) -> Path:
    """
    Write a synthetic SCL GeoTIFF to tmp_path.

    The raster is rows × cols pixels covering the TEST bounding box.
    Pixels selected by water_rows / water_cols are set to SCL_WATER_CLASS (6);
    all others are set to 4 (vegetation).
    """
    data = np.full((rows, cols), 4, dtype=np.uint8)
    data[water_rows, water_cols] = SCL_WATER_CLASS

    transform = from_bounds(WEST, SOUTH, EAST, NORTH, cols, rows)
    scl_path = tmp_path / name

    with rasterio.open(
        scl_path,
        "w",
        driver="GTiff",
        height=rows,
        width=cols,
        count=1,
        dtype=np.uint8,
        crs=TEST_CRS,
        transform=transform,
    ) as dst:
        dst.write(data, 1)

    return scl_path


# ---------------------------------------------------------------------------
# _parse_date_from_filename
# ---------------------------------------------------------------------------

class TestParseDateFromFilename:
    """Tests for the internal date-parsing helper."""

    def test_compact_format(self):
        name = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_20230919T094731.SAFE"
        assert _parse_date_from_filename(name) == "2025-08-01"

    def test_separated_format(self):
        name = "S2A_MSI_2017_07_13_14_01_45_000000_T21HUD_L2W.nc"
        assert _parse_date_from_filename(name) == "2017-07-13"

    def test_unrecognised_returns_none(self):
        assert _parse_date_from_filename("unexpected_filename.tif") is None

    def test_scl_filename_compact(self):
        name = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif"
        assert _parse_date_from_filename(name) == "2025-08-01"


# ---------------------------------------------------------------------------
# scl_water_to_geojson
# ---------------------------------------------------------------------------

class TestSclWaterToGeojson:
    """Tests for scl_water_to_geojson."""

    # --- Basic output ---

    def test_returns_path(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        result = scl_water_to_geojson(scl, out)
        assert isinstance(result, Path)
        assert result == out

    def test_output_file_exists(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out)
        assert out.exists()

    def test_output_is_valid_geojson_feature_collection(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out)
        gdf = gpd.read_file(out)
        assert len(gdf) == 1

    def test_output_crs_is_wgs84(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out)
        gdf = gpd.read_file(out)
        assert gdf.crs.to_epsg() == 4326

    def test_geometry_is_multipolygon(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out)
        gdf = gpd.read_file(out)
        assert gdf.geometry.iloc[0].geom_type == "MultiPolygon"

    # --- Properties ---

    def test_properties_scene_id_default(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out)
        gdf = gpd.read_file(out)
        assert gdf.iloc[0]["scene_id"] == scl.stem

    def test_properties_scene_id_custom(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out, scene_id="MY_SCENE")
        gdf = gpd.read_file(out)
        assert gdf.iloc[0]["scene_id"] == "MY_SCENE"

    def test_properties_date_auto_detected(self, tmp_path):
        scl = _make_scl_raster(
            tmp_path,
            slice(5, 25), slice(5, 25),
            name="S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif",
        )
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out)
        gdf = gpd.read_file(out)
        assert str(gdf.iloc[0]["date"])[:10] == "2025-08-01"

    def test_properties_date_custom(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out, scene_date="2017-07-13")
        gdf = gpd.read_file(out)
        assert str(gdf.iloc[0]["date"])[:10] == "2017-07-13"

    def test_properties_scl_source(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out)
        gdf = gpd.read_file(out)
        assert gdf.iloc[0]["scl_source"] == str(scl)

    def test_properties_n_water_px(self, tmp_path):
        # 20×20 block of water pixels = 400
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out, min_area_m2=0)
        gdf = gpd.read_file(out)
        assert gdf.iloc[0]["n_water_px"] == 400

    # --- Error conditions ---

    def test_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="SCL file not found"):
            scl_water_to_geojson(
                tmp_path / "nonexistent.tif",
                tmp_path / "out.geojson",
            )

    def test_raises_value_error_no_water_pixels(self, tmp_path):
        """Raster with no class-6 pixels must raise ValueError."""
        scl = _make_scl_raster(
            tmp_path,
            water_rows=slice(0, 0),  # empty slice — no water pixels
            water_cols=slice(0, 0),
        )
        with pytest.raises(ValueError, match="No water pixels"):
            scl_water_to_geojson(scl, tmp_path / "out.geojson")

    def test_raises_value_error_all_filtered_by_area(self, tmp_path):
        """Single tiny water pixel should be removed by a large area filter."""
        scl = _make_scl_raster(tmp_path, slice(0, 1), slice(0, 1))  # 1 pixel
        with pytest.raises(ValueError, match="area filter"):
            scl_water_to_geojson(
                scl,
                tmp_path / "out.geojson",
                min_area_m2=1_000_000,  # 1 km² — far larger than 1 pixel
            )

    def test_area_filter_error_message_includes_pixel_count(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(0, 1), slice(0, 1))
        with pytest.raises(ValueError, match="n_water_px=1"):
            scl_water_to_geojson(
                scl,
                tmp_path / "out.geojson",
                min_area_m2=1_000_000,
            )

    # --- Idempotency ---

    def test_skips_existing_file_when_overwrite_false(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out)
        mtime_first = out.stat().st_mtime

        # Second call — must not reprocess
        scl_water_to_geojson(scl, out, overwrite=False)
        assert out.stat().st_mtime == mtime_first

    def test_overwrites_when_overwrite_true(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out)
        mtime_first = out.stat().st_mtime

        import time
        time.sleep(0.05)  # ensure filesystem timestamp changes

        scl_water_to_geojson(scl, out, overwrite=True)
        assert out.stat().st_mtime > mtime_first

    # --- Area filter ---

    def test_area_filter_removes_small_polygons(self, tmp_path):
        """Two disconnected water blobs: one large, one tiny.
        The tiny one should be removed by the area filter."""
        data = np.full((30, 30), 4, dtype=np.uint8)
        data[2:22, 2:22] = SCL_WATER_CLASS   # large block ~stays
        data[0:1, 28:29] = SCL_WATER_CLASS   # 1-pixel blob ~removed

        transform = from_bounds(WEST, SOUTH, EAST, NORTH, 30, 30)
        scl = tmp_path / "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif"
        with rasterio.open(
            scl, "w", driver="GTiff", height=30, width=30,
            count=1, dtype=np.uint8, crs=TEST_CRS, transform=transform,
        ) as dst:
            dst.write(data, 1)

        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        # min_area_m2 large enough to drop the 1-pixel blob but keep the block
        scl_water_to_geojson(scl, out, min_area_m2=500)
        gdf = gpd.read_file(out)
        # Result is a single merged MultiPolygon — no tiny blobs survived
        assert len(gdf) == 1
        assert gdf.geometry.iloc[0].geom_type == "MultiPolygon"

    # --- Simplification and buffer (smoke tests) ---

    def test_simplification_does_not_raise(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out, simplify_tolerance=50)
        assert out.exists()

    def test_buffer_does_not_raise(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        out = tmp_path / GEOJSON_SUBDIR / "water.geojson"
        scl_water_to_geojson(scl, out, buffer_m=30)
        assert out.exists()

    def test_buffer_expands_geometry(self, tmp_path):
        """A positive buffer should increase the bounding-box area."""
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))

        out_no_buf = tmp_path / GEOJSON_SUBDIR / "no_buffer.geojson"
        out_buf = tmp_path / GEOJSON_SUBDIR / "with_buffer.geojson"

        scl_water_to_geojson(scl, out_no_buf, buffer_m=0)
        scl_water_to_geojson(scl, out_buf, buffer_m=60)

        gdf_no = gpd.read_file(out_no_buf).to_crs("EPSG:32721")
        gdf_buf = gpd.read_file(out_buf).to_crs("EPSG:32721")

        assert gdf_buf.geometry.area.iloc[0] > gdf_no.geometry.area.iloc[0]

    # --- Output directory creation ---

    def test_creates_output_parent_directory(self, tmp_path):
        scl = _make_scl_raster(tmp_path, slice(5, 25), slice(5, 25))
        nested = tmp_path / "a" / "b" / "c" / "water.geojson"
        scl_water_to_geojson(scl, nested)
        assert nested.exists()