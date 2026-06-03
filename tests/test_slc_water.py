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
            slice(5, 25),
            slice(5, 25),
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
        data[2:22, 2:22] = SCL_WATER_CLASS  # large block ~stays
        data[0:1, 28:29] = SCL_WATER_CLASS  # 1-pixel blob ~removed

        transform = from_bounds(WEST, SOUTH, EAST, NORTH, 30, 30)
        scl = tmp_path / "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif"
        with rasterio.open(
            scl,
            "w",
            driver="GTiff",
            height=30,
            width=30,
            count=1,
            dtype=np.uint8,
            crs=TEST_CRS,
            transform=transform,
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


# ---------------------------------------------------------------------------
# Update imports at module level — append new symbols
# (handled by appending tests that import directly where needed,
#  since the file-level import block is already set)
# ---------------------------------------------------------------------------

# Re-import with new symbols for Feature 2 tests
from rionegromatchup.scl_water import resolve_scl_path, build_water_polygon_datacube

# ---------------------------------------------------------------------------
# resolve_scl_path
# ---------------------------------------------------------------------------


class TestResolveScLPath:
    """Tests for resolve_scl_path."""

    SAFE_NAME = "S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD_20230919T094731.SAFE"
    STEM = "S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD_20230919T094731"

    def test_primary_resolution(self, tmp_path):
        """SCL file with matching stem is found via primary strategy."""
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        scl_file = scl_dir / f"{self.STEM}_SCL.tif"
        scl_file.write_bytes(b"fake")

        result = resolve_scl_path(tmp_path / self.SAFE_NAME, scl_dir)
        assert result == scl_file

    def test_fallback_timestamp_resolution(self, tmp_path):
        """SCL file with different baseline but same timestamp is found via fallback."""
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        # Different baseline suffix than the SAFE folder
        scl_file = (
            scl_dir
            / "S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD_20240101T000000_SCL.tif"
        )
        scl_file.write_bytes(b"fake")

        result = resolve_scl_path(tmp_path / self.SAFE_NAME, scl_dir)
        assert result == scl_file

    def test_returns_none_when_not_found(self, tmp_path):
        """Returns None when no matching SCL file exists."""
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()

        result = resolve_scl_path(tmp_path / self.SAFE_NAME, scl_dir)
        assert result is None

    def test_primary_takes_precedence_over_fallback(self, tmp_path):
        """When both primary and fallback exist, primary is returned."""
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        primary = scl_dir / f"{self.STEM}_SCL.tif"
        primary.write_bytes(b"primary")
        fallback = (
            scl_dir
            / "S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD_20240101T000000_SCL.tif"
        )
        fallback.write_bytes(b"fallback")

        result = resolve_scl_path(tmp_path / self.SAFE_NAME, scl_dir)
        assert result == primary

    def test_accepts_safe_path_without_safe_extension(self, tmp_path):
        """Works when the SAFE path has no .SAFE extension."""
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        scl_file = scl_dir / f"{self.STEM}_SCL.tif"
        scl_file.write_bytes(b"fake")

        result = resolve_scl_path(tmp_path / self.STEM, scl_dir)
        assert result == scl_file


# ---------------------------------------------------------------------------
# build_water_polygon_datacube
# ---------------------------------------------------------------------------


class TestBuildWaterPolygonDatacube:
    """Tests for build_water_polygon_datacube."""

    def _make_record(
        self,
        tmp_path: Path,
        name: str = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif",
        water_rows: slice = slice(5, 25),
        water_cols: slice = slice(5, 25),
    ) -> dict:
        """Create a synthetic SCL raster and return a datacube record for it."""
        scl = _make_scl_raster(tmp_path, water_rows, water_cols, name=name)
        return {"scl_path": scl}

    # --- Basic output ---

    def test_creates_gpkg(self, tmp_path):
        record = self._make_record(tmp_path)
        out = tmp_path / "water_polygons.gpkg"
        build_water_polygon_datacube([record], out, min_area_m2=0)
        assert out.exists()

    def test_returns_path(self, tmp_path):
        record = self._make_record(tmp_path)
        out = tmp_path / "water_polygons.gpkg"
        result = build_water_polygon_datacube([record], out, min_area_m2=0)
        assert result == out

    def test_gpkg_contains_one_feature_per_scene(self, tmp_path):
        records = [
            self._make_record(
                tmp_path,
                name=f"S2A_MSIL1C_2025080{i}T101031_N0500_R024_T21HUD_SCL.tif",
            )
            for i in range(1, 4)
        ]
        out = tmp_path / "water_polygons.gpkg"
        build_water_polygon_datacube(records, out, min_area_m2=0)
        gdf = gpd.read_file(out)
        assert len(gdf) == 3

    def test_geojson_files_are_kept(self, tmp_path):
        """Intermediate GeoJSON files must persist after datacube is built."""
        record = self._make_record(tmp_path)
        out = tmp_path / "water_polygons.gpkg"
        build_water_polygon_datacube([record], out, min_area_m2=0)
        geojson_dir = out.parent / GEOJSON_SUBDIR
        assert geojson_dir.exists()
        assert len(list(geojson_dir.glob("*.geojson"))) == 1

    def test_geojson_in_correct_subdir(self, tmp_path):
        """GeoJSON files land in {output_path.parent}/geojson/."""
        record = self._make_record(tmp_path)
        out = tmp_path / "water_polygons.gpkg"
        build_water_polygon_datacube([record], out, min_area_m2=0)
        geojson_dir = out.parent / GEOJSON_SUBDIR
        files = list(geojson_dir.glob("*.geojson"))
        assert len(files) == 1

    # --- Idempotency / duplicate handling ---

    def test_skips_duplicate_scene_id_date(self, tmp_path):
        """Calling twice with the same record must not duplicate features."""
        record = self._make_record(tmp_path)
        out = tmp_path / "water_polygons.gpkg"
        build_water_polygon_datacube([record], out, min_area_m2=0)
        build_water_polygon_datacube([record], out, min_area_m2=0)
        gdf = gpd.read_file(out)
        assert len(gdf) == 1

    def test_appends_new_scene(self, tmp_path):
        """A second call with a new scene appends without duplication."""
        record1 = self._make_record(
            tmp_path,
            name="S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif",
        )
        record2 = self._make_record(
            tmp_path,
            name="S2A_MSIL1C_20250802T101031_N0500_R024_T21HUD_SCL.tif",
        )
        out = tmp_path / "water_polygons.gpkg"
        build_water_polygon_datacube([record1], out, min_area_m2=0)
        build_water_polygon_datacube([record2], out, min_area_m2=0)
        gdf = gpd.read_file(out)
        assert len(gdf) == 2

    def test_overwrite_rebuilds_from_scratch(self, tmp_path):
        """overwrite=True must replace the existing datacube entirely."""
        record1 = self._make_record(
            tmp_path,
            name="S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif",
        )
        record2 = self._make_record(
            tmp_path,
            name="S2A_MSIL1C_20250802T101031_N0500_R024_T21HUD_SCL.tif",
        )
        out = tmp_path / "water_polygons.gpkg"
        # First build with two records
        build_water_polygon_datacube([record1, record2], out, min_area_m2=0)
        # Overwrite with only one
        build_water_polygon_datacube([record1], out, overwrite=True, min_area_m2=0)
        gdf = gpd.read_file(out)
        assert len(gdf) == 1

    # --- Chronological sorting ---

    def test_sorted_chronologically(self, tmp_path):
        """Features in the GeoPackage must be sorted by date ascending."""
        records = [
            self._make_record(
                tmp_path,
                name=f"S2A_MSIL1C_2025080{i}T101031_N0500_R024_T21HUD_SCL.tif",
            )
            for i in [3, 1, 2]  # intentionally out of order
        ]
        out = tmp_path / "water_polygons.gpkg"
        build_water_polygon_datacube(records, out, min_area_m2=0)
        gdf = gpd.read_file(out)
        dates = [str(d)[:10] for d in gdf["date"]]
        assert dates == sorted(dates)

    # --- No-water handling ---

    def test_skips_scene_with_no_water(self, tmp_path):
        """A scene with no water pixels is skipped; other scenes still processed."""
        good_record = self._make_record(
            tmp_path,
            name="S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif",
        )
        # No-water raster: empty water slice
        no_water_scl = _make_scl_raster(
            tmp_path,
            water_rows=slice(0, 0),
            water_cols=slice(0, 0),
            name="S2A_MSIL1C_20250802T101031_N0500_R024_T21HUD_SCL.tif",
        )
        no_water_record = {"scl_path": no_water_scl}

        out = tmp_path / "water_polygons.gpkg"
        build_water_polygon_datacube([good_record, no_water_record], out, min_area_m2=0)
        gdf = gpd.read_file(out)
        assert len(gdf) == 1  # only the good scene

    def test_returns_path_even_when_all_scenes_skipped(self, tmp_path):
        """Function must return the output path even if nothing was written."""
        no_water_scl = _make_scl_raster(
            tmp_path,
            water_rows=slice(0, 0),
            water_cols=slice(0, 0),
        )
        out = tmp_path / "water_polygons.gpkg"
        result = build_water_polygon_datacube(
            [{"scl_path": no_water_scl}], out, min_area_m2=0
        )
        assert result == out

    # --- scl_kwargs forwarding ---

    def test_scl_kwargs_forwarded(self, tmp_path):
        """min_area_m2 passed as scl_kwarg must be respected."""
        # 1-pixel water raster — will survive min_area_m2=0 but not a large value
        scl = _make_scl_raster(
            tmp_path,
            water_rows=slice(0, 1),
            water_cols=slice(0, 1),
        )
        out = tmp_path / "water_polygons.gpkg"
        build_water_polygon_datacube([{"scl_path": scl}], out, min_area_m2=1_000_000)
        # All polygons filtered → scene skipped → no gpkg written yet
        assert not out.exists() or len(gpd.read_file(out)) == 0
