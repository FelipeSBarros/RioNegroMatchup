"""
Unit tests for acolite_spec.py — Step 5: polygon_clip field and validation.
"""

from pathlib import Path

import pytest

from aquamatch.acolite_spec import IOConfig, RadCorConfig


class TestIOConfigPolygonClip:
    """Tests for the polygon_clip field and its validation."""

    # --- Default value ---

    def test_polygon_clip_defaults_to_false(self):
        io = IOConfig(inputfile="", output="")
        assert io.polygon_clip is False

    # --- Validation ---

    def test_polygon_clip_true_with_polygon_passes(self, tmp_path):
        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=str(polygon_file),
            polygon_clip=True,
        )
        # Should not raise
        io.validate()

    def test_polygon_clip_true_without_polygon_raises(self, tmp_path):
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=None,
            polygon_clip=True,
        )
        with pytest.raises(
            ValueError, match="polygon_clip=True requires a valid polygon path"
        ):
            io.validate()

    def test_polygon_clip_false_without_polygon_passes(self, tmp_path):
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=None,
            polygon_clip=False,
        )
        # Should not raise
        io.validate()

    def test_polygon_clip_false_with_polygon_passes(self, tmp_path):
        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=str(polygon_file),
            polygon_clip=False,
        )
        # polygon_clip=False — no constraint on polygon
        io.validate()

    def test_limit_and_polygon_still_mutually_exclusive(self, tmp_path):
        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            limit=(-33.0, -57.0, -32.5, -56.0),
            polygon=str(polygon_file),
        )
        with pytest.raises(ValueError, match="either `limit` or `polygon`"):
            io.validate()

    # --- Serialisation ---

    def test_polygon_clip_true_appears_in_settings_dict(self, tmp_path):
        from aquamatch.acolite_spec import AcoliteConfig

        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")

        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(
                inputfile="",
                output="",
                polygon=str(polygon_file),
                polygon_clip=True,
            ),
        )
        settings = cfg.to_settings_dict()
        assert settings.get("polygon_clip") == "true"
        assert settings.get("polygon") == str(polygon_file)

    def test_polygon_clip_false_absent_from_settings_dict(self):
        from aquamatch.acolite_spec import AcoliteConfig

        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(inputfile="", output="", polygon_clip=False),
        )
        settings = cfg.to_settings_dict()
        assert "polygon_clip" not in settings


# ---------------------------------------------------------------------------
# with_scl_polygon — Step 6 tests
# ---------------------------------------------------------------------------

import numpy as np
import rasterio
from rasterio.transform import from_bounds
from aquamatch.acolite_spec import AcoliteConfig
from aquamatch.scl_water import SCL_WATER_CLASS, GEOJSON_SUBDIR

# Reuse same synthetic raster helper pattern from test_scl_water.py
_TEST_CRS = "EPSG:32721"
_W, _S, _E, _N = 500_000.0, 6_350_000.0, 500_300.0, 6_350_300.0


def _make_scl(tmp_path, name="S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif"):
    data = np.full((30, 30), 4, dtype=np.uint8)
    data[5:25, 5:25] = SCL_WATER_CLASS
    transform = from_bounds(_W, _S, _E, _N, 30, 30)
    path = tmp_path / name
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=30,
        width=30,
        count=1,
        dtype=np.uint8,
        crs=_TEST_CRS,
        transform=transform,
    ) as dst:
        dst.write(data, 1)
    return path


def _make_cfg(tmp_path):
    return AcoliteConfig(
        acolite_executable="/fake/acolite",
        io=IOConfig(inputfile="", output=str(tmp_path)),
    )


class TestWithSclPolygon:
    """Tests for AcoliteConfig.with_scl_polygon()."""

    # --- Return type and immutability ---

    def test_returns_acolite_config(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert isinstance(result, AcoliteConfig)

    def test_returns_new_instance(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert result is not cfg

    def test_original_config_not_mutated(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        cfg.with_scl_polygon(scl, min_area_m2=0)
        assert cfg.io.polygon is None
        assert cfg.io.polygon_clip is False

    # --- polygon and polygon_clip wired correctly ---

    def test_polygon_set_to_geojson_path(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert result.io.polygon is not None
        assert result.io.polygon.endswith(".geojson")

    def test_polygon_clip_is_true(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert result.io.polygon_clip is True

    def test_limit_cleared(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(
                inputfile="",
                output=str(tmp_path),
                limit=(-33.0, -57.0, -32.5, -56.0),
            ),
        )
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert result.io.limit is None

    # --- GeoJSON file location ---

    def test_geojson_written_to_default_subdir(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        expected_dir = tmp_path / GEOJSON_SUBDIR
        assert Path(result.io.polygon).parent == expected_dir

    def test_geojson_written_to_custom_dir(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        custom_dir = tmp_path / "custom_geojson"
        result = cfg.with_scl_polygon(scl, geojson_output_dir=custom_dir, min_area_m2=0)
        assert Path(result.io.polygon).parent == custom_dir

    def test_geojson_file_exists_on_disk(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert Path(result.io.polygon).exists()

    # --- Idempotency ---

    def test_reuses_existing_geojson_when_overwrite_false(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result1 = cfg.with_scl_polygon(scl, min_area_m2=0)
        mtime = Path(result1.io.polygon).stat().st_mtime
        result2 = cfg.with_scl_polygon(scl, overwrite=False, min_area_m2=0)
        assert Path(result2.io.polygon).stat().st_mtime == mtime

    # --- Validation passes after with_scl_polygon ---

    def test_resulting_config_passes_io_validation(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        # Should not raise — polygon exists, polygon_clip=True, limit=None
        result.io.validate()

    # --- Serialisation ---

    def test_polygon_clip_in_settings_dict(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        settings = result.to_settings_dict()
        assert settings.get("polygon_clip") == "true"
        assert "polygon" in settings

    # --- Error propagation ---

    def test_raises_if_scl_not_found(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        with pytest.raises(FileNotFoundError):
            cfg.with_scl_polygon(tmp_path / "nonexistent.tif")

    def test_raises_if_no_water_pixels(self, tmp_path):
        # Raster with no water
        data = np.full((30, 30), 4, dtype=np.uint8)
        transform = from_bounds(_W, _S, _E, _N, 30, 30)
        scl = tmp_path / "no_water_SCL.tif"
        with rasterio.open(
            scl,
            "w",
            driver="GTiff",
            height=30,
            width=30,
            count=1,
            dtype=np.uint8,
            crs=_TEST_CRS,
            transform=transform,
        ) as dst:
            dst.write(data, 1)

        cfg = _make_cfg(tmp_path)
        with pytest.raises(ValueError, match="No water pixels"):
            cfg.with_scl_polygon(scl)


# ---------------------------------------------------------------------------
# run_batch extension — Step 9 tests
# ---------------------------------------------------------------------------

from unittest.mock import patch


def _make_safe(
    tmp_path, name="S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_20230919T094731.SAFE"
):
    """Create a minimal fake SAFE folder on disk."""
    safe = tmp_path / name
    safe.mkdir(parents=True, exist_ok=True)
    (safe / "dummy.xml").write_text("<root/>")
    return safe


def _make_scl_for_safe(scl_dir, safe_path):
    """Create a synthetic SCL raster paired with a SAFE folder."""
    stem = safe_path.stem
    scl_dir.mkdir(parents=True, exist_ok=True)
    scl_path = scl_dir / f"{stem}_SCL.tif"

    data = np.full((30, 30), 4, dtype=np.uint8)
    data[5:25, 5:25] = SCL_WATER_CLASS
    transform = from_bounds(_W, _S, _E, _N, 30, 30)

    with rasterio.open(
        scl_path,
        "w",
        driver="GTiff",
        height=30,
        width=30,
        count=1,
        dtype=np.uint8,
        crs=_TEST_CRS,
        transform=transform,
    ) as dst:
        dst.write(data, 1)
    return scl_path


def _make_batch_cfg(tmp_path):
    """AcoliteConfig with a fake executable that exists on disk."""
    exe = tmp_path / "acolite"
    exe.write_text("#!/bin/sh")
    exe.chmod(0o755)
    return AcoliteConfig(
        acolite_executable=str(exe),
        io=IOConfig(inputfile="", output=str(tmp_path)),
    )


class TestRunBatchSclExtension:
    """Tests for run_batch use_scl / scl_dir / scl_kwargs parameters."""

    # --- Early validation ---

    def test_raises_if_use_scl_true_and_scl_dir_none(self, tmp_path):
        cfg = _make_batch_cfg(tmp_path)
        with pytest.raises(ValueError, match="scl_dir"):
            cfg.run_batch([], tmp_path, use_scl=True, scl_dir=None)

    def test_no_raise_if_use_scl_false_and_scl_dir_none(self, tmp_path):
        cfg = _make_batch_cfg(tmp_path)
        # Empty safe_list — should return immediately without error
        results = cfg.run_batch([], tmp_path, use_scl=False, scl_dir=None)
        assert results == []

    # --- scl_used flag in results ---

    def test_scl_used_false_when_use_scl_disabled(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_batch_cfg(tmp_path)
        with patch.object(
            cfg,
            "_execute",
            return_value={
                "returncode": 0,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
                "inputfile": str(safe),
                "output_dir": tmp_path,
            },
        ):
            results = cfg.run_batch([safe], tmp_path, use_scl=False)
        assert results[0]["scl_used"] is False

    def test_scl_used_true_when_scl_found_and_extracted(self, tmp_path):
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        _make_scl_for_safe(scl_dir, safe)
        cfg = _make_batch_cfg(tmp_path)

        with patch.object(
            cfg,
            "_execute",
            return_value={
                "returncode": 0,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
                "inputfile": str(safe),
                "output_dir": tmp_path,
            },
        ):
            results = cfg.run_batch(
                [safe],
                tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
                scl_kwargs={"min_area_m2": 0},
            )
        assert results[0]["scl_used"] is True

    def test_scl_used_false_when_scl_not_found(self, tmp_path):
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()  # empty — no SCL file inside
        cfg = _make_batch_cfg(tmp_path)

        with patch.object(
            cfg,
            "_execute",
            return_value={
                "returncode": 0,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
                "inputfile": str(safe),
                "output_dir": tmp_path,
            },
        ):
            results = cfg.run_batch(
                [safe],
                tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
            )
        assert results[0]["scl_used"] is False

    # --- State isolation between scenes ---

    def test_polygon_state_reset_between_scenes(self, tmp_path):
        """polygon and polygon_clip must not bleed from one scene to the next."""
        safe1 = _make_safe(
            tmp_path,
            "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_20230919T094731.SAFE",
        )
        safe2 = _make_safe(
            tmp_path,
            "S2A_MSIL1C_20250802T101031_N0500_R024_T21HUD_20230919T094731.SAFE",
        )
        scl_dir = tmp_path / "scl"
        # Only safe1 has an SCL file
        _make_scl_for_safe(scl_dir, safe1)

        cfg = _make_batch_cfg(tmp_path)
        captured_settings = []

        def fake_execute(settings_path):
            captured_settings.append(settings_path.read_text())
            return {
                "returncode": 0,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
                "inputfile": "",
                "output_dir": tmp_path,
            }

        with patch.object(cfg, "_execute", side_effect=fake_execute):
            results = cfg.run_batch(
                [safe1, safe2],
                tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
                scl_kwargs={"min_area_m2": 0},
            )

        assert results[0]["scl_used"] is True
        assert results[1]["scl_used"] is False

        # safe2 settings must not contain polygon_clip
        assert "polygon_clip" not in captured_settings[1]

    def test_original_config_not_mutated_after_batch(self, tmp_path):
        """run_batch must leave the original config unchanged."""
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        _make_scl_for_safe(scl_dir, safe)
        cfg = _make_batch_cfg(tmp_path)

        original_polygon = cfg.io.polygon
        original_polygon_clip = cfg.io.polygon_clip

        with patch.object(
            cfg,
            "_execute",
            return_value={
                "returncode": 0,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
                "inputfile": str(safe),
                "output_dir": tmp_path,
            },
        ):
            cfg.run_batch(
                [safe],
                tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
                scl_kwargs={"min_area_m2": 0},
            )

        assert cfg.io.polygon == original_polygon
        assert cfg.io.polygon_clip == original_polygon_clip

    # --- SCL extraction failure resilience ---

    def test_continues_without_clipping_when_scl_extraction_fails(self, tmp_path):
        """If SCL extraction raises, the scene is still processed without clipping."""
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        _make_scl_for_safe(scl_dir, safe)
        cfg = _make_batch_cfg(tmp_path)

        with patch(
            "aquamatch.acolite_spec.AcoliteConfig.with_scl_polygon",
            side_effect=ValueError("no water pixels"),
        ), patch.object(
            cfg,
            "_execute",
            return_value={
                "returncode": 0,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
                "inputfile": str(safe),
                "output_dir": tmp_path,
            },
        ):
            results = cfg.run_batch(
                [safe],
                tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
            )

        assert results[0]["scl_used"] is False
        assert results[0]["returncode"] == 0  # still processed

    # --- scl_kwargs forwarding ---

    def test_scl_kwargs_forwarded_to_with_scl_polygon(self, tmp_path):
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        _make_scl_for_safe(scl_dir, safe)
        cfg = _make_batch_cfg(tmp_path)

        captured_kwargs = {}

        original_with_scl = cfg.with_scl_polygon

        def capturing_with_scl(scl_path, **kwargs):
            captured_kwargs.update(kwargs)
            return original_with_scl(scl_path, **kwargs)

        with patch.object(
            cfg, "with_scl_polygon", side_effect=capturing_with_scl
        ), patch.object(
            cfg,
            "_execute",
            return_value={
                "returncode": 0,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
                "inputfile": str(safe),
                "output_dir": tmp_path,
            },
        ):
            cfg.run_batch(
                [safe],
                tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
                scl_kwargs={"min_area_m2": 0, "buffer_m": 30},
            )

        assert captured_kwargs.get("min_area_m2") == 0
        assert captured_kwargs.get("buffer_m") == 30

    # --- dry_run compatibility ---

    def test_dry_run_with_use_scl(self, tmp_path):
        """dry_run=True must work alongside use_scl=True."""
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        _make_scl_for_safe(scl_dir, safe)
        cfg = _make_batch_cfg(tmp_path)

        results = cfg.run_batch(
            [safe],
            tmp_path,
            dry_run=True,
            use_scl=True,
            scl_dir=scl_dir,
            scl_kwargs={"min_area_m2": 0},
        )

        assert results[0]["returncode"] is None
        assert results[0]["scl_used"] is True


# ---------------------------------------------------------------------------
# AcoliteConfig.low_memory — preset tests
# ---------------------------------------------------------------------------


class TestLowMemoryPreset:
    """Tests for AcoliteConfig.low_memory() classmethod."""

    def test_returns_acolite_config(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert isinstance(cfg, AcoliteConfig)

    def test_dsf_tile_dimensions_reduced(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert cfg.radcor.dsf_tile_dimensions == (60, 60)

    def test_dsf_path_reflectance_is_tiled(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert cfg.radcor.dsf_path_reflectance == "tiled"

    def test_export_cog_is_false(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert cfg.output_format.export_cloud_optimized_geotiff is False

    def test_map_rgb_is_false(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert cfg.output_format.map_rgb is False

    def test_netcdf_compression_level_is_2(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert cfg.output_format.netcdf_compression_level == 2

    def test_output_rhorc_is_false(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert cfg.l2w.output_rhorc is False

    def test_kwargs_forwarded_io(self, tmp_path):
        io = IOConfig(inputfile="", output=str(tmp_path))
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite", io=io)
        assert cfg.io.output == str(tmp_path)

    def test_settings_dict_contains_tile_dimensions(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        settings = cfg.to_settings_dict()
        assert settings["dsf_tile_dimensions"] == "60,60"

    def test_settings_dict_contains_correct_compression_level(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        settings = cfg.to_settings_dict()
        assert settings["netcdf_compression_level"] == "2"

    def test_can_override_tile_dimensions_directly(self):
        """Tile dimensions can be further reduced by constructing RadCorConfig manually."""
        from dataclasses import replace as dc_replace
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        cfg_smaller = dc_replace(cfg, radcor=RadCorConfig(dsf_tile_dimensions=(30, 30)))
        assert cfg_smaller.radcor.dsf_tile_dimensions == (30, 30)


# ---------------------------------------------------------------------------
# Logging — to_settings_file and _with_scl_polygon
# ---------------------------------------------------------------------------

import logging


class TestLogging:
    """Smoke tests confirming key log messages are emitted."""

    def test_to_settings_file_logs_roi_limit(self, tmp_path, caplog):
        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(
                inputfile="",
                output="",
                limit=(-33.0, -57.0, -32.5, -56.0),
            ),
        )
        with caplog.at_level(logging.INFO, logger="aquamatch.acolite_spec"):
            cfg.to_settings_file(tmp_path / "settings.txt")

        assert any("ROI" in m for m in caplog.messages)
        assert any("-33.0" in m for m in caplog.messages)

    def test_to_settings_file_logs_polygon(self, tmp_path, caplog):
        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")
        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(
                inputfile="",
                output="",
                polygon=str(polygon_file),
                polygon_clip=True,
            ),
        )
        with caplog.at_level(logging.INFO, logger="aquamatch.acolite_spec"):
            cfg.to_settings_file(tmp_path / "settings.txt")

        assert any("polygon_clip=true" in m for m in caplog.messages)

    def test_to_settings_file_logs_full_scene_when_no_roi(self, tmp_path, caplog):
        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(inputfile="", output=""),
        )
        with caplog.at_level(logging.INFO, logger="aquamatch.acolite_spec"):
            cfg.to_settings_file(tmp_path / "settings.txt")

        assert any("full scene" in m for m in caplog.messages)

    def test_with_scl_polygon_logs_scl_name(self, tmp_path, caplog):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        with caplog.at_level(logging.INFO, logger="aquamatch.acolite_spec"):
            cfg.with_scl_polygon(scl, min_area_m2=0)

        assert any(scl.name in m for m in caplog.messages)

    def test_with_scl_polygon_logs_polygon_path(self, tmp_path, caplog):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        with caplog.at_level(logging.INFO, logger="aquamatch.acolite_spec"):
            result = cfg.with_scl_polygon(scl, min_area_m2=0)

        assert any(".geojson" in m for m in caplog.messages)
        assert any(Path(result.io.polygon).name in m for m in caplog.messages)
