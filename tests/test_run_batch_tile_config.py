"""
Tests for run_batch tile_config integration — Step 4.

Covers:
  - tile_config=None: existing behaviour unchanged (no tile_restriction key changes)
  - Static polygon from tile_config applied to io
  - Static limit from tile_config applied to io
  - Empty tile entry → full scene
  - Tile not listed → full scene
  - tile_restriction key present in all result dicts
  - Static polygon suppresses SCL clipping
  - SCL clipping still runs when tile has limit (not polygon)
  - SCL clipping still runs when tile not listed
  - State isolation: tile restriction does not bleed between scenes
  - dry_run compatibility with tile_config
  - tile_id extraction warning when SAFE name is unrecognised
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from rionegromatchup.acolite_spec import AcoliteConfig, IOConfig
from rionegromatchup.pipeline_config import TileEntry, TilesSection
from rionegromatchup.scl_water import SCL_WATER_CLASS

# ---------------------------------------------------------------------------
# Shared test fixtures / helpers
# ---------------------------------------------------------------------------

_TEST_CRS = "EPSG:32721"
_W, _S, _E, _N = 500_000.0, 6_350_000.0, 500_300.0, 6_350_300.0

# Canonical SAFE name — tile 21HUD
_SAFE_NAME = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_20230919T094731.SAFE"
_SAFE_NAME_2 = "S2A_MSIL1C_20250802T101031_N0500_R024_T21HUD_20230919T094731.SAFE"
# Different tile — 21HVD
_SAFE_NAME_HVD = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HVD_20230919T094731.SAFE"
# Unrecognisable name — no tile extractable
_SAFE_NAME_UNKNOWN = "unknown_scene.SAFE"


def _make_safe(tmp_path, name=_SAFE_NAME):
    safe = tmp_path / name
    safe.mkdir(parents=True, exist_ok=True)
    (safe / "dummy.xml").write_text("<root/>")
    return safe


def _make_scl(scl_dir, safe_path):
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


def _make_cfg(tmp_path):
    exe = tmp_path / "acolite"
    exe.write_text("#!/bin/sh")
    exe.chmod(0o755)
    return AcoliteConfig(
        acolite_executable=str(exe),
        io=IOConfig(inputfile="", output=str(tmp_path)),
    )


def _fake_execute(safe_path, output_dir):
    """Return a minimal success result dict."""
    return {
        "returncode": 0,
        "log_file": None,
        "l2w_file": None,
        "stdout": "",
        "stderr": "",
        "inputfile": str(safe_path),
        "output_dir": output_dir,
    }


# ---------------------------------------------------------------------------
# tile_restriction key always present
# ---------------------------------------------------------------------------


class TestTileRestrictionKeyAlwaysPresent:

    def test_present_when_tile_config_none(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            results = cfg.run_batch([safe], tmp_path)
        assert "tile_restriction" in results[0]

    def test_present_in_dry_run(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        results = cfg.run_batch([safe], tmp_path, dry_run=True)
        assert "tile_restriction" in results[0]

    def test_present_when_skipped(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        missing = tmp_path / "nonexistent.SAFE"
        results = cfg.run_batch([missing], tmp_path)
        assert "tile_restriction" in results[0]


# ---------------------------------------------------------------------------
# tile_config=None — no regression
# ---------------------------------------------------------------------------


class TestRunBatchNoTileConfig:

    def test_tile_restriction_is_none_string(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            results = cfg.run_batch([safe], tmp_path)
        assert results[0]["tile_restriction"] == "none"

    def test_io_limit_not_set_from_tile_config(self, tmp_path):
        """Without tile_config, run_batch must not apply any limit."""
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        captured = {}

        def fake_execute(settings_path):
            captured["limit"] = cfg.io.limit
            captured["polygon"] = cfg.io.polygon
            return _fake_execute(safe, tmp_path)

        with patch.object(cfg, "_execute", side_effect=fake_execute):
            cfg.run_batch([safe], tmp_path)

        assert captured["limit"] is None
        assert captured["polygon"] is None


# ---------------------------------------------------------------------------
# Static polygon from tile_config
# ---------------------------------------------------------------------------


class TestRunBatchTilePolygon:

    def _tiles_with_polygon(self):
        return TilesSection(
            entries={"21HUD": TileEntry(polygon="data/polygons/21HUD.geojson")}
        )

    def test_tile_restriction_is_polygon(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            results = cfg.run_batch(
                [safe],
                tmp_path,
                tile_config=self._tiles_with_polygon(),
            )
        assert results[0]["tile_restriction"] == "polygon"

    def test_polygon_applied_to_io(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        captured = {}

        def fake_execute(settings_path):
            captured["polygon"] = cfg.io.polygon
            captured["polygon_clip"] = cfg.io.polygon_clip
            return _fake_execute(safe, tmp_path)

        with patch.object(cfg, "_execute", side_effect=fake_execute):
            cfg.run_batch(
                [safe],
                tmp_path,
                tile_config=self._tiles_with_polygon(),
            )

        assert captured["polygon"] == "data/polygons/21HUD.geojson"
        assert captured["polygon_clip"] is True

    def test_limit_is_none_when_polygon_applied(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        captured = {}

        def fake_execute(settings_path):
            captured["limit"] = cfg.io.limit
            return _fake_execute(safe, tmp_path)

        with patch.object(cfg, "_execute", side_effect=fake_execute):
            cfg.run_batch(
                [safe],
                tmp_path,
                tile_config=self._tiles_with_polygon(),
            )

        assert captured["limit"] is None

    def test_scl_suppressed_when_static_polygon_set(self, tmp_path):
        """use_scl=True must be suppressed when tile has a static polygon."""
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        _make_scl(scl_dir, safe)
        cfg = _make_cfg(tmp_path)

        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            results = cfg.run_batch(
                [safe],
                tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
                scl_kwargs={"min_area_m2": 0},
                tile_config=self._tiles_with_polygon(),
            )

        assert results[0]["scl_used"] is False
        assert results[0]["tile_restriction"] == "polygon"

    def test_scl_suppressed_logs_message(self, tmp_path, caplog):
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        _make_scl(scl_dir, safe)
        cfg = _make_cfg(tmp_path)

        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ), caplog.at_level(logging.INFO, logger="rionegromatchup.acolite_spec"):
            cfg.run_batch(
                [safe],
                tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
                scl_kwargs={"min_area_m2": 0},
                tile_config=self._tiles_with_polygon(),
            )

        assert any("suppressed" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Static limit from tile_config
# ---------------------------------------------------------------------------


class TestRunBatchTileLimit:

    def _tiles_with_limit(self):
        return TilesSection(
            entries={"21HUD": TileEntry(limit=[-34.2, -56.8, -33.0, -55.1])}
        )

    def test_tile_restriction_is_limit(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            results = cfg.run_batch(
                [safe],
                tmp_path,
                tile_config=self._tiles_with_limit(),
            )
        assert results[0]["tile_restriction"] == "limit"

    def test_limit_applied_to_io(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        captured = {}

        def fake_execute(settings_path):
            captured["limit"] = cfg.io.limit
            return _fake_execute(safe, tmp_path)

        with patch.object(cfg, "_execute", side_effect=fake_execute):
            cfg.run_batch(
                [safe],
                tmp_path,
                tile_config=self._tiles_with_limit(),
            )

        assert captured["limit"] == (-34.2, -56.8, -33.0, -55.1)

    def test_no_polygon_when_limit_applied(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        captured = {}

        def fake_execute(settings_path):
            captured["polygon"] = cfg.io.polygon
            captured["polygon_clip"] = cfg.io.polygon_clip
            return _fake_execute(safe, tmp_path)

        with patch.object(cfg, "_execute", side_effect=fake_execute):
            cfg.run_batch(
                [safe],
                tmp_path,
                tile_config=self._tiles_with_limit(),
            )

        assert captured["polygon"] is None
        assert captured["polygon_clip"] is False

    def test_scl_still_runs_when_tile_has_limit(self, tmp_path):
        """SCL clipping should proceed normally when tile only has a limit."""
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        _make_scl(scl_dir, safe)
        cfg = _make_cfg(tmp_path)

        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            results = cfg.run_batch(
                [safe],
                tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
                scl_kwargs={"min_area_m2": 0},
                tile_config=self._tiles_with_limit(),
            )

        assert results[0]["scl_used"] is True
        assert results[0]["tile_restriction"] == "polygon"


# ---------------------------------------------------------------------------
# Empty tile entry and unlisted tile → full scene
# ---------------------------------------------------------------------------


class TestRunBatchTileFullScene:

    def test_empty_entry_gives_none_restriction(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        tiles = TilesSection(entries={"21HUD": TileEntry()})
        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            results = cfg.run_batch([safe], tmp_path, tile_config=tiles)
        assert results[0]["tile_restriction"] == "none"

    def test_empty_entry_no_limit_or_polygon(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        tiles = TilesSection(entries={"21HUD": TileEntry()})
        captured = {}

        def fake_execute(settings_path):
            captured["limit"] = cfg.io.limit
            captured["polygon"] = cfg.io.polygon
            return _fake_execute(safe, tmp_path)

        with patch.object(cfg, "_execute", side_effect=fake_execute):
            cfg.run_batch([safe], tmp_path, tile_config=tiles)

        assert captured["limit"] is None
        assert captured["polygon"] is None

    def test_unlisted_tile_gives_none_restriction(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        # 21HVD listed, but scene is 21HUD
        tiles = TilesSection(
            entries={"21HVD": TileEntry(limit=[-34.2, -56.8, -33.0, -55.1])}
        )
        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            results = cfg.run_batch([safe], tmp_path, tile_config=tiles)
        assert results[0]["tile_restriction"] == "none"

    def test_scl_still_runs_for_unlisted_tile(self, tmp_path):
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        _make_scl(scl_dir, safe)
        cfg = _make_cfg(tmp_path)
        tiles = TilesSection(entries={"21HVD": TileEntry()})  # 21HUD not listed

        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            results = cfg.run_batch(
                [safe],
                tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
                scl_kwargs={"min_area_m2": 0},
                tile_config=tiles,
            )

        assert results[0]["scl_used"] is True


# ---------------------------------------------------------------------------
# Unrecognisable SAFE filename
# ---------------------------------------------------------------------------


class TestRunBatchUnrecognisableSafeName:

    def test_unknown_tile_id_emits_warning(self, tmp_path, caplog):
        safe = _make_safe(tmp_path, name=_SAFE_NAME_UNKNOWN)
        cfg = _make_cfg(tmp_path)
        tiles = TilesSection(
            entries={"21HUD": TileEntry(limit=[-34.2, -56.8, -33.0, -55.1])}
        )

        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ), caplog.at_level(logging.WARNING, logger="rionegromatchup.acolite_spec"):
            cfg.run_batch([safe], tmp_path, tile_config=tiles)

        assert any("tile ID" in msg for msg in caplog.messages)

    def test_unknown_tile_id_gives_none_restriction(self, tmp_path):
        safe = _make_safe(tmp_path, name=_SAFE_NAME_UNKNOWN)
        cfg = _make_cfg(tmp_path)
        tiles = TilesSection(
            entries={"21HUD": TileEntry(limit=[-34.2, -56.8, -33.0, -55.1])}
        )

        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            results = cfg.run_batch([safe], tmp_path, tile_config=tiles)

        assert results[0]["tile_restriction"] == "none"


# ---------------------------------------------------------------------------
# State isolation between scenes
# ---------------------------------------------------------------------------


class TestRunBatchStateIsolation:

    def test_tile_restriction_does_not_bleed_between_scenes(self, tmp_path):
        """
        Scene 1 (21HUD) has a polygon; scene 2 (21HVD) is unlisted.
        Scene 2 must not inherit scene 1's polygon.
        """
        safe1 = _make_safe(tmp_path, name=_SAFE_NAME)
        safe2 = _make_safe(tmp_path, name=_SAFE_NAME_HVD)
        cfg = _make_cfg(tmp_path)
        tiles = TilesSection(
            entries={"21HUD": TileEntry(polygon="data/polygons/21HUD.geojson")}
        )
        captured = []

        def fake_execute(settings_path):
            captured.append(
                {
                    "polygon": cfg.io.polygon,
                    "limit": cfg.io.limit,
                    "polygon_clip": cfg.io.polygon_clip,
                }
            )
            # Return a minimal valid result
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
            results = cfg.run_batch([safe1, safe2], tmp_path, tile_config=tiles)

        assert captured[0]["polygon"] == "data/polygons/21HUD.geojson"
        assert captured[1]["polygon"] is None
        assert captured[1]["polygon_clip"] is False

        assert results[0]["tile_restriction"] == "polygon"
        assert results[1]["tile_restriction"] == "none"

    def test_original_io_restored_after_batch(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        original_polygon = cfg.io.polygon
        original_limit = cfg.io.limit
        tiles = TilesSection(
            entries={"21HUD": TileEntry(limit=[-34.2, -56.8, -33.0, -55.1])}
        )

        with patch.object(
            cfg, "_execute", side_effect=lambda p: _fake_execute(safe, tmp_path)
        ):
            cfg.run_batch([safe], tmp_path, tile_config=tiles)

        assert cfg.io.polygon == original_polygon
        assert cfg.io.limit == original_limit


# ---------------------------------------------------------------------------
# dry_run + tile_config
# ---------------------------------------------------------------------------


class TestRunBatchDryRunTileConfig:

    def test_dry_run_with_polygon_tile(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        tiles = TilesSection(
            entries={"21HUD": TileEntry(polygon="data/polygons/21HUD.geojson")}
        )
        results = cfg.run_batch(
            [safe],
            tmp_path,
            dry_run=True,
            tile_config=tiles,
        )
        assert results[0]["returncode"] is None
        assert results[0]["tile_restriction"] == "polygon"

    def test_dry_run_with_limit_tile(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        tiles = TilesSection(
            entries={"21HUD": TileEntry(limit=[-34.2, -56.8, -33.0, -55.1])}
        )
        results = cfg.run_batch(
            [safe],
            tmp_path,
            dry_run=True,
            tile_config=tiles,
        )
        assert results[0]["returncode"] is None
        assert results[0]["tile_restriction"] == "limit"

    def test_dry_run_scl_suppressed_by_polygon(self, tmp_path):
        safe = _make_safe(tmp_path)
        scl_dir = tmp_path / "scl"
        _make_scl(scl_dir, safe)
        cfg = _make_cfg(tmp_path)
        tiles = TilesSection(
            entries={"21HUD": TileEntry(polygon="data/polygons/21HUD.geojson")}
        )
        results = cfg.run_batch(
            [safe],
            tmp_path,
            dry_run=True,
            use_scl=True,
            scl_dir=scl_dir,
            scl_kwargs={"min_area_m2": 0},
            tile_config=tiles,
        )
        assert results[0]["scl_used"] is False
        assert results[0]["tile_restriction"] == "polygon"
