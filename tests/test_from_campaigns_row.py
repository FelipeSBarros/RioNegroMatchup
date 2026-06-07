"""
Tests for the updated AcoliteConfig.from_campaigns_row — Step 3.

Covers:
  - Legacy behaviour (tile_config=None) unchanged
  - tile_config with polygon entry → polygon + polygon_clip=True, limit=None
  - tile_config with limit entry  → limit set, no polygon
  - tile_config with empty entry  → full scene (no restriction)
  - tile_config provided but s2_tile missing from row → warning, full scene
  - tile_config provided but tile not listed → full scene (no restriction)
  - output_dir derived from date
  - kwargs forwarded to AcoliteConfig
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from rionegromatchup.acolite_spec import AcoliteConfig, IOConfig
from rionegromatchup.pipeline_config import TileEntry, TilesSection

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row(
    lat=-32.85,
    lon=-56.5,
    date="2025-08-01",
    s2_tile="21HUD",
    include_tile=True,
):
    """Build a minimal dict-like campaigns row."""
    row = {"latitud": lat, "longitud": lon, "date": date}
    if include_tile:
        row["s2_tile"] = s2_tile
    return row


def _tiles(polygon=None, limit=None):
    """Build a TilesSection with a single 21HUD entry."""
    entry = TileEntry(polygon=polygon, limit=limit)
    return TilesSection(entries={"21HUD": entry})


# ---------------------------------------------------------------------------
# Legacy behaviour — tile_config=None
# ---------------------------------------------------------------------------


class TestFromCampaignsRowLegacy:
    """tile_config=None must preserve the original lat/lon buffer behaviour."""

    def test_returns_acolite_config(self):
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
        )
        assert isinstance(cfg, AcoliteConfig)

    def test_limit_derived_from_latlon(self):
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(lat=-32.85, lon=-56.5),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
        )
        s, w, n, e = cfg.io.limit
        assert pytest.approx(s) == -32.85 - 0.1
        assert pytest.approx(w) == -56.5 - 0.1
        assert pytest.approx(n) == -32.85 + 0.1
        assert pytest.approx(e) == -56.5 + 0.1

    def test_no_polygon_when_tile_config_none(self):
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
        )
        assert cfg.io.polygon is None
        assert cfg.io.polygon_clip is False

    def test_output_dir_includes_date(self):
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(date="2025-08-01"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
        )
        assert "2025-08-01" in cfg.io.output

    def test_executable_set(self):
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
        )
        assert cfg.acolite_executable == "/fake/acolite"

    def test_kwargs_forwarded(self):
        from rionegromatchup.acolite_spec import RadCorConfig

        custom_radcor = RadCorConfig(dsf_tile_dimensions=(60, 60))
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            radcor=custom_radcor,
        )
        assert cfg.radcor.dsf_tile_dimensions == (60, 60)


# ---------------------------------------------------------------------------
# tile_config with polygon entry
# ---------------------------------------------------------------------------


class TestFromCampaignsRowPolygon:

    def test_polygon_set_from_tile_config(self):
        tiles = _tiles(polygon="data/polygons/21HUD.geojson")
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert cfg.io.polygon == "data/polygons/21HUD.geojson"

    def test_polygon_clip_true(self):
        tiles = _tiles(polygon="data/polygons/21HUD.geojson")
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert cfg.io.polygon_clip is True

    def test_limit_is_none_when_polygon_used(self):
        tiles = _tiles(polygon="data/polygons/21HUD.geojson")
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert cfg.io.limit is None


# ---------------------------------------------------------------------------
# tile_config with limit entry
# ---------------------------------------------------------------------------


class TestFromCampaignsRowLimit:

    def test_limit_set_from_tile_config(self):
        tiles = _tiles(limit=[-34.2, -56.8, -33.0, -55.1])
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert cfg.io.limit == (-34.2, -56.8, -33.0, -55.1)

    def test_limit_is_tuple(self):
        """limit must be a tuple to match IOConfig expectations."""
        tiles = _tiles(limit=[-34.2, -56.8, -33.0, -55.1])
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert isinstance(cfg.io.limit, tuple)

    def test_no_polygon_when_limit_used(self):
        tiles = _tiles(limit=[-34.2, -56.8, -33.0, -55.1])
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert cfg.io.polygon is None
        assert cfg.io.polygon_clip is False

    def test_limit_not_overridden_by_latlon_buffer(self):
        """Tile config limit must not be replaced by lat/lon buffer."""
        tiles = _tiles(limit=[-34.2, -56.8, -33.0, -55.1])
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(lat=-32.85, lon=-56.5, s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        # Must be the tile config value, not the lat/lon buffer
        assert cfg.io.limit == (-34.2, -56.8, -33.0, -55.1)


# ---------------------------------------------------------------------------
# tile_config with no restriction (empty entry)
# ---------------------------------------------------------------------------


class TestFromCampaignsRowNoRestriction:

    def test_empty_tile_entry_gives_full_scene(self):
        """A tile listed with no polygon or limit → full scene."""
        tiles = TilesSection(entries={"21HUD": TileEntry()})
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert cfg.io.limit is None
        assert cfg.io.polygon is None
        assert cfg.io.polygon_clip is False

    def test_empty_tile_entry_does_not_fall_back_to_latlon(self):
        """Explicit empty entry must NOT trigger the legacy lat/lon buffer."""
        tiles = TilesSection(entries={"21HUD": TileEntry()})
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(lat=-32.85, lon=-56.5, s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert cfg.io.limit is None


# ---------------------------------------------------------------------------
# tile_config provided but tile not listed
# ---------------------------------------------------------------------------


class TestFromCampaignsRowTileNotListed:

    def test_unlisted_tile_gives_full_scene(self):
        """Tile not present in config → no restriction, no fallback buffer."""
        tiles = TilesSection(entries={"21HVD": TileEntry()})  # 21HUD not listed
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert cfg.io.limit is None
        assert cfg.io.polygon is None

    def test_unlisted_tile_does_not_use_latlon_buffer(self):
        tiles = TilesSection(entries={"21HVD": TileEntry()})
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(lat=-32.85, lon=-56.5, s2_tile="21HUD"),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert cfg.io.limit is None


# ---------------------------------------------------------------------------
# Missing s2_tile in row
# ---------------------------------------------------------------------------


class TestFromCampaignsRowMissingTile:

    def test_missing_s2_tile_gives_full_scene(self):
        """Row without s2_tile → warning logged, full scene processed."""
        tiles = _tiles(limit=[-34.2, -56.8, -33.0, -55.1])
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(include_tile=False),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="",
            tile_config=tiles,
        )
        assert cfg.io.limit is None
        assert cfg.io.polygon is None

    def test_missing_s2_tile_emits_warning(self, caplog):
        tiles = _tiles(limit=[-34.2, -56.8, -33.0, -55.1])
        with caplog.at_level(logging.WARNING, logger="rionegromatchup.acolite_spec"):
            AcoliteConfig.from_campaigns_row(
                row=_row(include_tile=False),
                acolite_executable="/fake/acolite",
                base_output="/tmp/out",
                inputfile="",
                tile_config=tiles,
            )
        assert any("s2_tile" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# output_dir and inputfile wiring
# ---------------------------------------------------------------------------


class TestFromCampaignsRowOutputDir:

    def test_output_dir_uses_base_output_and_date(self):
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(date="2017-07-13"),
            acolite_executable="/fake/acolite",
            base_output="/data/out",
            inputfile="",
            tile_config=_tiles(limit=[-34.2, -56.8, -33.0, -55.1]),
        )
        assert cfg.io.output == str(Path("/data/out") / "2017-07-13")

    def test_inputfile_set(self):
        cfg = AcoliteConfig.from_campaigns_row(
            row=_row(),
            acolite_executable="/fake/acolite",
            base_output="/tmp/out",
            inputfile="/data/safe/scene.SAFE",
            tile_config=_tiles(limit=[-34.2, -56.8, -33.0, -55.1]),
        )
        assert cfg.io.inputfile == "/data/safe/scene.SAFE"
