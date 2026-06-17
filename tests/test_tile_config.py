"""
Tests for TileEntry, TilesSection, and their integration into PipelineConfig.

All existing tests in test_pipeline_config.py are preserved unchanged.
This file covers only the new tile-related additions from Task 1.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aquamatch.pipeline_config import (
    TileEntry,
    TilesSection,
    PipelineConfig,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# TileEntry — construction and defaults
# ---------------------------------------------------------------------------


class TestTileEntryDefaults:

    def test_both_fields_default_to_none(self):
        entry = TileEntry()
        assert entry.polygon is None
        assert entry.limit is None

    def test_polygon_only(self):
        entry = TileEntry(polygon="data/polygons/21HUD.geojson")
        assert entry.polygon == "data/polygons/21HUD.geojson"
        assert entry.limit is None

    def test_limit_only(self):
        entry = TileEntry(limit=[-34.2, -56.8, -33.0, -55.1])
        assert entry.polygon is None
        assert entry.limit == [-34.2, -56.8, -33.0, -55.1]


# ---------------------------------------------------------------------------
# TileEntry.validate()
# ---------------------------------------------------------------------------


class TestTileEntryValidate:

    # --- Happy paths ---

    def test_no_restriction_is_valid(self):
        TileEntry().validate()

    def test_polygon_only_is_valid(self):
        TileEntry(polygon="data/polygons/21HUD.geojson").validate()

    def test_limit_only_is_valid(self):
        TileEntry(limit=[-34.2, -56.8, -33.0, -55.1]).validate()

    # --- Both set ---

    def test_both_set_raises(self):
        with pytest.raises(ValueError, match="either 'polygon' or 'limit'"):
            TileEntry(
                polygon="data/polygons/21HUD.geojson",
                limit=[-34.2, -56.8, -33.0, -55.1],
            ).validate()

    def test_error_message_includes_tile_id(self):
        with pytest.raises(ValueError, match="21HUD"):
            TileEntry(
                polygon="data/polygons/21HUD.geojson",
                limit=[-34.2, -56.8, -33.0, -55.1],
            ).validate(tile_id="21HUD")

    # --- limit length ---

    def test_limit_wrong_length_raises(self):
        with pytest.raises(ValueError, match="exactly 4 values"):
            TileEntry(limit=[-34.2, -56.8, -33.0]).validate()

    def test_limit_five_values_raises(self):
        with pytest.raises(ValueError, match="exactly 4 values"):
            TileEntry(limit=[-34.2, -56.8, -33.0, -55.1, 0.0]).validate()

    # --- limit coordinate order ---

    def test_south_greater_than_north_raises(self):
        with pytest.raises(ValueError, match="south.*north"):
            TileEntry(limit=[-33.0, -56.8, -34.2, -55.1]).validate()

    def test_south_equal_north_raises(self):
        with pytest.raises(ValueError, match="south.*north"):
            TileEntry(limit=[-34.2, -56.8, -34.2, -55.1]).validate()

    def test_west_greater_than_east_raises(self):
        with pytest.raises(ValueError, match="west.*east"):
            TileEntry(limit=[-34.2, -55.1, -33.0, -56.8]).validate()

    # --- latitude range ---

    def test_latitude_out_of_range_raises(self):
        with pytest.raises(ValueError, match="latitude"):
            TileEntry(limit=[-91.0, -56.8, -33.0, -55.1]).validate()

    # --- longitude range ---

    def test_longitude_out_of_range_raises(self):
        with pytest.raises(ValueError, match="longitude"):
            TileEntry(limit=[-34.2, -181.0, -33.0, -55.1]).validate()

    # --- tile_id in error messages ---

    def test_tile_id_included_in_coordinate_error(self):
        with pytest.raises(ValueError, match="21HVD"):
            TileEntry(limit=[-33.0, -56.8, -34.2, -55.1]).validate(tile_id="21HVD")


# ---------------------------------------------------------------------------
# TilesSection.from_dict()
# ---------------------------------------------------------------------------


class TestTilesSectionFromDict:

    def test_empty_dict_gives_empty_section(self):
        ts = TilesSection.from_dict({})
        assert ts.entries == {}

    def test_polygon_entry_parsed(self):
        ts = TilesSection.from_dict({"21HUD": {"polygon": "data/21HUD.geojson"}})
        assert ts.entries["21HUD"].polygon == "data/21HUD.geojson"
        assert ts.entries["21HUD"].limit is None

    def test_limit_entry_parsed(self):
        ts = TilesSection.from_dict({"21HVD": {"limit": [-34.2, -56.8, -33.0, -55.1]}})
        assert ts.entries["21HVD"].limit == [-34.2, -56.8, -33.0, -55.1]
        assert ts.entries["21HVD"].polygon is None

    def test_null_entry_gives_empty_tile_entry(self):
        """A tile listed with no value (null in YAML) means no restriction."""
        ts = TilesSection.from_dict({"21HWD": None})
        entry = ts.entries["21HWD"]
        assert entry.polygon is None
        assert entry.limit is None

    def test_multiple_tiles_parsed(self):
        ts = TilesSection.from_dict(
            {
                "21HUD": {"polygon": "data/21HUD.geojson"},
                "21HVD": {"limit": [-34.2, -56.8, -33.0, -55.1]},
                "21HWD": None,
            }
        )
        assert len(ts.entries) == 3

    def test_unknown_key_raises(self):
        with pytest.raises(ValueError, match="unknown_key"):
            TilesSection.from_dict({"21HUD": {"unknown_key": "value"}})

    def test_unknown_key_error_includes_tile_id(self):
        with pytest.raises(ValueError, match="21HUD"):
            TilesSection.from_dict({"21HUD": {"unknown_key": "value"}})

    def test_both_set_raises_during_parse(self):
        with pytest.raises(ValueError, match="either 'polygon' or 'limit'"):
            TilesSection.from_dict(
                {
                    "21HUD": {
                        "polygon": "data/21HUD.geojson",
                        "limit": [-34.2, -56.8, -33.0, -55.1],
                    }
                }
            )


# ---------------------------------------------------------------------------
# TilesSection.get()
# ---------------------------------------------------------------------------


class TestTilesSectionGet:

    def test_returns_entry_for_known_tile(self):
        ts = TilesSection.from_dict({"21HUD": {"polygon": "data/21HUD.geojson"}})
        entry = ts.get("21HUD")
        assert isinstance(entry, TileEntry)

    def test_returns_none_for_unknown_tile(self):
        ts = TilesSection.from_dict({"21HUD": {"polygon": "data/21HUD.geojson"}})
        assert ts.get("99ZZZ") is None

    def test_returns_none_on_empty_section(self):
        ts = TilesSection()
        assert ts.get("21HUD") is None


# ---------------------------------------------------------------------------
# TilesSection.validate()
# ---------------------------------------------------------------------------


class TestTilesSectionValidate:

    def test_empty_section_is_valid(self):
        TilesSection().validate()

    def test_valid_entries_pass(self):
        ts = TilesSection.from_dict(
            {
                "21HUD": {"polygon": "data/21HUD.geojson"},
                "21HVD": {"limit": [-34.2, -56.8, -33.0, -55.1]},
            }
        )
        ts.validate()  # must not raise

    def test_invalid_entry_raises(self):
        # Bypass from_dict validation to test validate() directly
        ts = TilesSection(
            entries={
                "21HUD": TileEntry(
                    polygon="data/21HUD.geojson",
                    limit=[-34.2, -56.8, -33.0, -55.1],
                )
            }
        )
        with pytest.raises(ValueError, match="21HUD"):
            ts.validate()


# ---------------------------------------------------------------------------
# PipelineConfig — tiles field integration
# ---------------------------------------------------------------------------


class TestPipelineConfigTilesField:

    def test_default_tiles_section_is_empty(self):
        cfg = PipelineConfig()
        assert cfg.tiles.entries == {}

    def test_tiles_field_present_in_defaults(self):
        cfg = PipelineConfig()
        assert hasattr(cfg, "tiles")
        assert isinstance(cfg.tiles, TilesSection)


# ---------------------------------------------------------------------------
# PipelineConfig.from_yaml() — tiles parsing
# ---------------------------------------------------------------------------


class TestPipelineConfigFromYamlTiles:

    def test_missing_tiles_section_gives_empty(self, tmp_path):
        p = _write_yaml(tmp_path, "campaign_name: minimal\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.tiles.entries == {}

    def test_empty_tiles_section_gives_empty(self, tmp_path):
        p = _write_yaml(tmp_path, "tiles: {}\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.tiles.entries == {}

    def test_null_tiles_section_gives_empty(self, tmp_path):
        p = _write_yaml(tmp_path, "tiles:\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.tiles.entries == {}

    def test_polygon_tile_loaded(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            """\
tiles:
  21HUD:
    polygon: data/polygons/21HUD.geojson
""",
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.tiles.get("21HUD").polygon == "data/polygons/21HUD.geojson"

    def test_limit_tile_loaded(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            """\
tiles:
  21HVD:
    limit: [-34.2, -56.8, -33.0, -55.1]
""",
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.tiles.get("21HVD").limit == [-34.2, -56.8, -33.0, -55.1]

    def test_multiple_tiles_loaded(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            """\
tiles:
  21HUD:
    polygon: data/polygons/21HUD.geojson
  21HVD:
    limit: [-34.2, -56.8, -33.0, -55.1]
  21HWD:
""",
        )
        cfg = PipelineConfig.from_yaml(p)
        assert len(cfg.tiles.entries) == 3
        assert cfg.tiles.get("21HWD").polygon is None
        assert cfg.tiles.get("21HWD").limit is None

    def test_unknown_tile_key_raises(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            """\
tiles:
  21HUD:
    bad_key: value
""",
        )
        with pytest.raises(ValueError, match="bad_key"):
            PipelineConfig.from_yaml(p)

    def test_both_polygon_and_limit_raises(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            """\
tiles:
  21HUD:
    polygon: data/polygons/21HUD.geojson
    limit: [-34.2, -56.8, -33.0, -55.1]
""",
        )
        with pytest.raises(ValueError, match="either 'polygon' or 'limit'"):
            PipelineConfig.from_yaml(p)

    def test_invalid_limit_raises(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            """\
tiles:
  21HUD:
    limit: [-34.2, -56.8, -33.0]
""",
        )
        with pytest.raises(ValueError, match="exactly 4 values"):
            PipelineConfig.from_yaml(p)

    def test_tiles_coexists_with_other_sections(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            """\
campaign_name: test
sentinel:
  time_delta: 3
tiles:
  21HUD:
    polygon: data/polygons/21HUD.geojson
""",
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.sentinel.time_delta == 3
        assert cfg.tiles.get("21HUD").polygon == "data/polygons/21HUD.geojson"

    def test_unknown_top_level_key_still_raises(self, tmp_path):
        """Existing unknown-key validation must still work after adding tiles."""
        p = _write_yaml(tmp_path, "not_a_key: value\n")
        with pytest.raises(ValueError, match="not_a_key"):
            PipelineConfig.from_yaml(p)


# ---------------------------------------------------------------------------
# PipelineConfig.generate() — tiles section in template
# ---------------------------------------------------------------------------


class TestPipelineConfigGenerateTiles:

    def test_template_contains_tiles_section(self, tmp_path):
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        assert "tiles:" in out.read_text()

    def test_template_contains_tile_comments(self, tmp_path):
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        content = out.read_text()
        # Key documentation words should appear in the tiles comment block
        assert "polygon" in content
        assert "limit" in content
        assert "MGRS" in content

    def test_template_round_trips_through_from_yaml(self, tmp_path):
        """Generated template must load without errors and give empty tiles."""
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        cfg = PipelineConfig.from_yaml(out)
        assert cfg.tiles.entries == {}
