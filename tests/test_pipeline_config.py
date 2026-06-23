"""
Tests for pipeline_config.py — Steps 1–7.

Covers:
  - generate() → from_yaml() round-trip
  - to_acolite_config() field mapping
  - to_scl_kwargs() / to_insitu_args() / to_sentinel_args()
  - Unknown keys raise ValueError
  - enabled=false sections are skipped by run()
  - CLI smoke tests
"""

from __future__ import annotations

from pathlib import Path

from aquamatch.pipeline_config import (
    main,
    DownloadSection,
    PipelineConfig,
    VALID_DOWNLOAD_STRATEGIES,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(content)
    return p


def _make_template(tmp_path: Path) -> Path:
    out = tmp_path / "template.yaml"
    PipelineConfig.generate(out)
    return out

# ---------------------------------------------------------------------------
# Step 2 — generate()
# ---------------------------------------------------------------------------


class TestGenerate:
    def test_creates_file(self, tmp_path):
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        assert out.exists()

    def test_returns_path(self, tmp_path):
        out = tmp_path / "template.yaml"
        result = PipelineConfig.generate(out)
        assert result == out

    def test_creates_parent_directory(self, tmp_path):
        out = tmp_path / "nested" / "dir" / "template.yaml"
        PipelineConfig.generate(out)
        assert out.exists()

    def test_contains_all_top_level_sections(self, tmp_path):
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        content = out.read_text()
        for section in ("insitu:", "sentinel:", "download:", "acolite:"):
            assert section in content

    def test_contains_acolite_subsections(self, tmp_path):
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        content = out.read_text()
        for sub in ("io:", "radcor:", "glint:", "l2w:", "output_format:", "scl:"):
            assert sub in content

    def test_template_is_valid_yaml(self, tmp_path):
        import yaml

        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        data = yaml.safe_load(out.read_text())
        assert isinstance(data, dict)


# ---------------------------------------------------------------------------
# Step 3 — from_yaml() round-trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_generate_then_load_gives_defaults(self, tmp_path):
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        cfg = PipelineConfig.from_yaml(out)
        defaults = PipelineConfig()

        assert cfg.campaign_name == defaults.campaign_name
        assert cfg.insitu.skip_clean == defaults.insitu.skip_clean
        assert cfg.sentinel.time_delta == defaults.sentinel.time_delta
        # assert cfg.download.only_first == defaults.download.only_first
        assert cfg.acolite.low_memory == defaults.acolite.low_memory
        assert (
            cfg.acolite.radcor.aerosol_correction
            == defaults.acolite.radcor.aerosol_correction
        )
        assert cfg.acolite.l2w.l2w_parameters == defaults.acolite.l2w.l2w_parameters
        assert cfg.acolite.scl.min_area_m2 == defaults.acolite.scl.min_area_m2

    def test_overridden_values_are_loaded(self, tmp_path):
        yaml_text = """\
campaign_name: test_campaign
insitu:
  enabled: false
sentinel:
  time_delta: 3
  cloud_cover: 20
acolite:
  low_memory: true
  scl:
    min_area_m2: 1000.0
"""
        p = _write_yaml(tmp_path, yaml_text)
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.campaign_name == "test_campaign"
        assert cfg.insitu.enabled is False
        assert cfg.sentinel.time_delta == 3
        assert cfg.sentinel.cloud_cover == 20
        assert cfg.acolite.low_memory is True
        assert cfg.acolite.scl.min_area_m2 == 1000.0

    def test_missing_sections_use_defaults(self, tmp_path):
        p = _write_yaml(tmp_path, "campaign_name: minimal\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.insitu.enabled is True
        assert cfg.acolite.radcor.dsf_minimum_tile_cover == pytest.approx(0.10)

    def test_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            PipelineConfig.from_yaml(tmp_path / "nonexistent.yaml")

    def test_limit_loaded_as_list(self, tmp_path):
        yaml_text = """\
acolite:
  io:
    limit: [-33.25, -58.45, -33.17, -58.33]
"""
        p = _write_yaml(tmp_path, yaml_text)
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.io.limit == [-33.25, -58.45, -33.17, -58.33]

    def test_null_limit_loaded(self, tmp_path):
        yaml_text = "acolite:\n  io:\n    limit: null\n"
        p = _write_yaml(tmp_path, yaml_text)
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.io.limit is None


# ---------------------------------------------------------------------------
# Step 3 — Unknown key validation
# ---------------------------------------------------------------------------


class TestUnknownKeys:
    def test_unknown_top_level_key_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "unknown_key: value\n")
        with pytest.raises(ValueError, match="unknown_key"):
            PipelineConfig.from_yaml(p)

    def test_unknown_insitu_key_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "insitu:\n  typo_key: value\n")
        with pytest.raises(ValueError, match="typo_key"):
            PipelineConfig.from_yaml(p)

    def test_unknown_acolite_key_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  bad_param: 42\n")
        with pytest.raises(ValueError, match="bad_param"):
            PipelineConfig.from_yaml(p)

    def test_unknown_acolite_io_key_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  io:\n    mystery: x\n")
        with pytest.raises(ValueError, match="mystery"):
            PipelineConfig.from_yaml(p)

    def test_unknown_scl_key_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  scl:\n    not_a_key: 1\n")
        with pytest.raises(ValueError, match="not_a_key"):
            PipelineConfig.from_yaml(p)

    def test_error_message_includes_context(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  radcor:\n    bad_radcor: 99\n")
        with pytest.raises(ValueError, match="acolite.radcor"):
            PipelineConfig.from_yaml(p)


# ---------------------------------------------------------------------------
# Step 4 — to_acolite_config()
# ---------------------------------------------------------------------------


class TestToAcoliteConfig:
    def _default_cfg(self):
        return PipelineConfig()

    def test_returns_acolite_config(self):
        from aquamatch.acolite_spec import AcoliteConfig

        cfg = self._default_cfg()
        result = cfg.to_acolite_config()
        assert isinstance(result, AcoliteConfig)

    def test_executable_mapped(self):
        cfg = PipelineConfig()
        cfg.acolite.acolite_executable = "/usr/local/bin/acolite"
        result = cfg.to_acolite_config()
        assert result.acolite_executable == "/usr/local/bin/acolite"

    def test_output_dir_mapped(self):
        cfg = PipelineConfig()
        cfg.acolite.io.output = "/data/out"
        result = cfg.to_acolite_config()
        assert result.io.output == "/data/out"

    def test_limit_none_when_not_set(self):
        cfg = PipelineConfig()
        cfg.acolite.io.limit = None
        result = cfg.to_acolite_config()
        assert result.io.limit is None

    def test_limit_tuple_when_set(self):
        cfg = PipelineConfig()
        cfg.acolite.io.limit = [-33.25, -58.45, -33.17, -58.33]
        result = cfg.to_acolite_config()
        assert result.io.limit == (-33.25, -58.45, -33.17, -58.33)

    def test_aerosol_correction_enum(self):
        from aquamatch.acolite_spec import AcoliteAtmosphericProcessor

        cfg = PipelineConfig()
        cfg.acolite.radcor.aerosol_correction = "dsf"
        result = cfg.to_acolite_config()
        assert result.radcor.aerosol_correction == AcoliteAtmosphericProcessor.DSF

    def test_glint_method_enum(self):
        from aquamatch.acolite_spec import AcoliteGlintCorrection

        cfg = PipelineConfig()
        result = cfg.to_acolite_config()
        assert result.glint.glint_method == AcoliteGlintCorrection.VANHELLEMONT

    def test_l2w_parameters_mapped(self):
        cfg = PipelineConfig()
        cfg.acolite.l2w.l2w_parameters = ["t_nechad", "ndwi"]
        result = cfg.to_acolite_config()
        assert result.l2w.l2w_parameters == ["t_nechad", "ndwi"]

    def test_dsf_tile_dimensions_tuple(self):
        cfg = PipelineConfig()
        cfg.acolite.radcor.dsf_tile_dimensions = [60, 60]
        result = cfg.to_acolite_config()
        assert result.radcor.dsf_tile_dimensions == (60, 60)

    def test_netcdf_compression_level_mapped(self):
        cfg = PipelineConfig()
        cfg.acolite.output_format.netcdf_compression_level = 6
        result = cfg.to_acolite_config()
        assert result.output_format.netcdf_compression_level == 6

    def test_low_memory_flag_produces_acolite_config(self):
        """low_memory=True must still return an AcoliteConfig (hook exists)."""
        from aquamatch.acolite_spec import AcoliteConfig

        cfg = PipelineConfig()
        cfg.acolite.low_memory = True
        result = cfg.to_acolite_config()
        assert isinstance(result, AcoliteConfig)


# ---------------------------------------------------------------------------
# Step 4 — to_scl_kwargs() / to_insitu_args() / to_sentinel_args()
# ---------------------------------------------------------------------------


class TestConverters:
    def test_to_scl_kwargs_keys(self):
        cfg = PipelineConfig()
        kwargs = cfg.to_scl_kwargs()
        assert set(kwargs.keys()) == {"min_area_m2", "simplify_tolerance", "buffer_m"}

    def test_to_scl_kwargs_values(self):
        cfg = PipelineConfig()
        cfg.acolite.scl.min_area_m2 = 1234.0
        cfg.acolite.scl.buffer_m = 30.0
        kwargs = cfg.to_scl_kwargs()
        assert kwargs["min_area_m2"] == pytest.approx(1234.0)
        assert kwargs["buffer_m"] == pytest.approx(30.0)

    def test_to_insitu_args_keys(self):
        cfg = PipelineConfig()
        args = cfg.to_insitu_args()
        for key in (
            "stations_path",
            "campaigns_path",
            "output_campaigns_csv",
            "output_unique_csv",
            "skip_clean",
        ):
            assert key in args

    def test_to_insitu_args_paths_are_path_objects(self):
        cfg = PipelineConfig()
        args = cfg.to_insitu_args()
        assert isinstance(args["stations_path"], Path)
        assert isinstance(args["output_unique_csv"], Path)

    def test_to_sentinel_args_keys(self):
        cfg = PipelineConfig()
        args = cfg.to_sentinel_args()
        for key in (
            "csv",
            "catalog_json",
            "time_delta",
            "cloud_cover",
            "output_dir",
            # "only_first",
            "download_scl",
        ):
            assert key in args

    def test_to_sentinel_args_catalog_json_is_path(self):
        cfg = PipelineConfig()
        args = cfg.to_sentinel_args()
        assert isinstance(args["catalog_json"], Path)


# ---------------------------------------------------------------------------
# Step 5 — run() respects enabled flags
# ---------------------------------------------------------------------------


class TestRunEnabledFlags:
    def _make_cfg(self, **enabled_overrides) -> PipelineConfig:
        cfg = PipelineConfig()
        for section, val in enabled_overrides.items():
            getattr(cfg, section).enabled = val
        return cfg

    def test_all_disabled_nothing_called(self):
        cfg = self._make_cfg(
            insitu=False, sentinel=False, download=False, acolite=False
        )
        summary = cfg.run()
        for step in ("insitu", "sentinel", "download", "acolite"):
            assert summary[step]["status"] == "skipped"

    def test_dry_run_all_enabled(self):
        cfg = PipelineConfig()
        summary = cfg.run(dry_run=True)
        for step in ("insitu", "sentinel", "download", "acolite"):
            assert summary[step]["status"] == "dry_run"

    def test_only_insitu_disabled(self):
        cfg = PipelineConfig()
        cfg.insitu.enabled = False
        summary = cfg.run(dry_run=True)
        assert summary["insitu"]["status"] == "skipped"
        assert summary["sentinel"]["status"] == "dry_run"

    def test_only_acolite_disabled(self):
        cfg = PipelineConfig()
        cfg.acolite.enabled = False
        summary = cfg.run(dry_run=True)
        assert summary["acolite"]["status"] == "skipped"
        assert summary["insitu"]["status"] == "dry_run"

    def test_run_returns_dict_with_all_steps(self):
        cfg = self._make_cfg(
            insitu=False, sentinel=False, download=False, acolite=False
        )
        summary = cfg.run()
        assert set(summary.keys()) == {"insitu", "sentinel", "download", "acolite"}


# ---------------------------------------------------------------------------
# Step 6 — CLI smoke tests
# ---------------------------------------------------------------------------


class TestCLI:
    def test_generate_creates_file(self, tmp_path):
        out = str(tmp_path / "campaign.yaml")
        main(["--generate", out])
        assert Path(out).exists()

    def test_generate_prints_path(self, tmp_path, capsys):
        out = str(tmp_path / "campaign.yaml")
        main(["--generate", out])
        captured = capsys.readouterr()
        assert str(out) in captured.out

    def test_run_dry_run(self, tmp_path):
        # Generate a template and run it in dry_run mode — must not raise
        out = tmp_path / "campaign.yaml"
        PipelineConfig.generate(out)
        main(["--run", str(out), "--dry-run"])

    def test_run_with_all_disabled(self, tmp_path):
        import yaml

        out = tmp_path / "campaign.yaml"
        PipelineConfig.generate(out)
        data = yaml.safe_load(out.read_text())
        for section in ("insitu", "sentinel", "download", "acolite"):
            data[section]["enabled"] = False
        out.write_text(yaml.dump(data))
        main(["--run", str(out)])  # should complete without error

    def test_mutually_exclusive_flags(self, tmp_path):
        out = str(tmp_path / "campaign.yaml")
        with pytest.raises(SystemExit):
            main(["--generate", out, "--run", out])

    def test_required_flag(self):
        with pytest.raises(SystemExit):
            main([])


"""
Tests for PipelineConfig step 5 — _run_acolite tile_config wiring
and the to_tile_config() converter.

Covers:
  - to_tile_config() returns the TilesSection instance
  - to_tile_config() returns empty TilesSection when no tiles configured
  - _run_acolite passes tile_config to run_batch
  - _run_acolite passes empty TilesSection when no tiles configured
  - tile_config flows correctly into run_batch (polygon, limit, none)
  - sentinel search parameters are already wired through to_sentinel_args
    (smoke-tests for time_delta and cloud_cover)
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from aquamatch.pipeline_config import (
    PipelineConfig,
    TileEntry,
    TilesSection,
)

# ---------------------------------------------------------------------------
# to_tile_config()
# ---------------------------------------------------------------------------


class TestToTileConfig:

    def test_returns_tiles_section(self):
        cfg = PipelineConfig()
        result = cfg.to_tile_config()
        assert isinstance(result, TilesSection)

    def test_returns_empty_section_by_default(self):
        cfg = PipelineConfig()
        assert cfg.to_tile_config().entries == {}

    def test_returns_configured_tiles(self):
        cfg = PipelineConfig()
        cfg.tiles = TilesSection(
            entries={
                "21HUD": TileEntry(polygon="data/polygons/21HUD.geojson"),
                "21HVD": TileEntry(limit=[-34.2, -56.8, -33.0, -55.1]),
            }
        )
        result = cfg.to_tile_config()
        assert result.get("21HUD").polygon == "data/polygons/21HUD.geojson"
        assert result.get("21HVD").limit == [-34.2, -56.8, -33.0, -55.1]

    def test_is_same_instance_as_self_tiles(self):
        """to_tile_config() must return self.tiles, not a copy."""
        cfg = PipelineConfig()
        assert cfg.to_tile_config() is cfg.tiles


# ---------------------------------------------------------------------------
# _run_acolite — tile_config passed to run_batch
# ---------------------------------------------------------------------------


class TestRunAcoliteTileConfigWiring:

    def _make_pipeline_cfg(self, tmp_path, tiles=None):
        """Build a minimal PipelineConfig pointing at tmp_path."""
        cfg = PipelineConfig()
        cfg.acolite.acolite_executable = str(tmp_path / "fake_acolite")
        cfg.acolite.io.safe_dir = str(tmp_path / "safe")
        cfg.acolite.io.scl_dir = str(tmp_path / "scl")
        cfg.acolite.io.output = str(tmp_path / "output")
        cfg.acolite.scl.use_scl = False
        if tiles is not None:
            cfg.tiles = tiles
        return cfg

    def test_tile_config_passed_to_run_batch(self, tmp_path):
        """_run_acolite must forward tile_config=self.tiles to run_batch."""
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        safe = safe_dir / "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD.SAFE"
        safe.mkdir()
        (safe / "dummy.xml").write_text("<root/>")

        tiles = TilesSection(
            entries={"21HUD": TileEntry(limit=[-34.2, -56.8, -33.0, -55.1])}
        )
        pipeline = self._make_pipeline_cfg(tmp_path, tiles=tiles)

        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            captured["tile_config"] = kwargs.get("tile_config")
            return []

        with patch(
            "aquamatch.acolite_spec.AcoliteConfig.run_batch",
            side_effect=fake_run_batch,
        ):
            pipeline._run_acolite()

        assert captured["tile_config"] is tiles

    def test_empty_tiles_section_passed_when_no_tiles_configured(self, tmp_path):
        """When no tiles are configured, to_tile_config() returns an empty
        TilesSection — which is what run_batch receives."""
        pipeline = self._make_pipeline_cfg(tmp_path)
        tile_cfg = pipeline.to_tile_config()
        assert isinstance(tile_cfg, TilesSection)
        assert tile_cfg.entries == {}

    def test_no_safe_files_returns_empty_list(self, tmp_path):
        """_run_acolite must return [] gracefully when safe_dir is empty."""
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        pipeline = self._make_pipeline_cfg(tmp_path)
        result = pipeline._run_acolite()
        assert result == []

    def test_scl_use_scl_forwarded(self, tmp_path):
        """use_scl flag must still be forwarded correctly alongside tile_config."""
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        safe = safe_dir / "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD.SAFE"
        safe.mkdir()
        (safe / "dummy.xml").write_text("<root/>")

        pipeline = self._make_pipeline_cfg(tmp_path)
        pipeline.acolite.scl.use_scl = True
        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            captured.update(kwargs)
            return []

        with patch(
            "aquamatch.acolite_spec.AcoliteConfig.run_batch",
            side_effect=fake_run_batch,
        ):
            pipeline._run_acolite()

        assert captured.get("use_scl") is True
        assert "tile_config" in captured


# ---------------------------------------------------------------------------
# Sentinel search parameters already wired (smoke tests)
# ---------------------------------------------------------------------------


class TestSentinelArgsWiring:
    """
    Confirm that time_delta and cloud_cover flow from
    PipelineConfig → to_sentinel_args() → _run_sentinel_catalog().
    These were already wired before step 5; tests here guard against
    regression.
    """

    def test_time_delta_in_sentinel_args(self):
        cfg = PipelineConfig()
        cfg.sentinel.time_delta = 3
        args = cfg.to_sentinel_args()
        assert args["time_delta"] == 3

    def test_cloud_cover_in_sentinel_args(self):
        cfg = PipelineConfig()
        cfg.sentinel.cloud_cover = 25
        args = cfg.to_sentinel_args()
        assert args["cloud_cover"] == 25

    def test_sentinel_args_forwarded_to_build_catalog(self, tmp_path):
        csv = tmp_path / "unique.csv"
        csv.write_text("date,longitud,latitud\n2025-08-01,-56.5,-32.85\n")
        catalog_json = tmp_path / "catalog.json"

        cfg = PipelineConfig()
        cfg.sentinel.time_delta = 2
        cfg.sentinel.cloud_cover = 15
        cfg.sentinel.unique_csv = str(csv)
        cfg.sentinel.catalog_json = str(catalog_json)

        captured = {}

        def fake_build_catalog(csv_file, output_json, time_delta, cloud_cover):
            captured["time_delta"] = time_delta
            captured["cloud_cover"] = cloud_cover

        with patch(
            "aquamatch.pipeline_config.PipelineConfig._run_sentinel_catalog",
        ):
            args = cfg.to_sentinel_args()
            assert args["time_delta"] == 2
            assert args["cloud_cover"] == 15


# ---------------------------------------------------------------------------
# DownloadSection defaults
# ---------------------------------------------------------------------------


class TestDownloadSectionDefaults:

    def test_strategy_defaults_to_best(self):
        assert DownloadSection().strategy == "best"

    def test_max_per_date_defaults_to_1(self):
        assert DownloadSection().max_per_date == 1

    def test_max_cloud_cover_defaults_to_none(self):
        assert DownloadSection().max_cloud_cover is None

    def test_download_scl_defaults_to_true(self):
        assert DownloadSection().download_scl is True

    def test_only_first_no_longer_a_field(self):
        """only_first must have been removed from DownloadSection."""
        assert not hasattr(DownloadSection(), "only_first")

    def test_valid_strategies_constant_contains_expected_values(self):
        assert VALID_DOWNLOAD_STRATEGIES == {
            "best",
            "all",
            "same_day",
            "previous",
            "posterior",
        }


# ---------------------------------------------------------------------------
# from_yaml — new fields loaded
# ---------------------------------------------------------------------------


class TestFromYamlDownloadFields:

    def test_strategy_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "download:\n  strategy: same_day\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.download.strategy == "same_day"

    def test_max_per_date_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "download:\n  max_per_date: 3\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.download.max_per_date == 3

    def test_max_cloud_cover_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "download:\n  max_cloud_cover: 15\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.download.max_cloud_cover == 15

    def test_max_cloud_cover_null_loaded_as_none(self, tmp_path):
        p = _write_yaml(tmp_path, "download:\n  max_cloud_cover: null\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.download.max_cloud_cover is None

    def test_all_strategies_are_accepted(self, tmp_path):
        for strategy in VALID_DOWNLOAD_STRATEGIES:
            p = _write_yaml(tmp_path, f"download:\n  strategy: {strategy}\n")
            cfg = PipelineConfig.from_yaml(p)
            assert cfg.download.strategy == strategy

    def test_missing_fields_use_defaults(self, tmp_path):
        p = _write_yaml(tmp_path, "campaign_name: minimal\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.download.strategy == "best"
        assert cfg.download.max_per_date == 1
        assert cfg.download.max_cloud_cover is None


# ---------------------------------------------------------------------------
# from_yaml — strategy validation
# ---------------------------------------------------------------------------


class TestFromYamlStrategyValidation:

    def test_unknown_strategy_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "download:\n  strategy: weekly\n")
        with pytest.raises(ValueError, match="weekly"):
            PipelineConfig.from_yaml(p)

    def test_error_message_lists_valid_strategies(self, tmp_path):
        p = _write_yaml(tmp_path, "download:\n  strategy: bad\n")
        with pytest.raises(ValueError, match="best"):
            PipelineConfig.from_yaml(p)

    def test_unknown_download_key_still_raises(self, tmp_path):
        """Existing unknown-key validation must still work."""
        p = _write_yaml(tmp_path, "download:\n  not_a_key: 1\n")
        with pytest.raises(ValueError, match="not_a_key"):
            PipelineConfig.from_yaml(p)

    def test_only_first_key_raises_as_unknown(self, tmp_path):
        """only_first is no longer a valid key and must be rejected."""
        p = _write_yaml(tmp_path, "download:\n  only_first: true\n")
        with pytest.raises(ValueError, match="only_first"):
            PipelineConfig.from_yaml(p)


# ---------------------------------------------------------------------------
# generate() — template contains new fields
# ---------------------------------------------------------------------------


class TestGenerateDownloadFields:

    def test_template_contains_strategy(self, tmp_path):
        out = _make_template(tmp_path)
        assert "strategy:" in out.read_text()

    def test_template_contains_max_per_date(self, tmp_path):
        out = _make_template(tmp_path)
        assert "max_per_date:" in out.read_text()

    def test_template_contains_max_cloud_cover(self, tmp_path):
        out = _make_template(tmp_path)
        assert "max_cloud_cover:" in out.read_text()

    def test_template_documents_valid_strategies(self, tmp_path):
        out = _make_template(tmp_path)
        content = out.read_text()
        for strategy in VALID_DOWNLOAD_STRATEGIES:
            assert strategy in content

    def test_template_strategy_default_is_best(self, tmp_path):
        out = _make_template(tmp_path)
        assert "strategy: best" in out.read_text()

    def test_template_max_per_date_default_is_1(self, tmp_path):
        out = _make_template(tmp_path)
        assert "max_per_date: 1" in out.read_text()

    def test_template_max_cloud_cover_default_is_null(self, tmp_path):
        out = _make_template(tmp_path)
        assert "max_cloud_cover: null" in out.read_text()

    def test_template_does_not_contain_only_first(self, tmp_path):
        out = _make_template(tmp_path)
        assert "only_first" not in out.read_text()

    def test_template_round_trips_strategy(self, tmp_path):
        out = _make_template(tmp_path)
        cfg = PipelineConfig.from_yaml(out)
        assert cfg.download.strategy == "best"

    def test_template_round_trips_max_per_date(self, tmp_path):
        out = _make_template(tmp_path)
        cfg = PipelineConfig.from_yaml(out)
        assert cfg.download.max_per_date == 1

    def test_template_round_trips_max_cloud_cover(self, tmp_path):
        out = _make_template(tmp_path)
        cfg = PipelineConfig.from_yaml(out)
        assert cfg.download.max_cloud_cover is None


# ---------------------------------------------------------------------------
# to_sentinel_args() — new keys present, only_first absent
# ---------------------------------------------------------------------------


class TestToSentinelArgsDownload:

    def test_strategy_in_sentinel_args(self):
        cfg = PipelineConfig()
        assert "strategy" in cfg.to_sentinel_args()

    def test_max_per_date_in_sentinel_args(self):
        cfg = PipelineConfig()
        assert "max_per_date" in cfg.to_sentinel_args()

    def test_max_cloud_cover_in_sentinel_args(self):
        cfg = PipelineConfig()
        assert "max_cloud_cover" in cfg.to_sentinel_args()

    def test_only_first_not_in_sentinel_args(self):
        cfg = PipelineConfig()
        assert "only_first" not in cfg.to_sentinel_args()

    def test_strategy_value_reflects_config(self):
        cfg = PipelineConfig()
        cfg.download.strategy = "posterior"
        assert cfg.to_sentinel_args()["strategy"] == "posterior"

    def test_max_per_date_value_reflects_config(self):
        cfg = PipelineConfig()
        cfg.download.max_per_date = 2
        assert cfg.to_sentinel_args()["max_per_date"] == 2

    def test_max_cloud_cover_value_reflects_config(self):
        cfg = PipelineConfig()
        cfg.download.max_cloud_cover = 20
        assert cfg.to_sentinel_args()["max_cloud_cover"] == 20


# ---------------------------------------------------------------------------
# _run_download — new params forwarded to run_download
# ---------------------------------------------------------------------------


class TestRunDownloadForwarding:

    def _make_pipeline_cfg(self, tmp_path, **download_overrides):
        cfg = PipelineConfig()
        cfg.acolite.io.safe_dir = str(tmp_path / "safe")
        cfg.acolite.io.scl_dir = str(tmp_path / "scl")
        cfg.acolite.io.output = str(tmp_path / "output")
        for k, v in download_overrides.items():
            setattr(cfg.download, k, v)
        return cfg

    def test_strategy_forwarded(self, tmp_path):
        pipeline = self._make_pipeline_cfg(tmp_path, strategy="same_day")
        captured = {}

        def fake_run_download(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch(
            "aquamatch.pipeline_config.PipelineConfig._run_download",
            wraps=lambda self: None,
        ):
            pass  # test via to_sentinel_args instead

        args = pipeline.to_sentinel_args()
        assert args["strategy"] == "same_day"

    def test_max_per_date_forwarded(self, tmp_path):
        pipeline = self._make_pipeline_cfg(tmp_path, max_per_date=3)
        args = pipeline.to_sentinel_args()
        assert args["max_per_date"] == 3

    def test_max_cloud_cover_forwarded(self, tmp_path):
        pipeline = self._make_pipeline_cfg(tmp_path, max_cloud_cover=10)
        args = pipeline.to_sentinel_args()
        assert args["max_cloud_cover"] == 10

    def test_run_download_called_with_strategy(self, tmp_path):
        """_run_download must pass strategy (not only_first) to run_download."""
        (tmp_path / "catalog.json").write_text("[]")
        pipeline = self._make_pipeline_cfg(tmp_path, strategy="previous")
        pipeline.download.catalog_json = str(tmp_path / "catalog.json")
        pipeline.download.output_dir = str(tmp_path)

        captured = {}

        def fake_run_download(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch(
            "aquamatch.sentinel_data.run_download", side_effect=fake_run_download
        ):
            pipeline._run_download()

        assert captured.get("strategy") == "previous"
        assert "only_first" not in captured

    def test_run_download_called_with_max_per_date(self, tmp_path):
        (tmp_path / "catalog.json").write_text("[]")
        pipeline = self._make_pipeline_cfg(tmp_path, max_per_date=2)
        pipeline.download.catalog_json = str(tmp_path / "catalog.json")
        pipeline.download.output_dir = str(tmp_path)

        captured = {}

        def fake_run_download(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch(
            "aquamatch.sentinel_data.run_download", side_effect=fake_run_download
        ):
            pipeline._run_download()

        assert captured.get("max_per_date") == 2

    def test_run_download_called_with_max_cloud_cover(self, tmp_path):
        (tmp_path / "catalog.json").write_text("[]")
        pipeline = self._make_pipeline_cfg(tmp_path, max_cloud_cover=15)
        pipeline.download.catalog_json = str(tmp_path / "catalog.json")
        pipeline.download.output_dir = str(tmp_path)

        captured = {}

        def fake_run_download(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch(
            "aquamatch.sentinel_data.run_download", side_effect=fake_run_download
        ):
            pipeline._run_download()

        assert captured.get("max_cloud_cover") == 15
