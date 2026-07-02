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

import pytest

from aquamatch.acolite_spec import (
    S2Config,
    DsfConfig,
    ReprojectConfig,
)
from aquamatch.pipeline_config import (
    AcoliteSection,
    AcoliteS2Section,
    AcoliteDsfSection,
    AcoliteReprojectSection,
    AcoliteL2WSection,
    AcoliteOutputSection,
)
from aquamatch.pipeline_config import (
    PipelineConfig,
    TileEntry,
    TilesSection,
    main,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(content)
    return p


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

    def _template(self, tmp_path: Path) -> str:
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        return out.read_text()

    def test_template_contains_s2_section(self, tmp_path):
        assert "s2:" in self._template(tmp_path)

    def test_template_contains_s2_target_res(self, tmp_path):
        assert "s2_target_res" in self._template(tmp_path)

    def test_template_contains_merge_tiles(self, tmp_path):
        assert "merge_tiles" in self._template(tmp_path)

    def test_template_contains_blackfill_skip(self, tmp_path):
        assert "blackfill_skip" in self._template(tmp_path)

    def test_template_contains_dsf_section(self, tmp_path):
        assert "dsf:" in self._template(tmp_path)

    def test_template_contains_dsf_aot_estimate(self, tmp_path):
        assert "dsf_aot_estimate" in self._template(tmp_path)

    def test_template_contains_dsf_spectrum_option(self, tmp_path):
        assert "dsf_spectrum_option" in self._template(tmp_path)

    def test_template_contains_dsf_fixed_aot(self, tmp_path):
        assert "dsf_fixed_aot" in self._template(tmp_path)

    def test_template_contains_reproject_section(self, tmp_path):
        assert "reproject:" in self._template(tmp_path)

    def test_template_contains_reproject_outputs(self, tmp_path):
        assert "reproject_outputs" in self._template(tmp_path)

    def test_template_contains_output_projection_epsg(self, tmp_path):
        assert "output_projection_epsg" in self._template(tmp_path)

    def test_template_contains_l2w_mask_wave(self, tmp_path):
        assert "l2w_mask_wave" in self._template(tmp_path)

    def test_template_contains_output_xy(self, tmp_path):
        assert "output_xy" in self._template(tmp_path)

    def test_template_contains_l2w_export_geotiff(self, tmp_path):
        assert "l2w_export_geotiff" in self._template(tmp_path)

    def test_template_contains_copy_datasets(self, tmp_path):
        assert "copy_datasets" in self._template(tmp_path)

    def test_template_round_trips(self, tmp_path):
        """Generated template must load without errors."""
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        cfg = PipelineConfig.from_yaml(out)
        assert cfg.acolite.s2.s2_target_res == 10
        assert cfg.acolite.dsf.dsf_aot_estimate == "tiled"
        assert cfg.acolite.reproject.reproject_outputs is False


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
        assert cfg.download.strategy == defaults.download.strategy
        assert cfg.download.max_per_date == defaults.download.max_per_date
        assert cfg.download.max_cloud_cover == defaults.download.max_cloud_cover
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
download:
  strategy: same_day
  max_per_date: 2
  max_cloud_cover: 15
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
        assert cfg.download.strategy == "same_day"
        assert cfg.download.max_per_date == 2
        assert cfg.download.max_cloud_cover == 15
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

    def test_unknown_s2_key_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  s2:\n    bad_key: 99\n")
        with pytest.raises(ValueError, match="bad_key"):
            PipelineConfig.from_yaml(p)

    def test_unknown_dsf_key_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  dsf:\n    bad_key: 99\n")
        with pytest.raises(ValueError, match="bad_key"):
            PipelineConfig.from_yaml(p)

    def test_unknown_reproject_key_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  reproject:\n    bad_key: 99\n")
        with pytest.raises(ValueError, match="bad_key"):
            PipelineConfig.from_yaml(p)


# ===========================================================================
# PipelineConfig.from_yaml() — new sections loaded correctly
# ===========================================================================


class TestFromYamlNewSections:

    def test_missing_s2_section_uses_defaults(self, tmp_path):
        p = _write_yaml(tmp_path, "campaign_name: minimal\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.s2.s2_target_res == 10

    def test_missing_dsf_section_uses_defaults(self, tmp_path):
        p = _write_yaml(tmp_path, "campaign_name: minimal\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.dsf.dsf_aot_estimate == "tiled"

    def test_missing_reproject_section_uses_defaults(self, tmp_path):
        p = _write_yaml(tmp_path, "campaign_name: minimal\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.reproject.reproject_outputs is False

    def test_s2_target_res_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  s2:\n    s2_target_res: 20\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.s2.s2_target_res == 20

    def test_merge_tiles_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  s2:\n    merge_tiles: true\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.s2.merge_tiles is True

    def test_dsf_fixed_aot_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  dsf:\n    dsf_fixed_aot: 0.1\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.dsf.dsf_fixed_aot == pytest.approx(0.1)

    def test_dsf_spectrum_option_loaded(self, tmp_path):
        p = _write_yaml(
            tmp_path, "acolite:\n  dsf:\n    dsf_spectrum_option: darkest\n"
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.dsf.dsf_spectrum_option == "darkest"

    def test_reproject_epsg_loaded(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            "acolite:\n  reproject:\n    reproject_outputs: true\n    output_projection_epsg: 32721\n",
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.reproject.reproject_outputs is True
        assert cfg.acolite.reproject.output_projection_epsg == 32721

    def test_l2w_mask_wave_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  l2w:\n    l2w_mask_wave: 865\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.l2w.l2w_mask_wave == 865

    def test_output_xy_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  output_format:\n    output_xy: true\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.output_format.output_xy is True

    def test_l2w_export_geotiff_loaded(self, tmp_path):
        p = _write_yaml(
            tmp_path, "acolite:\n  output_format:\n    l2w_export_geotiff: true\n"
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.output_format.l2w_export_geotiff is True


# ---------------------------------------------------------------------------
# Step 4 — to_acolite_config()
# ---------------------------------------------------------------------------


class TestToAcoliteConfig:
    def _default_cfg(self):
        return PipelineConfig()

    def _pipeline_cfg(self) -> PipelineConfig:
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
        from aquamatch.acolite_spec import AcoliteConfig

        cfg = PipelineConfig()
        cfg.acolite.low_memory = True
        result = cfg.to_acolite_config()
        assert isinstance(result, AcoliteConfig)

    # --- New sub-config mapping ---

    def test_s2_config_mapped(self):
        cfg = self._pipeline_cfg().to_acolite_config()
        assert isinstance(cfg.s2, S2Config)
        assert cfg.s2.s2_target_res == 10

    def test_dsf_config_mapped(self):
        cfg = self._pipeline_cfg().to_acolite_config()
        assert isinstance(cfg.dsf, DsfConfig)
        assert cfg.dsf.dsf_aot_estimate == "tiled"

    def test_reproject_config_mapped(self):
        cfg = self._pipeline_cfg().to_acolite_config()
        assert isinstance(cfg.reproject, ReprojectConfig)
        assert cfg.reproject.reproject_outputs is False

    def test_l2w_extended_fields_mapped(self):
        pipeline = PipelineConfig()
        pipeline.acolite.l2w.l2w_mask_wave = 865
        pipeline.acolite.l2w.l2w_mask_threshold = 0.03
        cfg = pipeline.to_acolite_config()
        assert cfg.l2w.l2w_mask_wave == 865
        assert cfg.l2w.l2w_mask_threshold == pytest.approx(0.03)

    def test_output_extended_fields_mapped(self):
        pipeline = PipelineConfig()
        pipeline.acolite.output_format.output_xy = True
        pipeline.acolite.output_format.l2w_export_geotiff = True
        cfg = pipeline.to_acolite_config()
        assert cfg.output_format.output_xy is True
        assert cfg.output_format.l2w_export_geotiff is True

    def test_s2_target_res_propagates_to_settings(self):
        pipeline = PipelineConfig()
        pipeline.acolite.s2.s2_target_res = 20
        cfg = pipeline.to_acolite_config()
        assert cfg.to_settings_dict()["s2_target_res"] == "20"

    def test_dsf_fixed_aot_propagates_to_settings(self):
        pipeline = PipelineConfig()
        pipeline.acolite.dsf.dsf_fixed_aot = 0.08
        cfg = pipeline.to_acolite_config()
        assert cfg.to_settings_dict()["dsf_fixed_aot"] == "0.08"

    def test_reproject_epsg_propagates_to_settings(self):
        pipeline = PipelineConfig()
        pipeline.acolite.reproject.reproject_outputs = True
        pipeline.acolite.reproject.output_projection_epsg = 32721
        cfg = pipeline.to_acolite_config()
        settings = cfg.to_settings_dict()
        assert settings["reproject_outputs"] == "L1R,L2R,L2W"
        assert settings["output_projection_epsg"] == "32721"

    def test_existing_fields_still_mapped(self):
        """Backward-compatibility: existing field mapping must be unchanged."""
        cfg = self._pipeline_cfg().to_acolite_config()
        assert cfg.radcor.dsf_tile_dimensions == (120, 120)
        assert cfg.glint.glint_correction is True
        assert cfg.l2w.output_rhorc is False


# ===========================================================================
# Pipeline config section dataclasses
# ===========================================================================


class TestAcoliteS2SectionDefaults:

    def test_s2_target_res_default(self):
        assert AcoliteS2Section().s2_target_res == 10

    def test_merge_tiles_default(self):
        assert AcoliteS2Section().merge_tiles is False

    def test_blackfill_skip_default(self):
        assert AcoliteS2Section().blackfill_skip is True

    def test_geometry_type_default(self):
        assert AcoliteS2Section().geometry_type == "grids_footprint"


class TestAcoliteDsfSectionDefaults:

    def test_dsf_aot_estimate_default(self):
        assert AcoliteDsfSection().dsf_aot_estimate == "tiled"

    def test_dsf_spectrum_option_default(self):
        assert AcoliteDsfSection().dsf_spectrum_option == "intercept"

    def test_dsf_fixed_aot_default(self):
        assert AcoliteDsfSection().dsf_fixed_aot is None

    def test_dsf_aot_most_common_model_default(self):
        assert AcoliteDsfSection().dsf_aot_most_common_model is True


class TestAcoliteReprojectSectionDefaults:

    def test_reproject_outputs_default(self):
        assert AcoliteReprojectSection().reproject_outputs is False

    def test_epsg_default(self):
        assert AcoliteReprojectSection().output_projection_epsg is None

    def test_resampling_method_default(self):
        assert (
            AcoliteReprojectSection().output_projection_resampling_method == "bilinear"
        )


class TestAcoliteL2WSectionExtendedDefaults:

    def test_l2w_mask_wave_default(self):
        assert AcoliteL2WSection().l2w_mask_wave == 1600

    def test_l2w_mask_threshold_default(self):
        assert AcoliteL2WSection().l2w_mask_threshold == pytest.approx(0.0215)

    def test_l2w_mask_smooth_default(self):
        assert AcoliteL2WSection().l2w_mask_smooth is True


class TestAcoliteOutputSectionExtendedDefaults:

    def test_output_xy_default(self):
        assert AcoliteOutputSection().output_xy is False

    def test_output_geometry_default(self):
        assert AcoliteOutputSection().output_geometry is True

    def test_l2w_export_geotiff_default(self):
        assert AcoliteOutputSection().l2w_export_geotiff is False

    def test_copy_datasets_default(self):
        assert AcoliteOutputSection().copy_datasets == "lon,lat,rhot_*"


class TestAcoliteSectionNewSubSections:
    """Verify that AcoliteSection exposes the three new sub-section attributes."""

    def test_s2_subsection_present(self):
        assert hasattr(AcoliteSection(), "s2")
        assert isinstance(AcoliteSection().s2, AcoliteS2Section)

    def test_dsf_subsection_present(self):
        assert hasattr(AcoliteSection(), "dsf")
        assert isinstance(AcoliteSection().dsf, AcoliteDsfSection)

    def test_reproject_subsection_present(self):
        assert hasattr(AcoliteSection(), "reproject")
        assert isinstance(AcoliteSection().reproject, AcoliteReprojectSection)


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
            "strategy",
            "max_per_date",
            "max_cloud_cover",
            "download_scl",
        ):
            assert key in args

    def test_to_sentinel_args_does_not_contain_only_first(self):
        cfg = PipelineConfig()
        assert "only_first" not in cfg.to_sentinel_args()

    def test_to_sentinel_args_catalog_json_is_path(self):
        cfg = PipelineConfig()
        args = cfg.to_sentinel_args()
        assert isinstance(args["catalog_json"], Path)

    def test_to_sentinel_args_strategy_value(self):
        cfg = PipelineConfig()
        cfg.download.strategy = "posterior"
        assert cfg.to_sentinel_args()["strategy"] == "posterior"

    def test_to_sentinel_args_max_per_date_value(self):
        cfg = PipelineConfig()
        cfg.download.max_per_date = 3
        assert cfg.to_sentinel_args()["max_per_date"] == 3

    def test_to_sentinel_args_max_cloud_cover_value(self):
        cfg = PipelineConfig()
        cfg.download.max_cloud_cover = 20
        assert cfg.to_sentinel_args()["max_cloud_cover"] == 20


# ---------------------------------------------------------------------------
# Step 5 — run() respects enabled flags
# ---------------------------------------------------------------------------


class TestRunEnabledFlags:
    def _make_cfg(self, **enabled_overrides) -> PipelineConfig:
        cfg = PipelineConfig()
        for section, val in enabled_overrides.items():
            if section == "datacube":
                cfg.acolite.datacube.enabled = val
            else:
                getattr(cfg, section).enabled = val
        return cfg

    def test_all_disabled_nothing_called(self):
        cfg = PipelineConfig()
        cfg.insitu.enabled = False
        cfg.sentinel.enabled = False
        cfg.download.enabled = False
        cfg.acolite.enabled = False
        cfg.acolite.datacube.enabled = False
        summary = cfg.run()
        for step in ("insitu", "sentinel", "download", "acolite", "datacube"):
            assert summary[step]["status"] == "skipped"

    def test_dry_run_all_enabled(self):
        cfg = PipelineConfig()
        cfg.acolite.datacube.enabled = True
        summary = cfg.run(dry_run=True)
        for step in ("insitu", "sentinel", "download", "acolite", "datacube"):
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

    def test_datacube_disabled_by_default(self):
        """datacube.enabled defaults to False — step must be skipped."""
        cfg = PipelineConfig()
        summary = cfg.run(dry_run=True)
        assert summary["datacube"]["status"] == "skipped"

    def test_datacube_enabled_runs_in_dry_run(self):
        cfg = PipelineConfig()
        cfg.acolite.datacube.enabled = True
        summary = cfg.run(dry_run=True)
        assert summary["datacube"]["status"] == "dry_run"

    def test_run_returns_dict_with_all_five_steps(self):
        cfg = PipelineConfig()
        cfg.insitu.enabled = False
        cfg.sentinel.enabled = False
        cfg.download.enabled = False
        cfg.acolite.enabled = False
        cfg.acolite.datacube.enabled = False
        summary = cfg.run()
        assert set(summary.keys()) == {
            "insitu",
            "sentinel",
            "download",
            "acolite",
            "datacube",
        }


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
        main(["--run", str(out)])

    def test_mutually_exclusive_flags(self, tmp_path):
        out = str(tmp_path / "campaign.yaml")
        with pytest.raises(SystemExit):
            main(["--generate", out, "--run", out])

    def test_required_flag(self):
        with pytest.raises(SystemExit):
            main([])


# ---------------------------------------------------------------------------
# to_tile_config() and _run_acolite tile_config wiring
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
        cfg = PipelineConfig()
        assert cfg.to_tile_config() is cfg.tiles


class TestRunAcoliteTileConfigWiring:

    def _make_pipeline_cfg(self, tmp_path, tiles=None):
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
        from unittest.mock import patch

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
        pipeline = self._make_pipeline_cfg(tmp_path)
        tile_cfg = pipeline.to_tile_config()
        assert isinstance(tile_cfg, TilesSection)
        assert tile_cfg.entries == {}

    def test_no_safe_files_returns_empty_list(self, tmp_path):
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        pipeline = self._make_pipeline_cfg(tmp_path)
        result = pipeline._run_acolite()
        assert result == []


# ---------------------------------------------------------------------------
# SclSection extended fields — polygon datacube
# ---------------------------------------------------------------------------


class TestSclSectionPolygonDatacube:

    def test_build_polygon_datacube_default_false(self):
        from aquamatch.pipeline_config import SclSection

        assert SclSection().build_polygon_datacube is False

    def test_polygon_datacube_path_default(self):
        from aquamatch.pipeline_config import SclSection

        assert SclSection().polygon_datacube_path == "data/water_polygons.gpkg"

    def test_polygon_datacube_overwrite_default_false(self):
        from aquamatch.pipeline_config import SclSection

        assert SclSection().polygon_datacube_overwrite is False

    def test_build_polygon_datacube_loaded_from_yaml(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            "acolite:\n  scl:\n    build_polygon_datacube: true\n",
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.scl.build_polygon_datacube is True

    def test_polygon_datacube_path_loaded_from_yaml(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            "acolite:\n  scl:\n    polygon_datacube_path: data/my.gpkg\n",
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.scl.polygon_datacube_path == "data/my.gpkg"

    def test_polygon_datacube_overwrite_loaded_from_yaml(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            "acolite:\n  scl:\n    polygon_datacube_overwrite: true\n",
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.scl.polygon_datacube_overwrite is True

    def test_unknown_scl_key_still_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  scl:\n    bad_key: 1\n")
        with pytest.raises(ValueError, match="bad_key"):
            PipelineConfig.from_yaml(p)

    def test_template_contains_build_polygon_datacube(self, tmp_path):
        out = tmp_path / "t.yaml"
        PipelineConfig.generate(out)
        assert "build_polygon_datacube" in out.read_text()

    def test_template_contains_polygon_datacube_path(self, tmp_path):
        out = tmp_path / "t.yaml"
        PipelineConfig.generate(out)
        assert "polygon_datacube_path" in out.read_text()

    def test_scl_reuses_same_area_params_as_polygon_datacube(self):
        """min_area_m2 etc. live only in SclSection — no duplication."""
        from aquamatch.pipeline_config import SclSection

        s = SclSection()
        # Only one copy of each parameter exists
        assert hasattr(s, "min_area_m2")
        assert not hasattr(s, "polygon_datacube_min_area_m2")


# ---------------------------------------------------------------------------
# DatacubeSection — L2W product datacube
# ---------------------------------------------------------------------------


class TestDatacubeSection:

    def test_enabled_default_false(self):
        from aquamatch.pipeline_config import DatacubeSection

        assert DatacubeSection().enabled is False

    def test_output_path_default(self):
        from aquamatch.pipeline_config import DatacubeSection

        assert DatacubeSection().output_path == "data/l2w_datacube.zarr"

    def test_variables_default_none(self):
        from aquamatch.pipeline_config import DatacubeSection

        assert DatacubeSection().variables is None

    def test_target_crs_default(self):
        from aquamatch.pipeline_config import DatacubeSection

        assert DatacubeSection().target_crs == "EPSG:4326"

    def test_target_resolution_default(self):
        from aquamatch.pipeline_config import DatacubeSection

        assert DatacubeSection().target_resolution == pytest.approx(0.0001)

    def test_overwrite_date_default_false(self):
        from aquamatch.pipeline_config import DatacubeSection

        assert DatacubeSection().overwrite_date is False

    def test_zarr_chunks_default(self):
        from aquamatch.pipeline_config import DatacubeSection

        assert DatacubeSection().zarr_chunks == {"time": 1, "y": 512, "x": 512}


class TestAcoliteSectionDatacubeSubSection:

    def test_datacube_subsection_present(self):
        from aquamatch.pipeline_config import AcoliteSection, DatacubeSection

        assert hasattr(AcoliteSection(), "datacube")
        assert isinstance(AcoliteSection().datacube, DatacubeSection)

    def test_datacube_disabled_by_default(self):
        from aquamatch.pipeline_config import AcoliteSection

        assert AcoliteSection().datacube.enabled is False


class TestFromYamlDatacube:

    def test_missing_datacube_section_uses_defaults(self, tmp_path):
        p = _write_yaml(tmp_path, "campaign_name: minimal\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.datacube.enabled is False
        assert cfg.acolite.datacube.variables is None

    def test_datacube_enabled_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  datacube:\n    enabled: true\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.datacube.enabled is True

    def test_datacube_output_path_loaded(self, tmp_path):
        p = _write_yaml(
            tmp_path, "acolite:\n  datacube:\n    output_path: data/cube.zarr\n"
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.datacube.output_path == "data/cube.zarr"

    def test_datacube_variables_list_loaded(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            "acolite:\n  datacube:\n    variables: [t_nechad, spm_nechad, ndwi]\n",
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.datacube.variables == ["t_nechad", "spm_nechad", "ndwi"]

    def test_datacube_variables_null_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  datacube:\n    variables: null\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.datacube.variables is None

    def test_datacube_target_crs_loaded(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            'acolite:\n  datacube:\n    target_crs: "EPSG:32721"\n',
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.datacube.target_crs == "EPSG:32721"

    def test_datacube_overwrite_date_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  datacube:\n    overwrite_date: true\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.datacube.overwrite_date is True

    def test_datacube_zarr_chunks_loaded(self, tmp_path):
        p = _write_yaml(
            tmp_path,
            "acolite:\n  datacube:\n    zarr_chunks:\n      time: 2\n      y: 256\n      x: 256\n",
        )
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.datacube.zarr_chunks == {"time": 2, "y": 256, "x": 256}

    def test_unknown_datacube_key_raises(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  datacube:\n    bad_key: 99\n")
        with pytest.raises(ValueError, match="bad_key"):
            PipelineConfig.from_yaml(p)

    def test_template_round_trips_datacube(self, tmp_path):
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        cfg = PipelineConfig.from_yaml(out)
        assert cfg.acolite.datacube.enabled is False
        assert cfg.acolite.datacube.variables is None
        assert cfg.acolite.datacube.zarr_chunks == {"time": 1, "y": 512, "x": 512}


class TestGenerateDatacube:

    def _template(self, tmp_path: Path) -> str:
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        return out.read_text()

    def test_template_contains_datacube_section(self, tmp_path):
        assert "datacube:" in self._template(tmp_path)

    def test_template_contains_l2w_datacube_path(self, tmp_path):
        assert "l2w_datacube.zarr" in self._template(tmp_path)

    def test_template_contains_variables(self, tmp_path):
        assert "variables:" in self._template(tmp_path)

    def test_template_contains_target_crs(self, tmp_path):
        assert "target_crs" in self._template(tmp_path)

    def test_template_contains_target_resolution(self, tmp_path):
        assert "target_resolution" in self._template(tmp_path)

    def test_template_contains_overwrite_date(self, tmp_path):
        assert "overwrite_date" in self._template(tmp_path)

    def test_template_contains_zarr_chunks(self, tmp_path):
        assert "zarr_chunks" in self._template(tmp_path)

    def test_template_contains_build_polygon_datacube(self, tmp_path):
        assert "build_polygon_datacube" in self._template(tmp_path)

    def test_template_datacube_enabled_is_false(self, tmp_path):
        assert "enabled: false" in self._template(tmp_path)


class TestRunPolygonDatacube:

    def _make_pipeline_cfg(self, tmp_path):
        cfg = PipelineConfig()
        cfg.acolite.acolite_executable = str(tmp_path / "fake_acolite")
        cfg.acolite.io.safe_dir = str(tmp_path / "safe")
        cfg.acolite.io.scl_dir = str(tmp_path / "scl")
        cfg.acolite.io.output = str(tmp_path / "output")
        cfg.acolite.scl.use_scl = False
        cfg.acolite.scl.build_polygon_datacube = True
        cfg.acolite.scl.polygon_datacube_path = str(tmp_path / "water_polygons.gpkg")
        return cfg

    def test_skipped_when_no_scl_files(self, tmp_path):
        cfg = self._make_pipeline_cfg(tmp_path)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()  # empty — no SCL files
        result = cfg._run_polygon_datacube()
        assert result["status"] == "skipped"
        assert "No SCL files" in result["reason"]

    def test_calls_build_water_polygon_datacube(self, tmp_path):
        from unittest.mock import patch, MagicMock

        cfg = self._make_pipeline_cfg(tmp_path)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        # Create fake SCL files
        (scl_dir / "scene1_SCL.tif").write_bytes(b"fake")
        (scl_dir / "scene2_SCL.tif").write_bytes(b"fake")

        captured = {}

        def fake_build(records, output_path, overwrite, **kwargs):
            captured["n_records"] = len(records)
            captured["overwrite"] = overwrite
            captured["kwargs"] = kwargs
            return Path(output_path)

        with patch(
            "aquamatch.pipeline_config.build_water_polygon_datacube",
            side_effect=fake_build,
        ):
            result = cfg._run_polygon_datacube()

        assert captured["n_records"] == 2
        assert captured["overwrite"] is False
        assert "min_area_m2" in captured["kwargs"]
        assert result["status"] == "ok"
        assert result["n_records"] == 2

    def test_forwards_scl_kwargs(self, tmp_path):
        from unittest.mock import patch

        cfg = self._make_pipeline_cfg(tmp_path)
        cfg.acolite.scl.min_area_m2 = 1234.0
        cfg.acolite.scl.simplify_tolerance = 15.0
        cfg.acolite.scl.buffer_m = 30.0
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / "scene1_SCL.tif").write_bytes(b"fake")

        captured_kwargs = {}

        def fake_build(records, output_path, overwrite, **kwargs):
            captured_kwargs.update(kwargs)
            return Path(output_path)

        with patch(
            "aquamatch.pipeline_config.build_water_polygon_datacube",
            side_effect=fake_build,
        ):
            cfg._run_polygon_datacube()

        assert captured_kwargs["min_area_m2"] == pytest.approx(1234.0)
        assert captured_kwargs["simplify_tolerance"] == pytest.approx(15.0)
        assert captured_kwargs["buffer_m"] == pytest.approx(30.0)

    def test_overwrite_forwarded(self, tmp_path):
        from unittest.mock import patch

        cfg = self._make_pipeline_cfg(tmp_path)
        cfg.acolite.scl.polygon_datacube_overwrite = True
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / "scene1_SCL.tif").write_bytes(b"fake")

        captured = {}

        def fake_build(records, output_path, overwrite, **kwargs):
            captured["overwrite"] = overwrite
            return Path(output_path)

        with patch(
            "aquamatch.pipeline_config.build_water_polygon_datacube",
            side_effect=fake_build,
        ):
            cfg._run_polygon_datacube()

        assert captured["overwrite"] is True

    def test_polygon_datacube_in_acolite_result_when_enabled(self, tmp_path):
        from unittest.mock import patch

        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        cfg = self._make_pipeline_cfg(tmp_path)
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()
        (scl_dir / "scene1_SCL.tif").write_bytes(b"fake")

        def fake_build(records, output_path, overwrite, **kwargs):
            return Path(output_path)

        with patch(
            "aquamatch.acolite_spec.AcoliteConfig.run_batch", return_value=[]
        ), patch(
            "aquamatch.pipeline_config.build_water_polygon_datacube",
            side_effect=fake_build,
        ):
            result = cfg._run_acolite()

        assert "polygon_datacube" in result
        assert result["polygon_datacube"]["status"] == "ok"

    def test_polygon_datacube_skipped_in_acolite_result_when_disabled(self, tmp_path):
        from unittest.mock import patch

        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        cfg = self._make_pipeline_cfg(tmp_path)
        cfg.acolite.scl.build_polygon_datacube = False

        with patch("aquamatch.acolite_spec.AcoliteConfig.run_batch", return_value=[]):
            result = cfg._run_acolite()

        assert result["polygon_datacube"]["status"] == "skipped"


class TestRunL2WDatacube:

    def _make_pipeline_cfg(self, tmp_path):
        cfg = PipelineConfig()
        cfg.acolite.io.output = str(tmp_path / "output")
        cfg.acolite.datacube.enabled = True
        cfg.acolite.datacube.output_path = str(tmp_path / "l2w_datacube.zarr")
        return cfg

    def test_skipped_when_no_l2w_files(self, tmp_path):
        cfg = self._make_pipeline_cfg(tmp_path)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        result = cfg._run_l2w_datacube()
        assert result["status"] == "skipped"
        assert "No *_L2W.nc" in result["reason"]

    def test_calls_append_l2w_per_file(self, tmp_path):
        from unittest.mock import patch

        cfg = self._make_pipeline_cfg(tmp_path)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "scene1_L2W.nc").write_bytes(b"fake")
        (output_dir / "scene2_L2W.nc").write_bytes(b"fake")

        call_count = {"n": 0}

        def fake_append(l2w_nc, datacube_path, **kwargs):
            call_count["n"] += 1
            return Path(datacube_path)

        with patch(
            "aquamatch.pipeline_config.append_l2w_to_datacube",
            side_effect=fake_append,
        ):
            result = cfg._run_l2w_datacube()

        assert call_count["n"] == 2
        assert result["status"] == "ok"
        assert result["n_processed"] == 2
        assert result["n_error"] == 0

    def test_forwards_datacube_kwargs(self, tmp_path):
        from unittest.mock import patch

        cfg = self._make_pipeline_cfg(tmp_path)
        cfg.acolite.datacube.target_crs = "EPSG:32721"
        cfg.acolite.datacube.target_resolution = 0.0002
        cfg.acolite.datacube.overwrite_date = True
        cfg.acolite.datacube.zarr_chunks = {"time": 2, "y": 256, "x": 256}

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "scene1_L2W.nc").write_bytes(b"fake")

        captured = {}

        def fake_append(l2w_nc, datacube_path, **kwargs):
            captured.update(kwargs)
            return Path(datacube_path)

        with patch(
            "aquamatch.pipeline_config.append_l2w_to_datacube",
            side_effect=fake_append,
        ):
            cfg._run_l2w_datacube()

        assert captured["target_crs"] == "EPSG:32721"
        assert captured["target_resolution"] == pytest.approx(0.0002)
        assert captured["overwrite_date"] is True
        assert captured["zarr_chunks"] == {"time": 2, "y": 256, "x": 256}

    def test_variables_none_forwarded(self, tmp_path):
        from unittest.mock import patch

        cfg = self._make_pipeline_cfg(tmp_path)
        cfg.acolite.datacube.variables = None
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "scene1_L2W.nc").write_bytes(b"fake")

        captured = {}

        def fake_append(l2w_nc, datacube_path, **kwargs):
            captured["variables"] = kwargs.get("variables")
            return Path(datacube_path)

        with patch(
            "aquamatch.pipeline_config.append_l2w_to_datacube",
            side_effect=fake_append,
        ):
            cfg._run_l2w_datacube()

        assert captured["variables"] is None

    def test_error_in_one_file_continues_processing(self, tmp_path):
        from unittest.mock import patch

        cfg = self._make_pipeline_cfg(tmp_path)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "scene1_L2W.nc").write_bytes(b"fake")
        (output_dir / "scene2_L2W.nc").write_bytes(b"fake")

        call_count = {"n": 0}

        def fake_append(l2w_nc, datacube_path, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("simulated failure")
            return Path(datacube_path)

        with patch(
            "aquamatch.pipeline_config.append_l2w_to_datacube",
            side_effect=fake_append,
        ):
            result = cfg._run_l2w_datacube()

        assert result["n_processed"] == 1
        assert result["n_error"] == 1
        assert result["status"] == "ok"

    def test_warns_on_missing_variable(self, tmp_path, caplog):
        import logging
        from unittest.mock import patch, MagicMock

        cfg = self._make_pipeline_cfg(tmp_path)
        cfg.acolite.datacube.variables = ["t_nechad", "missing_var"]
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "scene1_L2W.nc").write_bytes(b"fake")

        mock_ds = MagicMock()
        mock_ds.data_vars = {"t_nechad": None}  # missing_var not present
        mock_ds.__enter__ = lambda s: mock_ds
        mock_ds.__exit__ = MagicMock(return_value=False)

        def fake_append(l2w_nc, datacube_path, **kwargs):
            return Path(datacube_path)

        with patch(
            "aquamatch.pipeline_config.append_l2w_to_datacube", side_effect=fake_append
        ), patch("xarray.open_dataset", return_value=mock_ds), caplog.at_level(
            logging.WARNING, logger="aquamatch.pipeline_config"
        ):
            cfg._run_l2w_datacube()

        assert any("missing_var" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Sentinel search parameters wiring
# ---------------------------------------------------------------------------


class TestSentinelArgsWiring:

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
