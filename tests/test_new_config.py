"""
Tests for the new ACOLITE configuration dataclasses and their integration.

Covers:
  - S2Config, DsfConfig, ReprojectConfig — construction, defaults, validate()
  - L2WConfig and OutputConfig extended fields
  - AcoliteConfig — new sub-configs present, low_memory() updated
  - to_settings_dict() — all new keys serialised correctly
  - AcoliteSection, AcoliteDsfSection, AcoliteS2Section, AcoliteReprojectSection
  - PipelineConfig.from_yaml() — new sections load correctly
  - PipelineConfig.generate() — template contains new sections
  - PipelineConfig.to_acolite_config() — new fields mapped correctly
  - Backward compatibility — existing defaults unchanged
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aquamatch.acolite_spec import (
    AcoliteConfig,
    IOConfig,
    L2WConfig,
    OutputConfig,
    S2Config,
    DsfConfig,
    ReprojectConfig,
    RadCorConfig,
)
from aquamatch.pipeline_config import (
    AcoliteSection,
    AcoliteS2Section,
    AcoliteDsfSection,
    AcoliteReprojectSection,
    AcoliteL2WSection,
    AcoliteOutputSection,
    PipelineConfig,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(content)
    return p


def _make_cfg(tmp_path: Path) -> AcoliteConfig:
    return AcoliteConfig(
        acolite_executable=str(tmp_path / "fake_acolite"),
        io=IOConfig(inputfile="", output=str(tmp_path)),
    )


# ===========================================================================
# S2Config
# ===========================================================================


class TestS2ConfigDefaults:

    def test_s2_target_res_default(self):
        assert S2Config().s2_target_res == 10

    def test_merge_tiles_default(self):
        assert S2Config().merge_tiles is False

    def test_blackfill_skip_default(self):
        assert S2Config().blackfill_skip is True

    def test_blackfill_max_default(self):
        assert S2Config().blackfill_max == pytest.approx(1.0)

    def test_blackfill_wave_default(self):
        assert S2Config().blackfill_wave == 1600

    def test_geometry_type_default(self):
        assert S2Config().geometry_type == "grids_footprint"

    def test_geometry_res_default(self):
        assert S2Config().geometry_res == 60


class TestS2ConfigValidate:

    def test_valid_res_10_passes(self):
        S2Config(s2_target_res=10).validate()

    def test_valid_res_20_passes(self):
        S2Config(s2_target_res=20).validate()

    def test_valid_res_60_passes(self):
        S2Config(s2_target_res=60).validate()

    def test_invalid_res_raises(self):
        with pytest.raises(ValueError, match="s2_target_res"):
            S2Config(s2_target_res=15).validate()

    def test_blackfill_max_out_of_range_raises(self):
        with pytest.raises(ValueError, match="blackfill_max"):
            S2Config(blackfill_max=1.5).validate()

    def test_blackfill_max_zero_passes(self):
        S2Config(blackfill_max=0.0).validate()

    def test_blackfill_max_one_passes(self):
        S2Config(blackfill_max=1.0).validate()


# ===========================================================================
# DsfConfig
# ===========================================================================


class TestDsfConfigDefaults:

    def test_dsf_aot_estimate_default(self):
        assert DsfConfig().dsf_aot_estimate == "tiled"

    def test_dsf_spectrum_option_default(self):
        assert DsfConfig().dsf_spectrum_option == "intercept"

    def test_dsf_nbands_default(self):
        assert DsfConfig().dsf_nbands == 2

    def test_dsf_fixed_aot_default(self):
        assert DsfConfig().dsf_fixed_aot is None

    def test_dsf_smooth_aot_default(self):
        assert DsfConfig().dsf_smooth_aot is False

    def test_dsf_filter_rhot_default(self):
        assert DsfConfig().dsf_filter_rhot is False

    def test_dsf_aot_most_common_model_default(self):
        assert DsfConfig().dsf_aot_most_common_model is True

    def test_dsf_min_tile_aot_default(self):
        assert DsfConfig().dsf_min_tile_aot == pytest.approx(0.01)

    def test_dsf_max_tile_aot_default(self):
        assert DsfConfig().dsf_max_tile_aot == pytest.approx(1.20)


class TestDsfConfigValidate:

    def test_valid_defaults_pass(self):
        DsfConfig().validate()

    def test_valid_darkest_spectrum_passes(self):
        DsfConfig(dsf_spectrum_option="darkest").validate()

    def test_valid_percentile_spectrum_passes(self):
        DsfConfig(dsf_spectrum_option="percentile").validate()

    def test_invalid_aot_estimate_raises(self):
        with pytest.raises(ValueError, match="dsf_aot_estimate"):
            DsfConfig(dsf_aot_estimate="magic").validate()

    def test_invalid_spectrum_option_raises(self):
        with pytest.raises(ValueError, match="dsf_spectrum_option"):
            DsfConfig(dsf_spectrum_option="unknown").validate()

    def test_fixed_aot_valid_range_passes(self):
        DsfConfig(dsf_fixed_aot=0.5).validate()

    def test_fixed_aot_zero_passes(self):
        DsfConfig(dsf_fixed_aot=0.0).validate()

    def test_fixed_aot_out_of_range_raises(self):
        with pytest.raises(ValueError, match="dsf_fixed_aot"):
            DsfConfig(dsf_fixed_aot=10.0).validate()

    def test_fixed_aot_none_always_passes(self):
        DsfConfig(dsf_fixed_aot=None).validate()

    def test_fixed_band_aot_estimate_passes(self):
        DsfConfig(dsf_aot_estimate="fixed_band").validate()


# ===========================================================================
# ReprojectConfig
# ===========================================================================


class TestReprojectConfigDefaults:

    def test_reproject_outputs_default_false(self):
        assert ReprojectConfig().reproject_outputs is False

    def test_epsg_default_none(self):
        assert ReprojectConfig().output_projection_epsg is None

    def test_resolution_default_none(self):
        assert ReprojectConfig().output_projection_resolution is None

    def test_resampling_method_default(self):
        assert ReprojectConfig().output_projection_resampling_method == "bilinear"


class TestReprojectConfigValidate:

    def test_disabled_always_passes(self):
        ReprojectConfig(reproject_outputs=False).validate()

    def test_enabled_without_epsg_raises(self):
        with pytest.raises(ValueError, match="output_projection_epsg"):
            ReprojectConfig(reproject_outputs=True).validate()

    def test_enabled_with_epsg_passes(self):
        ReprojectConfig(reproject_outputs=True, output_projection_epsg=32721).validate()

    def test_invalid_resampling_method_raises(self):
        with pytest.raises(ValueError, match="output_projection_resampling_method"):
            ReprojectConfig(
                reproject_outputs=True,
                output_projection_epsg=32721,
                output_projection_resampling_method="lanczos",
            ).validate()

    def test_nearest_resampling_passes(self):
        ReprojectConfig(
            reproject_outputs=True,
            output_projection_epsg=32721,
            output_projection_resampling_method="nearest",
        ).validate()

    def test_cubic_resampling_passes(self):
        ReprojectConfig(
            reproject_outputs=True,
            output_projection_epsg=32721,
            output_projection_resampling_method="cubic",
        ).validate()

    def test_negative_resolution_raises(self):
        with pytest.raises(ValueError, match="output_projection_resolution"):
            ReprojectConfig(
                reproject_outputs=True,
                output_projection_epsg=32721,
                output_projection_resolution=-10.0,
            ).validate()

    def test_valid_resolution_passes(self):
        ReprojectConfig(
            reproject_outputs=True,
            output_projection_epsg=32721,
            output_projection_resolution=10.0,
        ).validate()


# ===========================================================================
# L2WConfig — extended fields
# ===========================================================================


class TestL2WConfigExtendedDefaults:

    def test_l2w_mask_wave_default(self):
        assert L2WConfig().l2w_mask_wave == 1600

    def test_l2w_mask_threshold_default(self):
        assert L2WConfig().l2w_mask_threshold == pytest.approx(0.0215)

    def test_l2w_mask_cirrus_threshold_default(self):
        assert L2WConfig().l2w_mask_cirrus_threshold == pytest.approx(0.005)

    def test_l2w_mask_smooth_default(self):
        assert L2WConfig().l2w_mask_smooth is True

    def test_l2w_mask_smooth_sigma_default(self):
        assert L2WConfig().l2w_mask_smooth_sigma == 3


# ===========================================================================
# OutputConfig — extended fields
# ===========================================================================


class TestOutputConfigExtendedDefaults:

    def test_output_xy_default_false(self):
        assert OutputConfig().output_xy is False

    def test_output_geometry_default_true(self):
        assert OutputConfig().output_geometry is True

    def test_l2w_export_geotiff_default_false(self):
        assert OutputConfig().l2w_export_geotiff is False

    def test_copy_datasets_default(self):
        assert OutputConfig().copy_datasets == "lon,lat,rhot_*"


# ===========================================================================
# AcoliteConfig — new sub-configs present
# ===========================================================================


class TestAcoliteConfigNewSubConfigs:

    def test_s2_attribute_present(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        assert hasattr(cfg, "s2")
        assert isinstance(cfg.s2, S2Config)

    def test_dsf_attribute_present(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        assert hasattr(cfg, "dsf")
        assert isinstance(cfg.dsf, DsfConfig)

    def test_reproject_attribute_present(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        assert hasattr(cfg, "reproject")
        assert isinstance(cfg.reproject, ReprojectConfig)

    def test_s2_defaults_unchanged(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        assert cfg.s2.s2_target_res == 10
        assert cfg.s2.merge_tiles is False

    def test_dsf_defaults_unchanged(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        assert cfg.dsf.dsf_aot_estimate == "tiled"
        assert cfg.dsf.dsf_fixed_aot is None

    def test_reproject_defaults_unchanged(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        assert cfg.reproject.reproject_outputs is False


# ===========================================================================
# to_settings_dict() — new keys serialised
# ===========================================================================


class TestToSettingsDictNewKeys:

    def _settings(self, tmp_path, **overrides) -> dict:
        cfg = AcoliteConfig(
            acolite_executable=str(tmp_path / "acolite"),
            io=IOConfig(inputfile="", output=str(tmp_path)),
            **overrides,
        )
        return cfg.to_settings_dict()

    # S2 keys
    def test_s2_target_res_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["s2_target_res"] == "10"

    def test_merge_tiles_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["merge_tiles"] == "false"

    def test_blackfill_skip_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["blackfill_skip"] == "true"

    def test_blackfill_wave_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["blackfill_wave"] == "1600"

    def test_geometry_type_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["geometry_type"] == "grids_footprint"

    # DSF keys
    def test_dsf_aot_estimate_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["dsf_aot_estimate"] == "tiled"

    def test_dsf_spectrum_option_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["dsf_spectrum_option"] == "intercept"

    def test_dsf_nbands_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["dsf_nbands"] == "2"

    def test_dsf_smooth_aot_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["dsf_smooth_aot"] == "false"

    def test_dsf_fixed_aot_absent_when_none(self, tmp_path):
        s = self._settings(tmp_path)
        assert "dsf_fixed_aot" not in s

    def test_dsf_fixed_aot_present_when_set(self, tmp_path):
        s = self._settings(tmp_path, dsf=DsfConfig(dsf_fixed_aot=0.15))
        assert s["dsf_fixed_aot"] == "0.15"

    def test_dsf_filter_percentile_absent_when_filter_disabled(self, tmp_path):
        s = self._settings(tmp_path)
        assert "dsf_filter_percentile" not in s

    def test_dsf_filter_percentile_present_when_filter_enabled(self, tmp_path):
        s = self._settings(tmp_path, dsf=DsfConfig(dsf_filter_rhot=True))
        assert s["dsf_filter_percentile"] == "50"

    # Reproject keys — absent when disabled
    def test_reproject_outputs_absent_when_disabled(self, tmp_path):
        s = self._settings(tmp_path)
        assert "reproject_outputs" not in s

    def test_reproject_outputs_present_when_enabled(self, tmp_path):
        s = self._settings(
            tmp_path,
            reproject=ReprojectConfig(
                reproject_outputs=True, output_projection_epsg=32721
            ),
        )
        assert s["reproject_outputs"] == "L1R,L2R,L2W"
        assert s["output_projection_epsg"] == "32721"

    def test_reproject_resolution_present_when_set(self, tmp_path):
        s = self._settings(
            tmp_path,
            reproject=ReprojectConfig(
                reproject_outputs=True,
                output_projection_epsg=32721,
                output_projection_resolution=10.0,
            ),
        )
        assert s["output_projection_resolution"] == "10.0"

    def test_reproject_resolution_absent_when_none(self, tmp_path):
        s = self._settings(
            tmp_path,
            reproject=ReprojectConfig(
                reproject_outputs=True, output_projection_epsg=32721
            ),
        )
        assert "output_projection_resolution" not in s

    # Extended L2W keys
    def test_l2w_mask_wave_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["l2w_mask_wave"] == "1600"

    def test_l2w_mask_threshold_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["l2w_mask_threshold"] == "0.0215"

    def test_l2w_mask_cirrus_threshold_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["l2w_mask_cirrus_threshold"] == "0.005"

    def test_l2w_mask_smooth_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["l2w_mask_smooth"] == "true"

    def test_l2w_mask_smooth_sigma_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["l2w_mask_smooth_sigma"] == "3"

    # Extended output keys
    def test_output_xy_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["output_xy"] == "false"

    def test_output_geometry_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["output_geometry"] == "true"

    def test_l2w_export_geotiff_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["l2w_export_geotiff"] == "false"

    def test_copy_datasets_in_settings(self, tmp_path):
        s = self._settings(tmp_path)
        assert s["copy_datasets"] == "lon,lat,rhot_*"


# ===========================================================================
# Backward compatibility — existing keys still present and unchanged
# ===========================================================================


class TestBackwardCompatibility:

    def _settings(self, tmp_path) -> dict:
        return AcoliteConfig(
            acolite_executable=str(tmp_path / "acolite"),
            io=IOConfig(inputfile="", output=str(tmp_path)),
        ).to_settings_dict()

    def test_aerosol_correction_still_present(self, tmp_path):
        assert self._settings(tmp_path)["aerosol_correction"] == "dsf"

    def test_l2w_parameters_still_present(self, tmp_path):
        assert "l2w_parameters" in self._settings(tmp_path)

    def test_glint_correction_still_present(self, tmp_path):
        assert self._settings(tmp_path)["glint_correction"] == "true"

    def test_export_geotiff_still_present(self, tmp_path):
        assert self._settings(tmp_path)["export_geotiff"] == "true"

    def test_netcdf_compression_level_still_present(self, tmp_path):
        assert self._settings(tmp_path)["netcdf_compression_level"] == "4"

    def test_dsf_tile_dimensions_still_present(self, tmp_path):
        assert self._settings(tmp_path)["dsf_tile_dimensions"] == "120,120"

    def test_output_rhorc_still_present(self, tmp_path):
        assert self._settings(tmp_path)["output_rhorc"] == "false"


# ===========================================================================
# low_memory() preset — s2_target_res updated
# ===========================================================================


class TestLowMemoryPreset:

    def test_s2_target_res_is_20(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert cfg.s2.s2_target_res == 20

    def test_s2_target_res_in_settings_dict(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert cfg.to_settings_dict()["s2_target_res"] == "20"

    def test_dsf_tile_dimensions_still_reduced(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert cfg.radcor.dsf_tile_dimensions == (60, 60)

    def test_existing_low_memory_fields_unchanged(self):
        cfg = AcoliteConfig.low_memory(acolite_executable="/fake/acolite")
        assert cfg.output_format.netcdf_compression_level == 2
        assert cfg.output_format.export_cloud_optimized_geotiff is False
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

    def test_s2_subsection_present(self):
        assert hasattr(AcoliteSection(), "s2")
        assert isinstance(AcoliteSection().s2, AcoliteS2Section)

    def test_dsf_subsection_present(self):
        assert hasattr(AcoliteSection(), "dsf")
        assert isinstance(AcoliteSection().dsf, AcoliteDsfSection)

    def test_reproject_subsection_present(self):
        assert hasattr(AcoliteSection(), "reproject")
        assert isinstance(AcoliteSection().reproject, AcoliteReprojectSection)


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
# PipelineConfig.generate() — template contains new sections
# ===========================================================================


class TestGenerateNewSections:

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


# ===========================================================================
# PipelineConfig.to_acolite_config() — new fields mapped
# ===========================================================================


class TestToAcoliteConfigNewFields:

    def _pipeline_cfg(self) -> PipelineConfig:
        return PipelineConfig()

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
