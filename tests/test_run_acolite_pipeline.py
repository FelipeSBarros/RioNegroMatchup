from pathlib import Path
from unittest.mock import patch

import inspect

from aquamatch.acolite_spec import AcoliteConfig, IOConfig, run_acolite_pipeline
from aquamatch.pipeline_config import SclSection, AcoliteIOSection

_SCENE_STEM = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_20230919T094731"
_SAFE_NAME = f"{_SCENE_STEM}.SAFE"


def _make_exe(tmp_path: Path) -> Path:
    exe = tmp_path / "acolite"
    exe.write_text("#!/bin/sh\n")
    exe.chmod(0o755)
    return exe


class TestRunAcolitePipelineStatusDictBaseline:
    """Baseline regression check — untouched by A2."""

    def test_success_status_no_new_params(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
        )
        assert result["status"] == "success"


class TestRunAcolitePipelineDatacubeParamsSignature:
    """A2 — new parameters exist with None defaults."""

    def test_signature_has_new_params(self):
        sig = inspect.signature(run_acolite_pipeline)
        for name in (
            "build_polygon_datacube",
            "polygon_datacube_path",
            "polygon_datacube_overwrite",
        ):
            assert name in sig.parameters
            assert sig.parameters[name].default is None


class TestRunAcolitePipelineDatacubeParamsAccepted:
    """A2 — passing the new params explicitly must not raise or change
    behavior yet (wiring happens in A3+)."""

    def test_explicit_params_do_not_raise(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
            build_polygon_datacube=True,
            polygon_datacube_path=tmp_path / "custom.gpkg",
            polygon_datacube_overwrite=True,
        )
        assert result["status"] == "success"

    def test_none_defaults_do_not_raise(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
        )
        assert result["status"] == "success"


class TestRunAcolitePipelinePolygonDatacubeDisabled:
    """A3 — outputs['polygon_datacube'] is always present; disabled (default) path."""

    def test_key_present_when_not_passed(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
        )
        assert result["status"] == "success"
        assert "polygon_datacube" in result["outputs"]

    def test_skipped_status_and_reason_when_explicitly_false(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
            build_polygon_datacube=False,
        )
        pd = result["outputs"]["polygon_datacube"]
        assert pd["status"] == "skipped"
        assert pd["reason"] == "build_polygon_datacube=False"

    def test_default_none_resolves_to_disabled(self, tmp_path):
        """SclSection.build_polygon_datacube defaults to False."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        assert SclSection().build_polygon_datacube is False

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
            build_polygon_datacube=None,
        )
        pd = result["outputs"]["polygon_datacube"]
        assert pd["status"] == "skipped"


class TestRunAcolitePipelinePolygonDatacubeEnabledEmpty:
    """A4 — build_polygon_datacube=True but scl_dir has no *_SCL.tif files."""

    def test_skipped_when_scl_dir_empty(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        scl_dir.mkdir()  # exists but empty — no SCL files

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            scl_dir=scl_dir,
            use_scl=False,
            build_polygon_datacube=True,
        )

        assert result["status"] == "success"
        pd = result["outputs"]["polygon_datacube"]
        assert pd["status"] == "skipped"
        assert "No SCL files" in pd["reason"]
        assert str(scl_dir) in pd["reason"]

    def test_skipped_when_scl_dir_missing(self, tmp_path):
        """scl_dir doesn't exist at all — glob() on it must not raise."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            scl_dir=tmp_path / "nonexistent_scl",
            use_scl=False,
            build_polygon_datacube=True,
        )

        assert result["status"] == "success"
        pd = result["outputs"]["polygon_datacube"]
        assert pd["status"] == "skipped"

    def test_scl_dir_defaults_to_acolite_io_section_when_none(self, tmp_path):
        """When scl_dir is not passed, the datacube step must use the same
        resolved scl_dir_path as use_scl clipping would (AcoliteIOSection
        default), not some independent path."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
            build_polygon_datacube=True,
        )

        pd = result["outputs"]["polygon_datacube"]
        assert pd["status"] == "skipped"
        assert AcoliteIOSection().scl_dir in pd["reason"]
