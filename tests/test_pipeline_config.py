from pathlib import Path
from unittest.mock import patch

import inspect

from aquamatch.acolite_spec import AcoliteConfig, IOConfig, run_acolite_pipeline
from aquamatch.pipeline_config import SclSection

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