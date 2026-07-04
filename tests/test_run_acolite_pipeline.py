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


class TestRunAcolitePipelinePolygonDatacubeEnabledWithFiles:
    """A5 — build_polygon_datacube=True with real *_SCL.tif files in scl_dir."""

    def _make_scl_files(self, scl_dir: Path, n: int = 2) -> list[Path]:
        scl_dir.mkdir(parents=True, exist_ok=True)
        files = []
        for i in range(n):
            f = scl_dir / f"scene{i}_SCL.tif"
            f.write_bytes(b"fake")
            files.append(f)
        return files

    def test_calls_build_water_polygon_datacube_with_correct_records(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=2)
        dc_path = tmp_path / "custom_water_polygons.gpkg"

        captured = {}

        def fake_build(records, output_path, overwrite, **kwargs):
            captured["records"] = records
            captured["output_path"] = output_path
            captured["overwrite"] = overwrite
            captured["kwargs"] = kwargs
            return Path(output_path)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=fake_build,
        ):
            result = run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
                polygon_datacube_path=dc_path,
            )

        assert len(captured["records"]) == 2
        assert captured["output_path"] == dc_path
        assert result["outputs"]["polygon_datacube"]["status"] == "ok"
        assert result["outputs"]["polygon_datacube"]["n_records"] == 2

    def test_overwrite_forwarded(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        captured = {}

        def fake_build(records, output_path, overwrite, **kwargs):
            captured["overwrite"] = overwrite
            return Path(output_path)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=fake_build,
        ):
            run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
                polygon_datacube_overwrite=True,
            )

        assert captured["overwrite"] is True

    def test_overwrite_defaults_to_false(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        captured = {}

        def fake_build(records, output_path, overwrite, **kwargs):
            captured["overwrite"] = overwrite
            return Path(output_path)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=fake_build,
        ):
            run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
            )

        assert captured["overwrite"] is False
        assert SclSection().polygon_datacube_overwrite is False

    def test_default_scl_kwargs_forwarded(self, tmp_path):
        """min_area_m2/simplify_tolerance/buffer_m default to SclSection()."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        captured = {}

        def fake_build(records, output_path, overwrite, **kwargs):
            captured.update(kwargs)
            return Path(output_path)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=fake_build,
        ):
            run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
            )

        defaults = SclSection()
        assert captured["min_area_m2"] == defaults.min_area_m2
        assert captured["simplify_tolerance"] == defaults.simplify_tolerance
        assert captured["buffer_m"] == defaults.buffer_m

    def test_custom_scl_kwargs_forwarded_to_datacube_too(self, tmp_path):
        """scl_kwargs is shared between use_scl clipping and the datacube
        step by design — no duplicate parameter surface (see A5 plan)."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        captured = {}

        def fake_build(records, output_path, overwrite, **kwargs):
            captured.update(kwargs)
            return Path(output_path)

        custom_kwargs = {
            "min_area_m2": 9999,
            "simplify_tolerance": 5,
            "buffer_m": 42,
        }

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=fake_build,
        ):
            run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
                scl_kwargs=custom_kwargs,
            )

        assert captured["min_area_m2"] == 9999
        assert captured["simplify_tolerance"] == 5
        assert captured["buffer_m"] == 42

    def test_polygon_datacube_path_defaults_to_scl_section(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        captured = {}

        def fake_build(records, output_path, overwrite, **kwargs):
            captured["output_path"] = output_path
            return Path(output_path)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=fake_build,
        ):
            run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
            )

        assert captured["output_path"] == Path(SclSection().polygon_datacube_path)


class TestRunAcolitePipelinePolygonDatacubeFailurePropagation:
    """A6 — a failure inside the datacube step surfaces as status='error'
    on the whole run_acolite_pipeline() result, same severity as an
    ACOLITE sub-config validation failure. Confirmed design decision:
    the datacube call is intentionally left uncaught locally."""

    def _make_scl_files(self, scl_dir: Path, n: int = 1) -> None:
        scl_dir.mkdir(parents=True, exist_ok=True)
        for i in range(n):
            (scl_dir / f"scene{i}_SCL.tif").write_bytes(b"fake")

    def test_datacube_exception_sets_status_error(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=ValueError("no water pixels found"),
        ):
            result = run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
            )

        assert result["status"] == "error"

    def test_error_message_surfaces_from_datacube_failure(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=ValueError("no water pixels found"),
        ):
            result = run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
            )

        assert "no water pixels found" in result["error"]

    def test_error_result_has_empty_outputs(self, tmp_path):
        """Consistent with other fatal-error paths (e.g. missing executable):
        outputs is emptied entirely, not partially populated."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=RuntimeError("disk full"),
        ):
            result = run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
            )

        assert result["outputs"] == {}

    def test_elapsed_seconds_still_present_on_datacube_failure(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=RuntimeError("disk full"),
        ):
            result = run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
            )

        assert isinstance(result["elapsed_seconds"], float)
        assert result["elapsed_seconds"] >= 0.0

    def test_acolite_scenes_still_succeeded_before_datacube_failure(self, tmp_path):
        """The ACOLITE run_batch results themselves were fine — only the
        post-processing datacube step failed. Confirms run_batch was
        allowed to complete without raising before the datacube step runs."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()  # no .SAFE folders — run_batch trivially succeeds
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=ValueError("boom"),
        ) as mock_build:
            run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=True,
            )

        mock_build.assert_called_once()

    def test_no_failure_when_build_polygon_datacube_false(self, tmp_path):
        """Sanity check: the patched exception must never fire when the
        datacube step is disabled — status stays 'success'."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        scl_dir = tmp_path / "scl"
        self._make_scl_files(scl_dir, n=1)

        with patch(
            "aquamatch.acolite_spec.build_water_polygon_datacube",
            side_effect=ValueError("should never be called"),
        ) as mock_build:
            result = run_acolite_pipeline(
                acolite_executable=str(exe),
                safe_dir=safe_dir,
                output=tmp_path / "out",
                scl_dir=scl_dir,
                use_scl=False,
                build_polygon_datacube=False,
            )

        mock_build.assert_not_called()
        assert result["status"] == "success"


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
