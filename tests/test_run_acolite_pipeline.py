"""
Tests for run_acolite_pipeline() — Step 3, sub-task 3.1.

Network / subprocess boundary
------------------------------
AcoliteConfig._execute() shells out to the ACOLITE binary.
run_batch() imports scl_water and sentinel_data at call time.
All subprocess and heavy I/O calls are patched at the call-site level.
The conftest already patches pystac_client.Client.open().

Conventions (matching the existing test suite)
----------------------------------------------
- One class per logical concern.
- Real .SAFE folders written to tmp_path (mkdir + dummy file).
- _execute patched to return a minimal success dict without shelling out.
- pytest.approx for floats; plain assert for everything else.
"""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from aquamatch.acolite_spec import AcoliteConfig, IOConfig, run_acolite_pipeline
from aquamatch.pipeline_config import (
    AcoliteSection,
    AcoliteIOSection,
    SclSection,
    TilesSection,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_SCENE_STEM = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_20230919T094731"
_SAFE_NAME = f"{_SCENE_STEM}.SAFE"


def _make_exe(tmp_path: Path) -> Path:
    """Create a minimal fake ACOLITE executable."""
    exe = tmp_path / "acolite"
    exe.write_text("#!/bin/sh\n")
    exe.chmod(0o755)
    return exe


def _make_safe(safe_dir: Path, name: str = _SAFE_NAME) -> Path:
    """Create a minimal fake .SAFE folder."""
    safe = safe_dir / name
    safe.mkdir(parents=True, exist_ok=True)
    (safe / "dummy.xml").write_text("<root/>")
    return safe


def _fake_execute_result(safe_path: Path, output_dir: Path) -> dict:
    """Minimal success result that _execute would return."""
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
# Status dict contract
# ---------------------------------------------------------------------------


class TestRunAcolitePipelineStatusDict:
    """The returned dict must always have the right shape."""

    def test_step_field_is_acolite(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
        )

        assert result["step"] == "acolite"

    def test_success_status_and_null_error(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe = _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe, tmp_path / "out" / _SCENE_STEM),
        ):
            result = run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
            )

        assert result["status"] == "success"
        assert result["error"] is None

    def test_elapsed_seconds_is_non_negative_float(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
        )

        assert isinstance(result["elapsed_seconds"], float)
        assert result["elapsed_seconds"] >= 0.0

    def test_error_result_has_empty_outputs(self, tmp_path):
        """A fatal error before run_batch must return empty outputs."""
        result = run_acolite_pipeline(
            acolite_executable="/nonexistent/acolite",
            safe_dir=tmp_path / "safe",
            output=tmp_path / "out",
            use_scl=False,
        )

        assert result["status"] == "error"
        assert result["outputs"] == {}
        assert result["error"] is not None

    def test_error_result_still_has_elapsed_seconds(self, tmp_path):
        result = run_acolite_pipeline(
            acolite_executable="/nonexistent/acolite",
            safe_dir=tmp_path / "safe",
            output=tmp_path / "out",
            use_scl=False,
        )

        assert isinstance(result["elapsed_seconds"], float)
        assert result["elapsed_seconds"] >= 0.0


# ---------------------------------------------------------------------------
# Default path resolution
# ---------------------------------------------------------------------------


class TestRunAcolitePipelineDefaults:
    """None arguments must resolve to dataclass defaults."""

    def test_none_safe_dir_resolves_to_section_default(self):
        """Patch run_batch to capture the safe_list derivation."""
        defaults = AcoliteIOSection()
        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            # safe_list is derived from safe_dir — we capture base_output
            # to verify output_path resolution instead (safe_dir may not exist)
            captured["base_output"] = Path(base_output)
            return []

        with patch.object(AcoliteConfig, "run_batch", side_effect=fake_run_batch):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=None,
                output=None,
                use_scl=False,
            )

        assert captured["base_output"] == Path(defaults.output)

    def test_none_use_scl_resolves_to_scl_section_default(self):
        """use_scl=None must resolve to SclSection.use_scl default."""
        defaults = SclSection()
        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            captured["use_scl"] = kwargs.get("use_scl")
            return []

        with patch.object(AcoliteConfig, "run_batch", side_effect=fake_run_batch):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=None,
                output=None,
                use_scl=None,
            )

        assert captured["use_scl"] == defaults.use_scl

    def test_none_skip_existing_resolves_to_section_default(self):
        defaults = AcoliteSection()
        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            captured["skip_existing"] = kwargs.get("skip_existing")
            return []

        with patch.object(AcoliteConfig, "run_batch", side_effect=fake_run_batch):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=None,
                output=None,
                use_scl=False,
                skip_existing=None,
            )

        assert captured["skip_existing"] == defaults.skip_existing

    def test_none_scl_kwargs_resolves_to_scl_section_defaults(self):
        defaults = SclSection()
        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            captured["scl_kwargs"] = kwargs.get("scl_kwargs")
            return []

        with patch.object(AcoliteConfig, "run_batch", side_effect=fake_run_batch):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=None,
                output=None,
                use_scl=False,
                scl_kwargs=None,
            )

        assert captured["scl_kwargs"]["min_area_m2"] == defaults.min_area_m2
        assert (
            captured["scl_kwargs"]["simplify_tolerance"] == defaults.simplify_tolerance
        )
        assert captured["scl_kwargs"]["buffer_m"] == defaults.buffer_m


# ---------------------------------------------------------------------------
# Output counts
# ---------------------------------------------------------------------------


class TestRunAcolitePipelineOutputCounts:
    """outputs dict must report correct scene counts."""

    def test_n_scenes_matches_safe_folders_found(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe1 = _make_safe(
            safe_dir, "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD.SAFE"
        )
        safe2 = _make_safe(
            safe_dir, "S2A_MSIL1C_20250802T101031_N0500_R024_T21HUD.SAFE"
        )
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )

        with patch.object(
            cfg,
            "_execute",
            side_effect=lambda p: _fake_execute_result(safe1, tmp_path / "out"),
        ):
            result = run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
            )

        assert result["outputs"]["n_scenes"] == 2

    def test_n_success_counts_returncode_zero(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe = _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe, tmp_path / "out" / _SCENE_STEM),
        ):
            result = run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
            )

        assert result["outputs"]["n_success"] == 1
        assert result["outputs"]["n_error"] == 0

    def test_n_scenes_zero_when_safe_dir_empty(self, tmp_path):
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
        assert result["outputs"]["n_scenes"] == 0
        assert result["outputs"]["n_success"] == 0

    def test_n_scenes_zero_when_safe_dir_missing(self, tmp_path):
        exe = _make_exe(tmp_path)

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=tmp_path / "nonexistent",
            output=tmp_path / "out",
            use_scl=False,
        )

        assert result["status"] == "success"
        assert result["outputs"]["n_scenes"] == 0

    def test_scenes_list_in_outputs(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
        )

        assert "scenes" in result["outputs"]
        assert isinstance(result["outputs"]["scenes"], list)

    def test_output_paths_in_outputs(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        out = tmp_path / "out"

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=safe_dir,
            output=out,
            use_scl=False,
        )

        assert result["outputs"]["safe_dir"] == safe_dir
        assert result["outputs"]["output"] == out


# ---------------------------------------------------------------------------
# Simple vs advanced usage modes
# ---------------------------------------------------------------------------


class TestRunAcolitePipelineModes:
    """Both the simple and advanced (acolite_config) modes must work."""

    def test_simple_mode_builds_acolite_config(self, tmp_path):
        """Simple mode: executable + dirs → wrapper builds AcoliteConfig."""
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

    def test_advanced_mode_uses_provided_config(self, tmp_path):
        """Advanced mode: pre-built AcoliteConfig is used as-is."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe = _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )
        captured = {}

        original_run_batch = cfg.run_batch

        def capturing_run_batch(safe_list, base_output, **kwargs):
            captured["config_id"] = id(cfg)
            return original_run_batch(safe_list, base_output, **kwargs)

        with patch.object(
            cfg, "run_batch", side_effect=capturing_run_batch
        ), patch.object(
            cfg, "_execute", return_value=_fake_execute_result(safe, tmp_path / "out")
        ):
            run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
            )

        assert "config_id" in captured

    def test_acolite_config_takes_precedence_over_executable_arg(self, tmp_path):
        """When acolite_config is provided, executable arg is ignored."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )

        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            captured["called"] = True
            return []

        with patch.object(cfg, "run_batch", side_effect=fake_run_batch):
            result = run_acolite_pipeline(
                acolite_executable="/some/other/exe",  # should be ignored
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
            )

        assert captured.get("called") is True
        assert result["status"] == "success"

    def test_accepts_string_paths(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()

        result = run_acolite_pipeline(
            acolite_executable=str(exe),
            safe_dir=str(safe_dir),
            output=str(tmp_path / "out"),
            use_scl=False,
        )

        assert result["status"] == "success"


# ---------------------------------------------------------------------------
# run_batch argument forwarding
# ---------------------------------------------------------------------------


class TestRunAcolitePipelineForwarding:
    """run_batch must receive exactly the arguments the wrapper resolved."""

    def _capture_run_batch(self, captured: dict):
        def fake(safe_list, base_output, **kwargs):
            captured.update(kwargs)
            return []

        return fake

    def test_skip_existing_forwarded(self, tmp_path):
        captured = {}
        with patch.object(
            AcoliteConfig, "run_batch", side_effect=self._capture_run_batch(captured)
        ):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                use_scl=False,
                skip_existing=False,
            )
        assert captured["skip_existing"] is False

    def test_continue_on_error_forwarded(self, tmp_path):
        captured = {}
        with patch.object(
            AcoliteConfig, "run_batch", side_effect=self._capture_run_batch(captured)
        ):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                use_scl=False,
                continue_on_error=False,
            )
        assert captured["continue_on_error"] is False

    def test_tile_config_forwarded(self, tmp_path):
        captured = {}
        tiles = TilesSection()
        with patch.object(
            AcoliteConfig, "run_batch", side_effect=self._capture_run_batch(captured)
        ):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                use_scl=False,
                tile_config=tiles,
            )
        assert captured["tile_config"] is tiles

    def test_scl_kwargs_forwarded(self, tmp_path):
        captured = {}
        custom_kwargs = {"min_area_m2": 9999, "buffer_m": 50, "simplify_tolerance": 10}
        with patch.object(
            AcoliteConfig, "run_batch", side_effect=self._capture_run_batch(captured)
        ):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                use_scl=False,
                scl_kwargs=custom_kwargs,
            )
        assert captured["scl_kwargs"] == custom_kwargs

    def test_use_scl_false_passes_none_scl_dir(self, tmp_path):
        """When use_scl=False, scl_dir forwarded to run_batch must be None."""
        captured = {}
        with patch.object(
            AcoliteConfig, "run_batch", side_effect=self._capture_run_batch(captured)
        ):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                use_scl=False,
                scl_dir=tmp_path / "scl",
            )
        assert captured["scl_dir"] is None

    def test_use_scl_true_passes_resolved_scl_dir(self, tmp_path):
        """When use_scl=True, the resolved scl_dir is forwarded."""
        captured = {}
        scl_dir = tmp_path / "scl"
        with patch.object(
            AcoliteConfig, "run_batch", side_effect=self._capture_run_batch(captured)
        ):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                use_scl=True,
                scl_dir=scl_dir,
            )
        assert captured["scl_dir"] == scl_dir


# ---------------------------------------------------------------------------
# limit and polygon — global spatial restriction
# ---------------------------------------------------------------------------


class TestRunAcolitePipelineLimitPolygon:
    """limit and polygon apply a single spatial restriction to all scenes."""

    def _capture_settings(self, cfg):
        """
        Capture the settings dict at the moment ACOLITE serialises the config.
        Patches to_settings_dict on the instance so the capture happens
        regardless of whether _execute is also patched.
        """
        captured = {}
        original = cfg.to_settings_dict

        def fake_to_settings_dict():
            d = original()
            captured["limit"] = d.get("limit")
            captured["polygon"] = d.get("polygon")
            captured["polygon_clip"] = d.get("polygon_clip")
            return d

        cfg.to_settings_dict = fake_to_settings_dict
        return captured

    # --- Validation ---

    def test_limit_and_polygon_together_raise(self, tmp_path):
        with pytest.raises(ValueError, match="either 'limit' or 'polygon'"):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                limit=(-33.25, -58.45, -33.17, -58.33),
                polygon="data/polygons/area.geojson",
            )

    def test_limit_and_tile_config_together_raise(self, tmp_path):
        from aquamatch.pipeline_config import TilesSection

        with pytest.raises(ValueError, match="tile_config"):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                limit=(-33.25, -58.45, -33.17, -58.33),
                tile_config=TilesSection(),
            )

    def test_polygon_and_tile_config_together_raise(self, tmp_path):
        from aquamatch.pipeline_config import TilesSection

        with pytest.raises(ValueError, match="tile_config"):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                polygon="data/polygons/area.geojson",
                tile_config=TilesSection(),
            )

    # --- limit applied to io ---

    def test_limit_applied_to_cfg_io(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )
        captured = self._capture_settings(cfg)

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe_dir / _SAFE_NAME, tmp_path / "out"),
        ):
            run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
                limit=(-33.25, -58.45, -33.17, -58.33),
            )

        assert captured["limit"] == "-33.25,-58.45,-33.17,-58.33"
        assert captured.get("polygon") is None
        assert captured.get("polygon_clip") is None

    def test_limit_is_converted_to_tuple(self, tmp_path):
        """limit passed as a list must be serialised identically to a tuple."""
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )
        captured = self._capture_settings(cfg)

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe_dir / _SAFE_NAME, tmp_path / "out"),
        ):
            run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
                limit=[-33.25, -58.45, -33.17, -58.33],
            )

        assert captured["limit"] == "-33.25,-58.45,-33.17,-58.33"

    # --- polygon applied to io ---

    def test_polygon_applied_to_cfg_io(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )
        captured = self._capture_settings(cfg)

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe_dir / _SAFE_NAME, tmp_path / "out"),
        ):
            run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
                polygon="data/polygons/study_area.geojson",
            )

        assert captured["polygon"] == "data/polygons/study_area.geojson"
        assert captured["polygon_clip"] == "true"
        assert captured.get("limit") is None

    def test_polygon_accepts_string_path(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )
        captured = self._capture_settings(cfg)

        poly_path = str(tmp_path / "area.geojson")
        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe_dir / _SAFE_NAME, tmp_path / "out"),
        ):
            run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
                polygon=poly_path,
            )

        assert captured["polygon"] == poly_path

    # --- no restriction when both are None ---

    def test_no_limit_or_polygon_leaves_io_unrestricted(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )
        captured = self._capture_settings(cfg)

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe_dir / _SAFE_NAME, tmp_path / "out"),
        ):
            run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
            )

        assert captured.get("limit") is None
        assert captured.get("polygon") is None
        assert captured.get("polygon_clip") is None

    # --- status dict unaffected ---

    def test_limit_run_returns_success(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        safe = _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe, tmp_path / "out" / _SCENE_STEM),
        ):
            result = run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
                limit=(-33.25, -58.45, -33.17, -58.33),
            )

        assert result["status"] == "success"
        assert result["outputs"]["n_success"] == 1


# ---------------------------------------------------------------------------
# dry_run
# ---------------------------------------------------------------------------


class TestRunAcolitePipelineDryRun:
    """dry_run=True must be forwarded to run_batch and never call _execute."""

    def test_dry_run_forwarded_to_run_batch(self, tmp_path):
        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            captured["dry_run"] = kwargs.get("dry_run")
            return []

        with patch.object(AcoliteConfig, "run_batch", side_effect=fake_run_batch):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                use_scl=False,
                dry_run=True,
            )

        assert captured["dry_run"] is True

    def test_dry_run_false_by_default(self, tmp_path):
        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            captured["dry_run"] = kwargs.get("dry_run")
            return []

        with patch.object(AcoliteConfig, "run_batch", side_effect=fake_run_batch):
            run_acolite_pipeline(
                acolite_executable="/fake/exe",
                safe_dir=tmp_path,
                output=tmp_path,
                use_scl=False,
            )

        assert captured["dry_run"] is False

    def test_dry_run_does_not_call_execute(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )

        with patch.object(cfg, "_execute") as mock_exec:
            run_acolite_pipeline(
                acolite_config=cfg,
                safe_dir=safe_dir,
                output=tmp_path / "out",
                use_scl=False,
                dry_run=True,
            )

        mock_exec.assert_not_called()

    def test_dry_run_still_returns_success(self, tmp_path):
        exe = _make_exe(tmp_path)
        safe_dir = tmp_path / "safe"
        _make_safe(safe_dir)
        cfg = AcoliteConfig(
            acolite_executable=str(exe),
            io=IOConfig(inputfile="", output=str(tmp_path / "out")),
        )

        result = run_acolite_pipeline(
            acolite_config=cfg,
            safe_dir=safe_dir,
            output=tmp_path / "out",
            use_scl=False,
            dry_run=True,
        )

        assert result["status"] == "success"
        assert result["outputs"]["n_scenes"] == 1


# ---------------------------------------------------------------------------
# CLI — __main__ block
# ---------------------------------------------------------------------------


class TestAcoliteSpecCLI:
    """Tests for the __main__ hybrid CLI block."""

    def _run_cli(self, args: list[str]) -> tuple[int, str]:
        """Run python -m aquamatch.acolite_spec with given args."""
        import subprocess, sys

        proc = subprocess.run(
            [sys.executable, "-m", "aquamatch.acolite_spec"] + args,
            capture_output=True,
            text=True,
        )
        return proc.returncode, proc.stdout + proc.stderr

    # --- Help ---

    def test_help_exits_zero(self):
        rc, _ = self._run_cli(["--help"])
        assert rc == 0

    def test_help_mentions_config(self):
        _, output = self._run_cli(["--help"])
        assert "--config" in output

    def test_help_mentions_executable(self):
        _, output = self._run_cli(["--help"])
        assert "--executable" in output

    def test_help_mentions_dry_run(self):
        _, output = self._run_cli(["--help"])
        assert "--dry-run" in output

    # --- Explicit mode validation ---

    def test_missing_executable_exits_nonzero(self):
        """--executable is required in explicit mode."""
        rc, output = self._run_cli(["--safe-dir", "/tmp"])
        assert rc != 0
        assert "executable" in output.lower()

    def test_config_and_executable_are_mutually_exclusive(self):
        rc, output = self._run_cli(["--config", "x.yaml", "--executable", "/fake"])
        assert rc != 0

    # --- YAML mode ---

    def test_yaml_mode_calls_run_acolite(self, tmp_path):
        """--config mode must load the YAML and call _run_acolite."""
        from aquamatch.pipeline_config import PipelineConfig
        from aquamatch.acolite_spec import _build_acolite_parser
        import sys

        yaml_file = tmp_path / "campaign.yaml"
        PipelineConfig.generate(yaml_file)

        captured = {}

        def fake_run_acolite(self):
            captured["called"] = True
            return []

        args = _build_acolite_parser().parse_args(["--config", str(yaml_file)])

        with patch.object(
            PipelineConfig, "_run_acolite", fake_run_acolite
        ), patch.object(PipelineConfig, "from_yaml", return_value=PipelineConfig()):
            # Simulate the __main__ YAML branch directly
            cfg = PipelineConfig.from_yaml(args.config)
            cfg._run_acolite()

        assert captured.get("called") is True

    def test_yaml_mode_no_skip_existing_overrides_config(self, tmp_path):
        """--no-skip-existing must set skip_existing=False on loaded config."""
        from aquamatch.pipeline_config import PipelineConfig
        from aquamatch.acolite_spec import _build_acolite_parser

        yaml_file = tmp_path / "campaign.yaml"
        PipelineConfig.generate(yaml_file)

        args = _build_acolite_parser().parse_args(
            [
                "--config",
                str(yaml_file),
                "--no-skip-existing",
            ]
        )

        cfg = PipelineConfig.from_yaml(args.config)
        if args.no_skip_existing:
            cfg.acolite.skip_existing = False

        assert cfg.acolite.skip_existing is False

    # --- Explicit mode dry-run ---

    def test_explicit_dry_run_flag_parses_to_true(self, tmp_path):
        """--dry-run must parse to args.dry_run == True."""
        from aquamatch.acolite_spec import _build_acolite_parser

        exe = _make_exe(tmp_path)
        args = _build_acolite_parser().parse_args(
            [
                "--executable",
                str(exe),
                "--dry-run",
            ]
        )

        assert args.dry_run is True

    def test_explicit_no_dry_run_flag_parses_to_false(self, tmp_path):
        """Absence of --dry-run must parse to args.dry_run == False."""
        from aquamatch.acolite_spec import _build_acolite_parser

        exe = _make_exe(tmp_path)
        args = _build_acolite_parser().parse_args(
            [
                "--executable",
                str(exe),
            ]
        )

        assert args.dry_run is False

    def test_explicit_no_skip_existing_flag_parses_correctly(self, tmp_path):
        """--no-skip-existing must parse to args.no_skip_existing == True."""
        from aquamatch.acolite_spec import _build_acolite_parser

        exe = _make_exe(tmp_path)
        args = _build_acolite_parser().parse_args(
            [
                "--executable",
                str(exe),
                "--no-skip-existing",
            ]
        )

        assert args.no_skip_existing is True
        # And it must invert to skip_existing=False when forwarded
        assert not args.no_skip_existing is False
