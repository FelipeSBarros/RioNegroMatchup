"""
Unit tests for acolite_spec.py — Step 1: expected_outputs and is_scene_processed.

Tests use a minimal AcoliteConfig stub so they have no dependency on real
satellite data or a working ACOLITE installation.  All filesystem interaction
is against tmp_path.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rionegromatchup.acolite_spec import (
    AcoliteConfig,
    IOConfig,
    L2WConfig,
    expected_outputs,
    is_scene_processed,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_EXE = "/fake/acolite"

# A realistic per-scene ACOLITE output stem
_SCENE_STEM = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD"


def _make_cfg(l2w_parameters=None) -> AcoliteConfig:
    """Build a minimal AcoliteConfig with controllable l2w_parameters."""
    l2w = L2WConfig(
        l2w_parameters=(
            ["t_nechad", "ndwi"] if l2w_parameters is None else l2w_parameters
        )
    )
    return AcoliteConfig(
        acolite_executable=_FAKE_EXE,
        io=IOConfig(inputfile="", output=""),
        l2w=l2w,
    )


def _touch(path: Path, name: str) -> Path:
    """Create an empty file at path / name; ensure parent exists."""
    path.mkdir(parents=True, exist_ok=True)
    f = path / name
    f.write_bytes(b"")
    return f


# ---------------------------------------------------------------------------
# expected_outputs
# ---------------------------------------------------------------------------


class TestExpectedOutputs:
    """Tests for expected_outputs()."""

    # --- Directory does not exist ---

    def test_missing_dir_all_none(self, tmp_path):
        cfg = _make_cfg()
        result = expected_outputs(tmp_path / "nonexistent", cfg)
        assert result["l1r"] is None
        assert result["l2r"] is None
        assert result["l2w"] is None

    # --- Empty directory ---

    def test_empty_dir_all_none(self, tmp_path):
        out = tmp_path / "scene"
        out.mkdir()
        cfg = _make_cfg()
        result = expected_outputs(out, cfg)
        assert result["l1r"] is None
        assert result["l2r"] is None
        assert result["l2w"] is None

    # --- All stages enabled and all files present ---

    def test_all_present_returns_paths(self, tmp_path):
        out = tmp_path / "scene"
        l1r = _touch(out, f"{_SCENE_STEM}_L1R.nc")
        l2r = _touch(out, f"{_SCENE_STEM}_L2R.nc")
        l2w = _touch(out, f"{_SCENE_STEM}_L2W.nc")

        cfg = _make_cfg()
        result = expected_outputs(out, cfg)

        assert result["l1r"] == l1r
        assert result["l2r"] == l2r
        assert result["l2w"] == l2w

    # --- L1R present, L2R and L2W missing ---

    def test_only_l1r_present(self, tmp_path):
        out = tmp_path / "scene"
        l1r = _touch(out, f"{_SCENE_STEM}_L1R.nc")

        cfg = _make_cfg()
        result = expected_outputs(out, cfg)

        assert result["l1r"] == l1r
        assert result["l2r"] is None
        assert result["l2w"] is None

    # --- L2W disabled (empty l2w_parameters) ---

    def test_l2w_disabled_l2w_key_is_none_even_if_file_exists(self, tmp_path):
        """
        When l2w_parameters is empty the l2w key must be None regardless of
        whether a *_L2W.nc file happens to be on disk.
        """
        out = tmp_path / "scene"
        _touch(out, f"{_SCENE_STEM}_L1R.nc")
        _touch(out, f"{_SCENE_STEM}_L2R.nc")
        _touch(out, f"{_SCENE_STEM}_L2W.nc")  # on disk but stage is disabled

        cfg = _make_cfg(l2w_parameters=[])
        result = expected_outputs(out, cfg)

        assert result["l2w"] is None

    def test_l2w_disabled_l1r_l2r_still_returned(self, tmp_path):
        out = tmp_path / "scene"
        l1r = _touch(out, f"{_SCENE_STEM}_L1R.nc")
        l2r = _touch(out, f"{_SCENE_STEM}_L2R.nc")

        cfg = _make_cfg(l2w_parameters=[])
        result = expected_outputs(out, cfg)

        assert result["l1r"] == l1r
        assert result["l2r"] == l2r

    # --- Unrelated files do not satisfy stage checks ---

    def test_unrelated_files_do_not_satisfy_stages(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, "acolite_run_20250801T101031.log")
        _touch(out, "some_other_file.tif")

        cfg = _make_cfg()
        result = expected_outputs(out, cfg)

        assert result["l1r"] is None
        assert result["l2r"] is None
        assert result["l2w"] is None

    # --- Returns correct keys ---

    def test_return_dict_has_all_three_keys(self, tmp_path):
        cfg = _make_cfg()
        result = expected_outputs(tmp_path / "scene", cfg)
        assert set(result.keys()) == {"l1r", "l2r", "l2w"}

    # --- Accepts str path as well as Path ---

    def test_accepts_string_path(self, tmp_path):
        out = tmp_path / "scene"
        out.mkdir()
        cfg = _make_cfg()
        result = expected_outputs(str(out), cfg)
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# is_scene_processed
# ---------------------------------------------------------------------------


class TestIsSceneProcessed:
    """Tests for is_scene_processed()."""

    # --- Directory missing ---

    def test_missing_dir_returns_false(self, tmp_path):
        cfg = _make_cfg()
        assert is_scene_processed(tmp_path / "nonexistent", cfg) is False

    # --- Empty directory ---

    def test_empty_dir_returns_false(self, tmp_path):
        out = tmp_path / "scene"
        out.mkdir()
        cfg = _make_cfg()
        assert is_scene_processed(out, cfg) is False

    # --- All stages enabled, all files present ---

    def test_all_stages_complete_returns_true(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, f"{_SCENE_STEM}_L1R.nc")
        _touch(out, f"{_SCENE_STEM}_L2R.nc")
        _touch(out, f"{_SCENE_STEM}_L2W.nc")

        cfg = _make_cfg()
        assert is_scene_processed(out, cfg) is True

    # --- L2W enabled but file missing ---

    def test_l2w_missing_when_enabled_returns_false(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, f"{_SCENE_STEM}_L1R.nc")
        _touch(out, f"{_SCENE_STEM}_L2R.nc")
        # no L2W file

        cfg = _make_cfg()  # l2w_parameters non-empty → L2W enabled
        assert is_scene_processed(out, cfg) is False

    # --- L1R missing ---

    def test_l1r_missing_returns_false(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, f"{_SCENE_STEM}_L2R.nc")
        _touch(out, f"{_SCENE_STEM}_L2W.nc")

        cfg = _make_cfg()
        assert is_scene_processed(out, cfg) is False

    # --- L2R missing ---

    def test_l2r_missing_returns_false(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, f"{_SCENE_STEM}_L1R.nc")
        _touch(out, f"{_SCENE_STEM}_L2W.nc")

        cfg = _make_cfg()
        assert is_scene_processed(out, cfg) is False

    # --- L2W disabled, L1R + L2R present ---

    def test_l2w_disabled_l1r_l2r_present_returns_true(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, f"{_SCENE_STEM}_L1R.nc")
        _touch(out, f"{_SCENE_STEM}_L2R.nc")

        cfg = _make_cfg(l2w_parameters=[])
        assert is_scene_processed(out, cfg) is True

    def test_l2w_disabled_l1r_missing_returns_false(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, f"{_SCENE_STEM}_L2R.nc")

        cfg = _make_cfg(l2w_parameters=[])
        assert is_scene_processed(out, cfg) is False

    def test_l2w_disabled_l2r_missing_returns_false(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, f"{_SCENE_STEM}_L1R.nc")

        cfg = _make_cfg(l2w_parameters=[])
        assert is_scene_processed(out, cfg) is False

    # --- Unrelated files do not count ---

    def test_unrelated_files_only_returns_false(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, "acolite_run_20250801T101031.log")
        _touch(out, "some_output.tif")

        cfg = _make_cfg()
        assert is_scene_processed(out, cfg) is False

    # --- Accepts str path ---

    def test_accepts_string_path(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, f"{_SCENE_STEM}_L1R.nc")
        _touch(out, f"{_SCENE_STEM}_L2R.nc")
        _touch(out, f"{_SCENE_STEM}_L2W.nc")

        cfg = _make_cfg()
        assert is_scene_processed(str(out), cfg) is True

    # --- Return type is bool, not truthy object ---

    def test_returns_bool_true(self, tmp_path):
        out = tmp_path / "scene"
        _touch(out, f"{_SCENE_STEM}_L1R.nc")
        _touch(out, f"{_SCENE_STEM}_L2R.nc")
        _touch(out, f"{_SCENE_STEM}_L2W.nc")

        cfg = _make_cfg()
        result = is_scene_processed(out, cfg)
        assert type(result) is bool and result is True

    def test_returns_bool_false(self, tmp_path):
        out = tmp_path / "scene"
        out.mkdir()
        cfg = _make_cfg()
        result = is_scene_processed(out, cfg)
        assert type(result) is bool and result is False
