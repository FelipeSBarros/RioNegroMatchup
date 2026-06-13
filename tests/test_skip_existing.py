"""
Unit tests for run_batch skip_existing — Step 2.

Covers:
  - skipped_existing key present in all result dict shapes
  - skip_existing=True (default): already-processed scene is skipped
  - skip_existing=True: unprocessed scene is not skipped
  - skip_existing=False: already-processed scene is still executed
  - skipped_existing=False on "SAFE not found" result
  - skipped_existing=False on dry_run result
  - skipped_existing=False on normal execute result
  - skipped_existing=False on validation-failure result
  - log message emitted when scene is skipped
  - multiple scenes: mix of processed and unprocessed
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from aquamatch.acolite_spec import AcoliteConfig, IOConfig
from aquamatch.scl_water import SCL_WATER_CLASS

# ---------------------------------------------------------------------------
# Shared helpers (mirrors pattern from test_acoilite_spec.py)
# ---------------------------------------------------------------------------

_TEST_CRS = "EPSG:32721"
_W, _S, _E, _N = 500_000.0, 6_350_000.0, 500_300.0, 6_350_300.0
_SAFE_NAME = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_20230919T094731.SAFE"
_SCENE_STEM = "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_20230919T094731"


def _make_safe(tmp_path, name=_SAFE_NAME):
    safe = tmp_path / name
    safe.mkdir(parents=True, exist_ok=True)
    (safe / "dummy.xml").write_text("<root/>")
    return safe


def _make_cfg(tmp_path):
    exe = tmp_path / "acolite"
    exe.write_text("#!/bin/sh")
    exe.chmod(0o755)
    return AcoliteConfig(
        acolite_executable=str(exe),
        io=IOConfig(inputfile="", output=str(tmp_path)),
    )


def _fake_execute_result(safe_path, output_dir):
    return {
        "returncode": 0,
        "log_file": None,
        "l2w_file": None,
        "stdout": "",
        "stderr": "",
        "inputfile": str(safe_path),
        "output_dir": output_dir,
    }


def _write_outputs(output_dir: Path, stem: str = _SCENE_STEM) -> None:
    """Simulate a fully-processed scene by writing all expected output files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for suffix in ("_L1R.nc", "_L2R.nc", "_L2W.nc"):
        (output_dir / f"{stem}{suffix}").write_bytes(b"")


# ---------------------------------------------------------------------------
# skipped_existing key always present
# ---------------------------------------------------------------------------


class TestSkippedExistingKeyAlwaysPresent:

    def test_present_on_safe_not_found(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        missing = tmp_path / "nonexistent.SAFE"
        results = cfg.run_batch([missing], tmp_path)
        assert "skipped_existing" in results[0]

    def test_present_on_dry_run(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        results = cfg.run_batch([safe], tmp_path, dry_run=True)
        assert "skipped_existing" in results[0]

    def test_present_on_normal_execute(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe, tmp_path / _SCENE_STEM),
        ):
            results = cfg.run_batch([safe], tmp_path, skip_existing=False)
        assert "skipped_existing" in results[0]

    def test_present_on_skipped_existing(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)
        results = cfg.run_batch([safe], tmp_path, skip_existing=True)
        assert "skipped_existing" in results[0]


# ---------------------------------------------------------------------------
# skip_existing=True (default) — already-processed scene
# ---------------------------------------------------------------------------


class TestSkipExistingTrue:

    def test_processed_scene_is_skipped(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)

        with patch.object(cfg, "_execute") as mock_exec:
            results = cfg.run_batch([safe], tmp_path, skip_existing=True)
            mock_exec.assert_not_called()

        assert results[0]["skipped_existing"] is True

    def test_processed_scene_returncode_is_none(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)

        results = cfg.run_batch([safe], tmp_path, skip_existing=True)
        assert results[0]["returncode"] is None

    def test_processed_scene_skipped_is_false(self, tmp_path):
        """skipped_existing=True must not also set skipped=True (different conditions)."""
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)

        results = cfg.run_batch([safe], tmp_path, skip_existing=True)
        assert results[0]["skipped"] is False

    def test_processed_scene_scl_used_is_false(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)

        results = cfg.run_batch([safe], tmp_path, skip_existing=True)
        assert results[0]["scl_used"] is False

    def test_unprocessed_scene_is_not_skipped(self, tmp_path):
        """Output dir exists but is empty — scene must still be executed."""
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        (tmp_path / _SCENE_STEM).mkdir()  # exists but no output files

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe, tmp_path / _SCENE_STEM),
        ) as mock_exec:
            results = cfg.run_batch([safe], tmp_path, skip_existing=True)
            mock_exec.assert_called_once()

        assert results[0]["skipped_existing"] is False

    def test_missing_output_dir_is_not_skipped(self, tmp_path):
        """No output dir at all — scene must be executed."""
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe, tmp_path / _SCENE_STEM),
        ) as mock_exec:
            results = cfg.run_batch([safe], tmp_path, skip_existing=True)
            mock_exec.assert_called_once()

        assert results[0]["skipped_existing"] is False

    def test_skip_existing_true_is_default(self, tmp_path):
        """Calling run_batch without skip_existing must behave as skip_existing=True."""
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)

        with patch.object(cfg, "_execute") as mock_exec:
            results = cfg.run_batch([safe], tmp_path)  # no skip_existing arg
            mock_exec.assert_not_called()

        assert results[0]["skipped_existing"] is True


# ---------------------------------------------------------------------------
# skip_existing=False — already-processed scene is re-executed
# ---------------------------------------------------------------------------


class TestSkipExistingFalse:

    def test_processed_scene_is_reexecuted(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe, tmp_path / _SCENE_STEM),
        ) as mock_exec:
            results = cfg.run_batch([safe], tmp_path, skip_existing=False)
            mock_exec.assert_called_once()

        assert results[0]["skipped_existing"] is False

    def test_processed_scene_returncode_from_execute(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)

        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe, tmp_path / _SCENE_STEM),
        ):
            results = cfg.run_batch([safe], tmp_path, skip_existing=False)

        assert results[0]["returncode"] == 0


# ---------------------------------------------------------------------------
# skipped_existing=False on other result shapes
# ---------------------------------------------------------------------------


class TestSkippedExistingFalseOnOtherPaths:

    def test_safe_not_found_skipped_existing_false(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        missing = tmp_path / "nonexistent.SAFE"
        results = cfg.run_batch([missing], tmp_path)
        assert results[0]["skipped_existing"] is False

    def test_dry_run_skipped_existing_false(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        results = cfg.run_batch([safe], tmp_path, dry_run=True, skip_existing=False)
        assert results[0]["skipped_existing"] is False

    def test_normal_execute_skipped_existing_false(self, tmp_path):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        with patch.object(
            cfg,
            "_execute",
            return_value=_fake_execute_result(safe, tmp_path / _SCENE_STEM),
        ):
            results = cfg.run_batch([safe], tmp_path, skip_existing=False)
        assert results[0]["skipped_existing"] is False


# ---------------------------------------------------------------------------
# Log message
# ---------------------------------------------------------------------------


class TestSkipExistingLogging:

    def test_skip_logs_info_message(self, tmp_path, caplog):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)

        with caplog.at_level(logging.INFO, logger="aquamatch.acolite_spec"):
            cfg.run_batch([safe], tmp_path, skip_existing=True)

        assert any("already" in msg.lower() for msg in caplog.messages)

    def test_skip_log_includes_scene_stem(self, tmp_path, caplog):
        safe = _make_safe(tmp_path)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)

        with caplog.at_level(logging.INFO, logger="aquamatch.acolite_spec"):
            cfg.run_batch([safe], tmp_path, skip_existing=True)

        assert any(_SCENE_STEM in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Mixed batch — some processed, some not
# ---------------------------------------------------------------------------


_SAFE_NAME_2 = "S2A_MSIL1C_20250802T101031_N0500_R024_T21HUD_20230919T094731.SAFE"
_STEM_2 = "S2A_MSIL1C_20250802T101031_N0500_R024_T21HUD_20230919T094731"


class TestSkipExistingMixedBatch:

    def test_only_unprocessed_scenes_are_executed(self, tmp_path):
        safe1 = _make_safe(tmp_path, name=_SAFE_NAME)
        safe2 = _make_safe(tmp_path, name=_SAFE_NAME_2)
        cfg = _make_cfg(tmp_path)

        # safe1 already processed, safe2 is not
        _write_outputs(tmp_path / _SCENE_STEM)

        executed = []

        def fake_execute(settings_path):
            executed.append(settings_path)
            return {
                "returncode": 0,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
                "inputfile": "",
                "output_dir": tmp_path,
            }

        with patch.object(cfg, "_execute", side_effect=fake_execute):
            results = cfg.run_batch([safe1, safe2], tmp_path, skip_existing=True)

        # Only safe2 should have been executed
        assert len(executed) == 1
        assert results[0]["skipped_existing"] is True
        assert results[1]["skipped_existing"] is False

    def test_counts_correct_in_mixed_batch(self, tmp_path):
        safe1 = _make_safe(tmp_path, name=_SAFE_NAME)
        safe2 = _make_safe(tmp_path, name=_SAFE_NAME_2)
        cfg = _make_cfg(tmp_path)
        _write_outputs(tmp_path / _SCENE_STEM)

        with patch.object(
            cfg,
            "_execute",
            return_value={
                "returncode": 0,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
                "inputfile": "",
                "output_dir": tmp_path,
            },
        ):
            results = cfg.run_batch([safe1, safe2], tmp_path, skip_existing=True)

        skipped = [r for r in results if r["skipped_existing"]]
        executed = [
            r for r in results if not r["skipped_existing"] and not r.get("skipped")
        ]
        assert len(skipped) == 1
        assert len(executed) == 1
