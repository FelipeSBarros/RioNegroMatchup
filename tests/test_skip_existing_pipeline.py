"""
Unit tests for pipeline_config.py — Step 3: skip_existing wiring.

Covers:
  - AcoliteSection.skip_existing defaults to True
  - skip_existing loaded correctly from YAML (true / false / missing)
  - unknown key still raises (guards existing validation)
  - _run_acolite forwards skip_existing to run_batch
  - generate() template contains skip_existing
  - generate() template round-trips through from_yaml with correct default
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from aquamatch.pipeline_config import (
    AcoliteSection,
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
# AcoliteSection default
# ---------------------------------------------------------------------------


class TestAcoliteSectionDefault:

    def test_skip_existing_defaults_to_true(self):
        assert AcoliteSection().skip_existing is True

    def test_skip_existing_can_be_set_false(self):
        assert AcoliteSection(skip_existing=False).skip_existing is False


# ---------------------------------------------------------------------------
# from_yaml — skip_existing loading
# ---------------------------------------------------------------------------


class TestFromYamlSkipExisting:

    def test_skip_existing_true_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  skip_existing: true\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.skip_existing is True

    def test_skip_existing_false_loaded(self, tmp_path):
        p = _write_yaml(tmp_path, "acolite:\n  skip_existing: false\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.skip_existing is False

    def test_skip_existing_missing_uses_default(self, tmp_path):
        p = _write_yaml(tmp_path, "campaign_name: minimal\n")
        cfg = PipelineConfig.from_yaml(p)
        assert cfg.acolite.skip_existing is True

    def test_unknown_acolite_key_still_raises(self, tmp_path):
        """Existing unknown-key validation must still work."""
        p = _write_yaml(tmp_path, "acolite:\n  not_a_key: 1\n")
        with pytest.raises(ValueError, match="not_a_key"):
            PipelineConfig.from_yaml(p)


# ---------------------------------------------------------------------------
# _run_acolite — skip_existing forwarded to run_batch
# ---------------------------------------------------------------------------


class TestRunAcoliteSkipExistingWiring:

    def _make_pipeline_cfg(self, tmp_path, skip_existing=True):
        safe_dir = tmp_path / "safe"
        safe_dir.mkdir()
        safe = safe_dir / "S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD.SAFE"
        safe.mkdir()
        (safe / "dummy.xml").write_text("<root/>")

        cfg = PipelineConfig()
        cfg.acolite.acolite_executable = str(tmp_path / "fake_acolite")
        cfg.acolite.io.safe_dir = str(safe_dir)
        cfg.acolite.io.scl_dir = str(tmp_path / "scl")
        cfg.acolite.io.output = str(tmp_path / "output")
        cfg.acolite.scl.use_scl = False
        cfg.acolite.skip_existing = skip_existing
        return cfg

    def test_skip_existing_true_forwarded(self, tmp_path):
        pipeline = self._make_pipeline_cfg(tmp_path, skip_existing=True)
        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            captured["skip_existing"] = kwargs.get("skip_existing")
            return []

        with patch(
            "aquamatch.acolite_spec.AcoliteConfig.run_batch",
            side_effect=fake_run_batch,
        ):
            pipeline._run_acolite()

        assert captured["skip_existing"] is True

    def test_skip_existing_false_forwarded(self, tmp_path):
        pipeline = self._make_pipeline_cfg(tmp_path, skip_existing=False)
        captured = {}

        def fake_run_batch(safe_list, base_output, **kwargs):
            captured["skip_existing"] = kwargs.get("skip_existing")
            return []

        with patch(
            "aquamatch.acolite_spec.AcoliteConfig.run_batch",
            side_effect=fake_run_batch,
        ):
            pipeline._run_acolite()

        assert captured["skip_existing"] is False


# ---------------------------------------------------------------------------
# generate() — template contains skip_existing
# ---------------------------------------------------------------------------


class TestGenerateSkipExisting:

    def test_template_contains_skip_existing(self, tmp_path):
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        assert "skip_existing" in out.read_text()

    def test_template_skip_existing_is_true(self, tmp_path):
        """Default value in template must match the dataclass default."""
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        content = out.read_text()
        assert "skip_existing: true" in content

    def test_template_round_trips_skip_existing(self, tmp_path):
        """Generated template must load with skip_existing=True."""
        out = tmp_path / "template.yaml"
        PipelineConfig.generate(out)
        cfg = PipelineConfig.from_yaml(out)
        assert cfg.acolite.skip_existing is True
