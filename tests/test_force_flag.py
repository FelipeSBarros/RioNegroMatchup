"""
Unit tests for pipeline_config.py — Step 4: --force CLI flag.

Covers:
  - --force flag exists in the parser
  - --force sets skip_existing=False on the loaded config
  - --force is independent of --dry-run (both can be used together)
  - --force without --run is a no-op (generate path unaffected)
  - YAML with skip_existing: true is overridden by --force
  - YAML with skip_existing: false is unchanged by absence of --force
  - main() still calls cfg.run() once with correct arguments
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from aquamatch.pipeline_config import (
    PipelineConfig,
    _build_parser,
    main,
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
# Parser — --force flag exists
# ---------------------------------------------------------------------------


class TestParserForceFlag:

    def test_force_flag_exists(self):
        parser = _build_parser()
        args = parser.parse_args(["--run", "config.yaml", "--force"])
        assert args.force is True

    def test_force_defaults_to_false(self):
        parser = _build_parser()
        args = parser.parse_args(["--run", "config.yaml"])
        assert args.force is False

    def test_force_is_store_true(self):
        """--force must be a boolean flag, not a value argument."""
        parser = _build_parser()
        # If it were a value arg this would raise or assign "config.yaml" to force
        args = parser.parse_args(["--run", "config.yaml", "--force"])
        assert isinstance(args.force, bool)

    def test_force_and_dry_run_together(self):
        parser = _build_parser()
        args = parser.parse_args(["--run", "config.yaml", "--force", "--dry-run"])
        assert args.force is True
        assert args.dry_run is True


# ---------------------------------------------------------------------------
# main() — --force overrides skip_existing
# ---------------------------------------------------------------------------


class TestMainForceOverride:

    def test_force_sets_skip_existing_false(self, tmp_path):
        """--force must set skip_existing=False on the config object before run()."""
        yaml_path = _make_template(tmp_path)
        captured_cfg = {}

        original_from_yaml = PipelineConfig.from_yaml

        def capturing_from_yaml(path):
            cfg = original_from_yaml(path)
            captured_cfg["cfg"] = cfg
            return cfg

        with patch.object(
            PipelineConfig, "from_yaml", side_effect=capturing_from_yaml
        ), patch.object(PipelineConfig, "run", return_value={}):
            main(["--run", str(yaml_path), "--force"])

        assert captured_cfg["cfg"].acolite.skip_existing is False

    def test_force_overrides_yaml_skip_existing_true(self, tmp_path):
        """YAML has skip_existing: true; --force must override to False."""
        p = _write_yaml(tmp_path, "acolite:\n  skip_existing: true\n")
        captured_cfg = {}

        original_from_yaml = PipelineConfig.from_yaml

        def capturing_from_yaml(path):
            cfg = original_from_yaml(path)
            captured_cfg["cfg"] = cfg
            return cfg

        with patch.object(
            PipelineConfig, "from_yaml", side_effect=capturing_from_yaml
        ), patch.object(PipelineConfig, "run", return_value={}):
            main(["--run", str(p), "--force"])

        assert captured_cfg["cfg"].acolite.skip_existing is False

    def test_no_force_preserves_yaml_skip_existing_false(self, tmp_path):
        """YAML has skip_existing: false; absence of --force must not change it."""
        p = _write_yaml(tmp_path, "acolite:\n  skip_existing: false\n")
        captured_cfg = {}

        original_from_yaml = PipelineConfig.from_yaml

        def capturing_from_yaml(path):
            cfg = original_from_yaml(path)
            captured_cfg["cfg"] = cfg
            return cfg

        with patch.object(
            PipelineConfig, "from_yaml", side_effect=capturing_from_yaml
        ), patch.object(PipelineConfig, "run", return_value={}):
            main(["--run", str(p)])

        assert captured_cfg["cfg"].acolite.skip_existing is False

    def test_no_force_preserves_yaml_skip_existing_true(self, tmp_path):
        """Without --force the YAML value (true) must be preserved."""
        p = _make_template(tmp_path)
        captured_cfg = {}

        original_from_yaml = PipelineConfig.from_yaml

        def capturing_from_yaml(path):
            cfg = original_from_yaml(path)
            captured_cfg["cfg"] = cfg
            return cfg

        with patch.object(
            PipelineConfig, "from_yaml", side_effect=capturing_from_yaml
        ), patch.object(PipelineConfig, "run", return_value={}):
            main(["--run", str(p)])

        assert captured_cfg["cfg"].acolite.skip_existing is True

    def test_run_called_once(self, tmp_path):
        """main() must call cfg.run() exactly once regardless of --force."""
        p = _make_template(tmp_path)
        with patch.object(PipelineConfig, "run", return_value={}) as mock_run:
            main(["--run", str(p), "--force"])
        mock_run.assert_called_once()

    def test_dry_run_forwarded_with_force(self, tmp_path):
        """--dry-run must still be forwarded when --force is also set."""
        p = _make_template(tmp_path)
        with patch.object(PipelineConfig, "run", return_value={}) as mock_run:
            main(["--run", str(p), "--force", "--dry-run"])
        mock_run.assert_called_once_with(dry_run=True)

    def test_generate_unaffected_by_force_absence(self, tmp_path):
        """--generate path must still work (--force only applies to --run)."""
        out = str(tmp_path / "out.yaml")
        main(["--generate", out])
        assert Path(out).exists()
