"""
Unit tests for sentinel_data.py — Step 4: CLI (_build_sentinel_parser).

Tests use _build_sentinel_parser() directly to avoid triggering the
module-level network calls (SentinelHub / pystac_client) that fire on
import.  The conftest.py already patches Client.open() at collection time,
so full-module imports are safe, but parser tests are kept independent.

Covers:
  - --mode is required; invalid mode is rejected
  - --strategy default is "best"; all five valid values accepted
  - --strategy rejects unknown values (argparse exits 2)
  - --max-per-date default is 1; parsed as int
  - --max-cloud-cover default is None; parsed as int when provided
  - --help exits zero and mentions key flags
  - run_sentinel_pipeline called with correct args from parsed namespace
"""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import patch

import pytest

from aquamatch.sentinel_data import _build_sentinel_parser, run_sentinel_pipeline
from aquamatch.pipeline_config import VALID_DOWNLOAD_STRATEGIES

# ---------------------------------------------------------------------------
# --mode
# ---------------------------------------------------------------------------


class TestSentinelCLIMode:

    def test_mode_is_required(self):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args([])
        assert exc.value.code == 2

    def test_valid_modes_accepted(self):
        parser = _build_sentinel_parser()
        for mode in ("catalog", "download", "all"):
            args = parser.parse_args(["--mode", mode])
            assert args.mode == mode

    def test_invalid_mode_exits_2(self):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--mode", "invalid"])
        assert exc.value.code == 2


# ---------------------------------------------------------------------------
# --strategy
# ---------------------------------------------------------------------------


class TestSentinelCLIStrategy:

    def test_strategy_defaults_to_best(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download"])
        assert args.strategy == "best"

    def test_all_valid_strategies_accepted(self):
        parser = _build_sentinel_parser()
        for strategy in VALID_DOWNLOAD_STRATEGIES:
            args = parser.parse_args(["--mode", "download", "--strategy", strategy])
            assert args.strategy == strategy

    def test_invalid_strategy_exits_2(self):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--mode", "download", "--strategy", "weekly"])
        assert exc.value.code == 2

    def test_strategy_is_string(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download", "--strategy", "same_day"])
        assert isinstance(args.strategy, str)


# ---------------------------------------------------------------------------
# --max-per-date
# ---------------------------------------------------------------------------


class TestSentinelCLIMaxPerDate:

    def test_max_per_date_defaults_to_1(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download"])
        assert args.max_per_date == 1

    def test_max_per_date_parsed_as_int(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download", "--max-per-date", "3"])
        assert args.max_per_date == 3
        assert isinstance(args.max_per_date, int)

    def test_non_integer_max_per_date_exits_2(self):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--mode", "download", "--max-per-date", "two"])
        assert exc.value.code == 2


# ---------------------------------------------------------------------------
# --max-cloud-cover
# ---------------------------------------------------------------------------


class TestSentinelCLIMaxCloudCover:

    def test_max_cloud_cover_defaults_to_none(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download"])
        assert args.max_cloud_cover is None

    def test_max_cloud_cover_parsed_as_int(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download", "--max-cloud-cover", "15"])
        assert args.max_cloud_cover == 15
        assert isinstance(args.max_cloud_cover, int)

    def test_non_integer_max_cloud_cover_exits_2(self):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--mode", "download", "--max-cloud-cover", "low"])
        assert exc.value.code == 2


# ---------------------------------------------------------------------------
# --help
# ---------------------------------------------------------------------------


class TestSentinelCLIHelp:

    def test_help_exits_zero(self):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--help"])
        assert exc.value.code == 0

    def test_help_mentions_strategy(self, capsys):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])
        output = capsys.readouterr().out
        assert "strategy" in output

    def test_help_mentions_max_per_date(self, capsys):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])
        output = capsys.readouterr().out
        assert "max-per-date" in output

    def test_help_mentions_max_cloud_cover(self, capsys):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])
        output = capsys.readouterr().out
        assert "max-cloud-cover" in output

    def test_help_mentions_mode(self, capsys):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])
        output = capsys.readouterr().out
        assert "mode" in output

    def test_help_does_not_mention_only_first(self, capsys):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])
        output = capsys.readouterr().out
        assert "only_first" not in output
        assert "only-first" not in output


# ---------------------------------------------------------------------------
# Combined flags
# ---------------------------------------------------------------------------


class TestSentinelCLICombinedFlags:

    def test_all_new_flags_together(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(
            [
                "--mode",
                "download",
                "--strategy",
                "previous",
                "--max-per-date",
                "2",
                "--max-cloud-cover",
                "10",
            ]
        )
        assert args.strategy == "best" or args.strategy == "previous"
        assert args.strategy == "previous"
        assert args.max_per_date == 2
        assert args.max_cloud_cover == 10

    def test_catalog_mode_does_not_require_strategy(self):
        """Strategy is a download flag; catalog mode must still parse without it."""
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "catalog"])
        assert args.mode == "catalog"
        assert args.strategy == "best"  # default still present

    def test_strategy_forwarded_to_run_sentinel_pipeline(self, tmp_path):
        """Parsed args must be forwarded correctly to run_sentinel_pipeline."""
        parser = _build_sentinel_parser()
        args = parser.parse_args(
            [
                "--mode",
                "download",
                "--strategy",
                "same_day",
                "--max-per-date",
                "2",
                "--max-cloud-cover",
                "20",
                "--output",
                str(tmp_path),
                "--output-json",
                str(tmp_path / "catalog.json"),
            ]
        )

        # Write a minimal catalog so run_sentinel_pipeline doesn't error on missing file
        import json

        (tmp_path / "catalog.json").write_text(json.dumps([]))

        captured = {}

        def fake_run(catalog_json, output_dir, **kwargs):
            captured.update(kwargs)

        with patch("aquamatch.sentinel_data.run_download", side_effect=fake_run):
            run_sentinel_pipeline(
                catalog_json=args.output_json,
                output_dir=args.output,
                mode=args.mode,
                strategy=args.strategy,
                max_per_date=args.max_per_date,
                max_cloud_cover=args.max_cloud_cover,
            )

        assert captured["strategy"] == "same_day"
        assert captured["max_per_date"] == 2
        assert captured["max_cloud_cover"] == 20
