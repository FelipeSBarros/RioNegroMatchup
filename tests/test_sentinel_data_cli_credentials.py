"""
Covers:
  - --sh-client-id / --sh-client-secret / --dataspace-access-key /
    --dataspace-secret-key exist on the parser, all default to None
  - --help mentions the new flags
  - _credentials_from_cli_args() returns None when no credential flag given
  - _credentials_from_cli_args() merges CLI overrides on top of env vars
    when only some flags are given (partial override doesn't blank the rest)
  - _credentials_from_cli_args() returns pure CLI values when all four
    flags are given, ignoring env entirely for those fields
  - end-to-end: parsed args -> _credentials_from_cli_args() -> forwarded
    correctly into run_sentinel_pipeline(credentials=...)

Follows the existing test_sentinel_data_cli.py convention: tests build
the parser directly (no module-level network calls triggered) and, for
the pipeline-forwarding checks, patch run_download/build_catalog rather
than executing the __main__ block.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from aquamatch.credentials import SentinelCredentials
from aquamatch.sentinel_data import (
    _build_sentinel_parser,
    _credentials_from_cli_args,
    run_sentinel_pipeline,
)

# ---------------------------------------------------------------------------
# Parser — flags exist with correct defaults
# ---------------------------------------------------------------------------


class TestParserCredentialFlagsExist:

    def test_sh_client_id_default_none(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download"])
        assert args.sh_client_id is None

    def test_sh_client_secret_default_none(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download"])
        assert args.sh_client_secret is None

    def test_dataspace_access_key_default_none(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download"])
        assert args.dataspace_access_key is None

    def test_dataspace_secret_key_default_none(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download"])
        assert args.dataspace_secret_key is None

    def test_all_four_flags_parsed_correctly(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(
            [
                "--mode",
                "download",
                "--sh-client-id",
                "cli-id",
                "--sh-client-secret",
                "cli-secret",
                "--dataspace-access-key",
                "cli-access",
                "--dataspace-secret-key",
                "cli-secretkey",
            ]
        )
        assert args.sh_client_id == "cli-id"
        assert args.sh_client_secret == "cli-secret"
        assert args.dataspace_access_key == "cli-access"
        assert args.dataspace_secret_key == "cli-secretkey"


class TestParserHelpMentionsCredentialFlags:

    def test_help_mentions_sh_client_id(self, capsys):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])
        output = capsys.readouterr().out
        assert "sh-client-id" in output

    def test_help_mentions_dataspace_access_key(self, capsys):
        parser = _build_sentinel_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])
        output = capsys.readouterr().out
        assert "dataspace-access-key" in output


# ---------------------------------------------------------------------------
# _credentials_from_cli_args — no flags given
# ---------------------------------------------------------------------------


class TestCredentialsFromCliArgsNoFlags:

    def test_returns_none_when_nothing_passed(self):
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download"])
        assert _credentials_from_cli_args(args) is None


# ---------------------------------------------------------------------------
# _credentials_from_cli_args — partial override merges with env
# ---------------------------------------------------------------------------


class TestCredentialsFromCliArgsPartialMerge:

    def _clear_env(self, monkeypatch):
        for var in (
            "SH_CLIENT_ID",
            "SH_CLIENT_SECRET",
            "DATASPACE_ACCESS_KEY",
            "DATASPACE_SECRET_KEY",
        ):
            monkeypatch.delenv(var, raising=False)

    def test_single_cli_flag_does_not_blank_env_fields(self, monkeypatch):
        """The whole point of the merge: overriding just sh_client_id via
        CLI must NOT wipe out env-sourced dataspace keys."""
        self._clear_env(monkeypatch)
        monkeypatch.setenv("DATASPACE_ACCESS_KEY", "env-access")
        monkeypatch.setenv("DATASPACE_SECRET_KEY", "env-secretkey")

        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download", "--sh-client-id", "cli-id"])
        creds = _credentials_from_cli_args(args)

        assert creds.sh_client_id == "cli-id"
        assert creds.dataspace_access_key == "env-access"
        assert creds.dataspace_secret_key == "env-secretkey"

    def test_cli_value_wins_over_env_for_overridden_field(self, monkeypatch):
        self._clear_env(monkeypatch)
        monkeypatch.setenv("SH_CLIENT_ID", "env-id-should-be-overridden")

        parser = _build_sentinel_parser()
        args = parser.parse_args(
            ["--mode", "download", "--sh-client-id", "cli-id-wins"]
        )
        creds = _credentials_from_cli_args(args)

        assert creds.sh_client_id == "cli-id-wins"

    def test_returns_sentinel_credentials_instance(self, monkeypatch):
        self._clear_env(monkeypatch)
        parser = _build_sentinel_parser()
        args = parser.parse_args(
            ["--mode", "download", "--dataspace-access-key", "cli-access"]
        )
        creds = _credentials_from_cli_args(args)
        assert isinstance(creds, SentinelCredentials)


# ---------------------------------------------------------------------------
# _credentials_from_cli_args — all four flags given
# ---------------------------------------------------------------------------


class TestCredentialsFromCliArgsAllFlags:

    def test_all_cli_values_used_ignoring_env(self, monkeypatch):
        monkeypatch.setenv("SH_CLIENT_ID", "env-id-ignored")
        monkeypatch.setenv("SH_CLIENT_SECRET", "env-secret-ignored")
        monkeypatch.setenv("DATASPACE_ACCESS_KEY", "env-access-ignored")
        monkeypatch.setenv("DATASPACE_SECRET_KEY", "env-secretkey-ignored")

        parser = _build_sentinel_parser()
        args = parser.parse_args(
            [
                "--mode",
                "download",
                "--sh-client-id",
                "cli-id",
                "--sh-client-secret",
                "cli-secret",
                "--dataspace-access-key",
                "cli-access",
                "--dataspace-secret-key",
                "cli-secretkey",
            ]
        )
        creds = _credentials_from_cli_args(args)

        assert creds.sh_client_id == "cli-id"
        assert creds.sh_client_secret == "cli-secret"
        assert creds.dataspace_access_key == "cli-access"
        assert creds.dataspace_secret_key == "cli-secretkey"

    def test_urls_still_come_from_env_defaults_not_hardcoded(self, monkeypatch):
        """sh_base_url/sh_token_url aren't CLI flags — they must still
        come from SentinelCredentials' own defaults via from_env()."""
        parser = _build_sentinel_parser()
        args = parser.parse_args(["--mode", "download", "--sh-client-id", "cli-id"])
        creds = _credentials_from_cli_args(args)

        assert creds.sh_base_url == "https://sh.dataspace.copernicus.eu"
        assert creds.sh_token_url == (
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
            "protocol/openid-connect/token"
        )


# ---------------------------------------------------------------------------
# End-to-end — parsed args -> credentials -> forwarded to the pipeline
# ---------------------------------------------------------------------------


class TestCredentialsFromCliArgsEndToEnd:

    def test_forwarded_correctly_to_run_sentinel_pipeline(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SH_CLIENT_ID", raising=False)
        catalog_file = tmp_path / "catalog.json"
        catalog_file.write_text(json.dumps([]))

        parser = _build_sentinel_parser()
        args = parser.parse_args(
            [
                "--mode",
                "download",
                "--output",
                str(tmp_path),
                "--output-json",
                str(catalog_file),
                "--sh-client-id",
                "cli-id",
            ]
        )
        creds = _credentials_from_cli_args(args)

        with patch("aquamatch.sentinel_data.run_download", return_value={}) as mock_rd:
            run_sentinel_pipeline(
                catalog_json=args.output_json,
                output_dir=args.output,
                mode=args.mode,
                credentials=creds,
            )

        _, kwargs = mock_rd.call_args
        assert kwargs["credentials"].sh_client_id == "cli-id"

    def test_no_credential_flags_forwards_none(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        catalog_file.write_text(json.dumps([]))

        parser = _build_sentinel_parser()
        args = parser.parse_args(
            [
                "--mode",
                "download",
                "--output",
                str(tmp_path),
                "--output-json",
                str(catalog_file),
            ]
        )
        creds = _credentials_from_cli_args(args)
        assert creds is None

        with patch("aquamatch.sentinel_data.run_download", return_value={}) as mock_rd:
            run_sentinel_pipeline(
                catalog_json=args.output_json,
                output_dir=args.output,
                mode=args.mode,
                credentials=creds,
            )

        _, kwargs = mock_rd.call_args
        assert kwargs["credentials"] is None
