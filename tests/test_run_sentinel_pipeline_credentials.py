"""
Unit tests for aquamatch.sentinel_data.run_sentinel_pipeline() — Task 7.

run_sentinel_pipeline() must accept an optional `credentials` parameter,
either a SentinelCredentials instance or a plain dict with matching field
names, and forward it unchanged (normalised once) to both build_catalog()
and run_download() — whichever of the two actually run, per `mode`.

  - credentials=None (default): both calls receive credentials=None —
    unchanged behaviour (Tasks 5/6 defaults).
  - credentials=<SentinelCredentials>: forwarded as-is to both calls.
  - credentials=<dict>: converted ONCE to a SentinelCredentials instance,
    and that same instance (not two separately-constructed ones) is
    passed to both build_catalog() and run_download().
  - mode="catalog" / "download": credentials only reaches the call that
    actually runs for that mode.

All build_catalog()/run_download() calls are mocked — no real network,
credentials, or catalog/SAFE files are needed for these tests.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from aquamatch.credentials import SentinelCredentials
from aquamatch.sentinel_data import run_sentinel_pipeline

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_unique_csv(path: Path) -> None:
    pd.DataFrame(
        {
            "date": ["2024-03-15"],
            "latitud": [-32.85],
            "longitud": [-56.57],
            "s2_tile": ["21HUD"],
        }
    ).to_csv(path, index=False)


def _write_catalog_json(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([]))


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------


class TestRunSentinelPipelineCredentialsSignature:

    def test_has_credentials_param(self):
        sig = inspect.signature(run_sentinel_pipeline)
        assert "credentials" in sig.parameters

    def test_credentials_defaults_to_none(self):
        sig = inspect.signature(run_sentinel_pipeline)
        assert sig.parameters["credentials"].default is None


# ---------------------------------------------------------------------------
# credentials=None — unchanged behaviour
# ---------------------------------------------------------------------------


class TestRunSentinelPipelineCredentialsNone:

    def test_build_catalog_receives_none(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        with patch("aquamatch.sentinel_data.build_catalog") as mock_bc:
            run_sentinel_pipeline(
                csv=unique_csv, catalog_json=catalog_file, mode="catalog"
            )

        _, kwargs = mock_bc.call_args
        assert kwargs["credentials"] is None

    def test_run_download_receives_none(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)

        with patch("aquamatch.sentinel_data.run_download", return_value={}) as mock_rd:
            run_sentinel_pipeline(
                catalog_json=catalog_file, output_dir=tmp_path, mode="download"
            )

        _, kwargs = mock_rd.call_args
        assert kwargs["credentials"] is None


# ---------------------------------------------------------------------------
# credentials=<SentinelCredentials instance> — forwarded as-is
# ---------------------------------------------------------------------------


class TestRunSentinelPipelineCredentialsInstance:

    def test_forwarded_to_build_catalog(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)
        creds = SentinelCredentials(sh_client_id="explicit-id")

        with patch("aquamatch.sentinel_data.build_catalog") as mock_bc:
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
                credentials=creds,
            )

        _, kwargs = mock_bc.call_args
        assert kwargs["credentials"] is creds

    def test_forwarded_to_run_download(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        creds = SentinelCredentials(sh_client_id="explicit-id")

        with patch("aquamatch.sentinel_data.run_download", return_value={}) as mock_rd:
            run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="download",
                credentials=creds,
            )

        _, kwargs = mock_rd.call_args
        assert kwargs["credentials"] is creds

    def test_same_instance_forwarded_to_both_calls_in_mode_all(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)
        creds = SentinelCredentials(sh_client_id="explicit-id")

        with patch("aquamatch.sentinel_data.build_catalog") as mock_bc, patch(
            "aquamatch.sentinel_data.run_download", return_value={}
        ) as mock_rd:
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="all",
                credentials=creds,
            )

        _, bc_kwargs = mock_bc.call_args
        _, rd_kwargs = mock_rd.call_args
        assert bc_kwargs["credentials"] is creds
        assert rd_kwargs["credentials"] is creds


# ---------------------------------------------------------------------------
# credentials=<dict> — converted once, same instance reused
# ---------------------------------------------------------------------------


class TestRunSentinelPipelineCredentialsDict:

    def test_dict_converted_to_sentinel_credentials(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)
        creds_dict = {"sh_client_id": "dict-id", "dataspace_access_key": "dict-access"}

        with patch("aquamatch.sentinel_data.build_catalog") as mock_bc:
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
                credentials=creds_dict,
            )

        _, kwargs = mock_bc.call_args
        converted = kwargs["credentials"]
        assert isinstance(converted, SentinelCredentials)
        assert converted.sh_client_id == "dict-id"
        assert converted.dataspace_access_key == "dict-access"

    def test_same_converted_instance_reused_across_both_calls(self, tmp_path):
        """The dict must be converted ONCE — build_catalog() and
        run_download() must receive the identical SentinelCredentials
        object, not two independently-constructed ones."""
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)
        creds_dict = {"sh_client_id": "dict-id"}

        with patch("aquamatch.sentinel_data.build_catalog") as mock_bc, patch(
            "aquamatch.sentinel_data.run_download", return_value={}
        ) as mock_rd:
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="all",
                credentials=creds_dict,
            )

        _, bc_kwargs = mock_bc.call_args
        _, rd_kwargs = mock_rd.call_args
        assert bc_kwargs["credentials"] is rd_kwargs["credentials"]

    def test_empty_dict_produces_all_none_fields(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        with patch("aquamatch.sentinel_data.build_catalog") as mock_bc:
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
                credentials={},
            )

        _, kwargs = mock_bc.call_args
        converted = kwargs["credentials"]
        assert isinstance(converted, SentinelCredentials)
        assert converted.sh_client_id is None

    def test_unknown_dict_key_raises_typeerror(self, tmp_path):
        """A dict with an unrecognised field name must surface as a clear
        TypeError from the dataclass constructor, not silently ignored."""
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)

        with pytest.raises(TypeError):
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
                credentials={"not_a_real_field": "x"},
            )


# ---------------------------------------------------------------------------
# mode gating — credentials only reaches the call that actually runs
# ---------------------------------------------------------------------------


class TestRunSentinelPipelineCredentialsModeGating:

    def test_mode_catalog_does_not_call_run_download(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)
        creds = SentinelCredentials(sh_client_id="x")

        with patch("aquamatch.sentinel_data.build_catalog") as mock_bc, patch(
            "aquamatch.sentinel_data.run_download"
        ) as mock_rd:
            run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
                credentials=creds,
            )

        mock_bc.assert_called_once()
        mock_rd.assert_not_called()

    def test_mode_download_does_not_call_build_catalog(self, tmp_path):
        catalog_file = tmp_path / "catalog.json"
        _write_catalog_json(catalog_file)
        creds = SentinelCredentials(sh_client_id="x")

        with patch("aquamatch.sentinel_data.build_catalog") as mock_bc, patch(
            "aquamatch.sentinel_data.run_download", return_value={}
        ) as mock_rd:
            run_sentinel_pipeline(
                catalog_json=catalog_file,
                output_dir=tmp_path,
                mode="download",
                credentials=creds,
            )

        mock_bc.assert_not_called()
        mock_rd.assert_called_once()


# ---------------------------------------------------------------------------
# Regression — status dict shape / success path unaffected by credentials
# ---------------------------------------------------------------------------


class TestRunSentinelPipelineCredentialsRegression:

    def test_status_dict_shape_unchanged_with_credentials(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)
        creds = SentinelCredentials(sh_client_id="x")

        with patch("aquamatch.sentinel_data.build_catalog"):
            result = run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
                credentials=creds,
            )

        assert result["step"] == "sentinel"
        assert result["status"] == "success"
        assert result["error"] is None
        assert "elapsed_seconds" in result

    def test_error_from_build_catalog_still_reported_with_credentials(self, tmp_path):
        unique_csv = tmp_path / "unique.csv"
        catalog_file = tmp_path / "catalog.json"
        _write_unique_csv(unique_csv)
        creds = SentinelCredentials(sh_client_id="x")

        with patch(
            "aquamatch.sentinel_data.build_catalog",
            side_effect=RuntimeError("boom"),
        ):
            result = run_sentinel_pipeline(
                csv=unique_csv,
                catalog_json=catalog_file,
                mode="catalog",
                credentials=creds,
            )

        assert result["status"] == "error"
        assert "boom" in result["error"]
