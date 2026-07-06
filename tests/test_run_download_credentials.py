"""
Unit tests for aquamatch.sentinel_data.run_download() — Task 6.

run_download() must accept an optional `credentials` parameter, used to
build an S3 resource via build_clients() when `s3` is not explicitly
passed. Precedence (highest to lowest):

  1. explicit `s3` argument (Task 4)      — always wins if given
  2. `credentials` argument (Task 6)      — used to build s3 if s3 is None
  3. module-level `s3` global             — fallback when neither is given

This mirrors build_catalog()'s credentials wiring (Task 5), completing
the injection seam for the download path.

All S3 interaction and build_clients() are mocked — no real network
access or credentials used.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from aquamatch.credentials import SentinelCredentials
from aquamatch.sentinel_data import run_download

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SCENE_ID = "S2A_MSIL1C_20240315T135111_N0500_R024_T21HUD_20240315T160000"

_NOT_DOWNLOADED_STATUS = {
    "safe_exists": False,
    "scl_exists": False,
    "all_downloaded": False,
}


def _make_catalog(tmp_path: Path) -> Path:
    catalog_data = [
        {
            "field_date": "2024-03-15",
            "images_found": {
                "same_day": [
                    {
                        "id": _SCENE_ID,
                        "href": f"https://eodata.dataspace.copernicus.eu/eodata/{_SCENE_ID}/path",
                        "l2a_scl": None,
                        "delta_days": 0,
                        "cloud_cover": 5,
                        "datetime": "2024-03-15T13:51:11Z",
                    }
                ],
                "previous": [],
                "posterior": [],
            },
        }
    ]
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(catalog_data))
    return path


def _fake_s3_with_bucket():
    fake_bucket = MagicMock(name="fake_bucket")
    fake_s3 = MagicMock(name="fake_s3")
    fake_s3.Bucket.return_value = fake_bucket
    return fake_s3, fake_bucket


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------


class TestRunDownloadSignatureCredentials:

    def test_has_credentials_param(self):
        sig = inspect.signature(run_download)
        assert "credentials" in sig.parameters

    def test_credentials_defaults_to_none(self):
        sig = inspect.signature(run_download)
        assert sig.parameters["credentials"].default is None

    def test_s3_param_still_present(self):
        """Task 4's s3 param must remain — Task 6 adds credentials
        alongside it, not instead of it."""
        sig = inspect.signature(run_download)
        assert "s3" in sig.parameters
        assert sig.parameters["s3"].default is None


# ---------------------------------------------------------------------------
# credentials=None, s3=None — unchanged (module-level) behaviour
# ---------------------------------------------------------------------------


class TestRunDownloadNoOverrides:

    def test_build_clients_not_called_when_neither_given(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        fake_s3, fake_bucket = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.s3", fake_s3), patch(
            "aquamatch.sentinel_data.download_product"
        ), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ), patch(
            "aquamatch.sentinel_data.build_clients"
        ) as mock_build_clients:
            run_download(
                catalog, tmp_path, strategy="best", max_per_date=1, download_scl=False
            )

        mock_build_clients.assert_not_called()

    def test_module_level_s3_used_when_neither_given(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        fake_s3, fake_bucket = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.s3", fake_s3), patch(
            "aquamatch.sentinel_data.download_product"
        ) as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ):
            run_download(
                catalog, tmp_path, strategy="best", max_per_date=1, download_scl=False
            )

        args, _ = mock_dl.call_args
        assert args[0] is fake_bucket


# ---------------------------------------------------------------------------
# credentials provided, s3 not given — build via build_clients()
# ---------------------------------------------------------------------------


class TestRunDownloadCredentialsOnly:

    def test_build_clients_called_with_credentials(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        creds = SentinelCredentials(dataspace_access_key="explicit-access")
        fake_s3, fake_bucket = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.download_product"), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ), patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(MagicMock(), MagicMock(), fake_s3),
        ) as mock_build_clients:
            run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
                credentials=creds,
            )

        mock_build_clients.assert_called_once_with(creds)

    def test_s3_from_credentials_used_for_bucket_call(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        creds = SentinelCredentials(dataspace_access_key="explicit-access")
        fake_s3, fake_bucket = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.download_product") as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ), patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(MagicMock(), MagicMock(), fake_s3),
        ):
            run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
                credentials=creds,
            )

        fake_s3.Bucket.assert_called_once_with("eodata")
        args, _ = mock_dl.call_args
        assert args[0] is fake_bucket

    def test_module_level_s3_not_used_when_credentials_given(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        creds = SentinelCredentials(dataspace_access_key="explicit-access")
        module_s3, module_bucket = _fake_s3_with_bucket()
        creds_s3, creds_bucket = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.s3", module_s3), patch(
            "aquamatch.sentinel_data.download_product"
        ) as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ), patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(MagicMock(), MagicMock(), creds_s3),
        ):
            run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
                credentials=creds,
            )

        module_s3.Bucket.assert_not_called()
        args, _ = mock_dl.call_args
        assert args[0] is creds_bucket


# ---------------------------------------------------------------------------
# Precedence — explicit s3 wins over credentials when both are given
# ---------------------------------------------------------------------------


class TestRunDownloadPrecedence:

    def test_explicit_s3_wins_over_credentials(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        creds = SentinelCredentials(dataspace_access_key="should-be-ignored")
        explicit_s3, explicit_bucket = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.download_product") as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ), patch("aquamatch.sentinel_data.build_clients") as mock_build_clients:
            run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
                s3=explicit_s3,
                credentials=creds,
            )

        mock_build_clients.assert_not_called()
        explicit_s3.Bucket.assert_called_once_with("eodata")
        args, _ = mock_dl.call_args
        assert args[0] is explicit_bucket


# ---------------------------------------------------------------------------
# Regression — stats/behaviour unaffected
# ---------------------------------------------------------------------------


class TestRunDownloadCredentialsRegression:

    def test_stats_dict_unchanged_shape_with_credentials(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        creds = SentinelCredentials(dataspace_access_key="explicit-access")
        fake_s3, _ = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.download_product"), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ), patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(MagicMock(), MagicMock(), fake_s3),
        ):
            stats = run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
                credentials=creds,
            )

        for key in (
            "total_processed",
            "already_downloaded",
            "safe_downloaded",
            "scl_downloaded",
            "errors",
        ):
            assert key in stats
        assert stats["safe_downloaded"] == 1

    def test_already_downloaded_skips_build_clients_call_path_too(self, tmp_path):
        """Even with credentials passed, if the status check says everything
        is already downloaded, s3.Bucket()/download_product() must not be
        reached — build_clients() itself still runs (credentials resolved
        eagerly up front), but no download attempt follows."""
        catalog = _make_catalog(tmp_path)
        creds = SentinelCredentials(dataspace_access_key="explicit-access")
        fake_s3, fake_bucket = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.download_product") as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value={
                "safe_exists": True,
                "scl_exists": True,
                "all_downloaded": True,
            },
        ), patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(MagicMock(), MagicMock(), fake_s3),
        ):
            stats = run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=True,
                credentials=creds,
            )

        fake_s3.Bucket.assert_not_called()
        mock_dl.assert_not_called()
        assert stats["already_downloaded"] == 1
