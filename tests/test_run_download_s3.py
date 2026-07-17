"""
Unit tests for aquamatch.sentinel_data.run_download() — Task 4.

run_download() must accept an optional `s3` override, falling back to the
module-level `s3` global (built by build_clients()) when not provided.
This mirrors the search_images() catalog/client injection from Task 3,
completing the credential-injection seam for the download path.

download_product() itself is unaffected — it already accepted an explicit
`bucket` argument; only run_download()'s resolution of *which* S3 resource
builds that bucket is new here.

All S3 interaction is mocked — no real network access or credentials used.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

from aquamatch.sentinel_data import run_download

from .conftest import _fake_s3_with_bucket, _make_catalog

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOT_DOWNLOADED_STATUS = {
    "safe_exists": False,
    "scl_exists": False,
    "all_downloaded": False,
}


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------


class TestRunDownloadSignature:

    def test_has_s3_param(self):
        sig = inspect.signature(run_download)
        assert "s3" in sig.parameters

    def test_s3_defaults_to_none(self):
        sig = inspect.signature(run_download)
        assert sig.parameters["s3"].default is None

    def test_original_params_unchanged(self):
        sig = inspect.signature(run_download)
        names = list(sig.parameters.keys())
        assert names[:2] == ["catalog_json", "output_dir"]
        for name in ("strategy", "max_per_date", "max_cloud_cover", "download_scl"):
            assert name in names


# ---------------------------------------------------------------------------
# Default behaviour — falls back to module-level s3
# ---------------------------------------------------------------------------


class TestRunDownloadDefaultsToModuleGlobal:
    """When s3 is not passed, the module-level aquamatch.sentinel_data.s3
    must be used — existing behaviour, must not regress."""

    def test_uses_module_level_s3_when_not_provided(self, tmp_path):
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

        fake_s3.Bucket.assert_called_once_with("eodata")
        mock_dl.assert_called_once()
        args, _ = mock_dl.call_args
        assert args[0] is fake_bucket

    def test_explicit_none_falls_back_to_module_global(self, tmp_path):
        """Passing s3=None explicitly must behave identically to omitting it."""
        catalog = _make_catalog(tmp_path)
        fake_s3, fake_bucket = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.s3", fake_s3), patch(
            "aquamatch.sentinel_data.download_product"
        ) as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ):
            run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
                s3=None,
            )

        args, _ = mock_dl.call_args
        assert args[0] is fake_bucket


# ---------------------------------------------------------------------------
# Explicit override takes precedence
# ---------------------------------------------------------------------------


class TestRunDownloadExplicitS3Override:

    def test_explicit_s3_used_instead_of_module_level(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        module_s3, module_bucket = _fake_s3_with_bucket()
        explicit_s3, explicit_bucket = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.s3", module_s3), patch(
            "aquamatch.sentinel_data.download_product"
        ) as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ):
            run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
                s3=explicit_s3,
            )

        module_s3.Bucket.assert_not_called()
        explicit_s3.Bucket.assert_called_once_with("eodata")
        args, _ = mock_dl.call_args
        assert args[0] is explicit_bucket
        assert args[0] is not module_bucket

    def test_explicit_s3_bucket_name_is_eodata(self, tmp_path):
        """Regardless of which s3 resource is used, the bucket name
        requested must always be 'eodata' (Copernicus Dataspace)."""
        catalog = _make_catalog(tmp_path)
        explicit_s3, _ = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.download_product"), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ):
            run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
                s3=explicit_s3,
            )

        explicit_s3.Bucket.assert_called_once_with("eodata")


# ---------------------------------------------------------------------------
# Regression — stats / behaviour unaffected by the new param
# ---------------------------------------------------------------------------


class TestRunDownloadStatsUnaffectedByInjection:

    def test_stats_dict_unchanged_shape(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        explicit_s3, _ = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.download_product"), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ):
            stats = run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
                s3=explicit_s3,
            )

        for key in (
            "total_processed",
            "already_downloaded",
            "safe_downloaded",
            "scl_downloaded",
            "errors",
        ):
            assert key in stats

    def test_already_downloaded_skips_bucket_call_entirely(self, tmp_path):
        """When status.all_downloaded is True, neither s3.Bucket() nor
        download_product() should be reached — injection must not change
        this short-circuit."""
        catalog = _make_catalog(tmp_path)
        explicit_s3, _ = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.download_product") as mock_dl, patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value={
                "safe_exists": True,
                "scl_exists": True,
                "all_downloaded": True,
            },
        ):
            stats = run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=True,
                s3=explicit_s3,
            )

        explicit_s3.Bucket.assert_not_called()
        mock_dl.assert_not_called()
        assert stats["already_downloaded"] == 1

    def test_safe_downloaded_count_correct_with_explicit_s3(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        explicit_s3, _ = _fake_s3_with_bucket()

        with patch("aquamatch.sentinel_data.download_product"), patch(
            "aquamatch.sentinel_data.get_download_status",
            return_value=_NOT_DOWNLOADED_STATUS,
        ):
            stats = run_download(
                catalog,
                tmp_path,
                strategy="best",
                max_per_date=1,
                download_scl=False,
                s3=explicit_s3,
            )

        assert stats["safe_downloaded"] == 1
        assert stats["total_processed"] == 1
