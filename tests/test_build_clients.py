"""
Unit tests for aquamatch.sentinel_data.build_clients() — Task 2.

build_clients() constructs (catalog, client, s3) from an explicit
SentinelCredentials instance, or falls back to SentinelCredentials.from_env()
when none is given.

All three underlying constructors (SHConfig/SentinelHubCatalog, pystac_client's
Client.open, boto3.resource) are patched at their call sites in
aquamatch.sentinel_data so no real network access or credentials are needed.
The conftest.py-level patch of pystac_client.Client.open (module import time)
is independent of these tests, which patch the same target explicitly again
per-test for full control over the return value.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from aquamatch.credentials import SentinelCredentials
from aquamatch.sentinel_data import build_clients, earthsearch_catalog_url

# ---------------------------------------------------------------------------
# Return value / wiring
# ---------------------------------------------------------------------------


class TestBuildClientsReturnValue:
    """build_clients() must return a 3-tuple (catalog, client, s3)."""

    def test_returns_three_values(self):
        with patch("aquamatch.sentinel_data.SHConfig"), patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            result = build_clients(SentinelCredentials())

        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_catalog_is_sentinelhubcatalog_instance(self):
        with patch("aquamatch.sentinel_data.SHConfig"), patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ) as mock_catalog_cls, patch(
            "aquamatch.sentinel_data.Client"
        ) as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_catalog_cls.return_value = "FAKE_CATALOG"
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            catalog_, client_, s3_ = build_clients(SentinelCredentials())

        assert catalog_ == "FAKE_CATALOG"

    def test_client_comes_from_client_open(self):
        with patch("aquamatch.sentinel_data.SHConfig"), patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_client_cls.open.return_value = "FAKE_CLIENT"
            mock_boto3.resource.return_value = MagicMock()

            catalog_, client_, s3_ = build_clients(SentinelCredentials())

        assert client_ == "FAKE_CLIENT"

    def test_s3_comes_from_boto3_resource(self):
        with patch("aquamatch.sentinel_data.SHConfig"), patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = "FAKE_S3"

            catalog_, client_, s3_ = build_clients(SentinelCredentials())

        assert s3_ == "FAKE_S3"


# ---------------------------------------------------------------------------
# Client.open is called with the EarthSearch URL
# ---------------------------------------------------------------------------


class TestBuildClientsEarthSearch:

    def test_client_open_called_with_earthsearch_url(self):
        with patch("aquamatch.sentinel_data.SHConfig"), patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(SentinelCredentials())

        mock_client_cls.open.assert_called_once_with(earthsearch_catalog_url)


# ---------------------------------------------------------------------------
# Explicit credentials are wired into SHConfig
# ---------------------------------------------------------------------------


class TestBuildClientsExplicitCredentials:

    def test_sh_client_id_set_on_config(self):
        creds = SentinelCredentials(sh_client_id="explicit-id")
        with patch("aquamatch.sentinel_data.SHConfig") as mock_sh_config_cls, patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_sh_config_instance = MagicMock()
            mock_sh_config_cls.return_value = mock_sh_config_instance
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(creds)

        assert mock_sh_config_instance.sh_client_id == "explicit-id"

    def test_sh_client_secret_set_on_config(self):
        creds = SentinelCredentials(sh_client_secret="explicit-secret")
        with patch("aquamatch.sentinel_data.SHConfig") as mock_sh_config_cls, patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_sh_config_instance = MagicMock()
            mock_sh_config_cls.return_value = mock_sh_config_instance
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(creds)

        assert mock_sh_config_instance.sh_client_secret == "explicit-secret"

    def test_sh_base_url_set_on_config(self):
        creds = SentinelCredentials(sh_base_url="https://custom.example.com")
        with patch("aquamatch.sentinel_data.SHConfig") as mock_sh_config_cls, patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_sh_config_instance = MagicMock()
            mock_sh_config_cls.return_value = mock_sh_config_instance
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(creds)

        assert mock_sh_config_instance.sh_base_url == "https://custom.example.com"

    def test_sh_token_url_set_on_config(self):
        creds = SentinelCredentials(sh_token_url="https://custom.example.com/token")
        with patch("aquamatch.sentinel_data.SHConfig") as mock_sh_config_cls, patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_sh_config_instance = MagicMock()
            mock_sh_config_cls.return_value = mock_sh_config_instance
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(creds)

        assert (
            mock_sh_config_instance.sh_token_url == "https://custom.example.com/token"
        )

    def test_sentinelhubcatalog_constructed_with_config(self):
        creds = SentinelCredentials(sh_client_id="explicit-id")
        with patch("aquamatch.sentinel_data.SHConfig") as mock_sh_config_cls, patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ) as mock_catalog_cls, patch(
            "aquamatch.sentinel_data.Client"
        ) as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_sh_config_instance = MagicMock()
            mock_sh_config_cls.return_value = mock_sh_config_instance
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(creds)

        mock_catalog_cls.assert_called_once_with(config=mock_sh_config_instance)


# ---------------------------------------------------------------------------
# Explicit credentials are wired into boto3.resource
# ---------------------------------------------------------------------------


class TestBuildClientsS3Wiring:

    def test_dataspace_access_key_forwarded(self):
        creds = SentinelCredentials(dataspace_access_key="explicit-access")
        with patch("aquamatch.sentinel_data.SHConfig"), patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(creds)

        _, kwargs = mock_boto3.resource.call_args
        assert kwargs["aws_access_key_id"] == "explicit-access"

    def test_dataspace_secret_key_forwarded(self):
        creds = SentinelCredentials(dataspace_secret_key="explicit-secretkey")
        with patch("aquamatch.sentinel_data.SHConfig"), patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(creds)

        _, kwargs = mock_boto3.resource.call_args
        assert kwargs["aws_secret_access_key"] == "explicit-secretkey"

    def test_s3_endpoint_url_is_dataspace(self):
        with patch("aquamatch.sentinel_data.SHConfig"), patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(SentinelCredentials())

        _, kwargs = mock_boto3.resource.call_args
        assert kwargs["endpoint_url"] == "https://eodata.dataspace.copernicus.eu"

    def test_s3_resource_type_is_s3(self):
        with patch("aquamatch.sentinel_data.SHConfig"), patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(SentinelCredentials())

        args, _ = mock_boto3.resource.call_args
        assert args[0] == "s3"

    def test_region_name_is_default(self):
        with patch("aquamatch.sentinel_data.SHConfig"), patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(SentinelCredentials())

        _, kwargs = mock_boto3.resource.call_args
        assert kwargs["region_name"] == "default"


# ---------------------------------------------------------------------------
# credentials=None falls back to SentinelCredentials.from_env()
# ---------------------------------------------------------------------------


class TestBuildClientsFallbackToEnv:

    def test_none_credentials_calls_from_env(self, monkeypatch):
        monkeypatch.setenv("SH_CLIENT_ID", "env-id")
        monkeypatch.setenv("SH_CLIENT_SECRET", "env-secret")
        monkeypatch.setenv("DATASPACE_ACCESS_KEY", "env-access")
        monkeypatch.setenv("DATASPACE_SECRET_KEY", "env-secretkey")

        with patch("aquamatch.sentinel_data.SHConfig") as mock_sh_config_cls, patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_sh_config_instance = MagicMock()
            mock_sh_config_cls.return_value = mock_sh_config_instance
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(None)

        assert mock_sh_config_instance.sh_client_id == "env-id"
        assert mock_sh_config_instance.sh_client_secret == "env-secret"

    def test_no_args_at_all_uses_env(self, monkeypatch):
        """Calling build_clients() with zero arguments must behave like
        build_clients(None) — falls back to SentinelCredentials.from_env()."""
        monkeypatch.setenv("SH_CLIENT_ID", "env-id-2")

        with patch("aquamatch.sentinel_data.SHConfig") as mock_sh_config_cls, patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_sh_config_instance = MagicMock()
            mock_sh_config_cls.return_value = mock_sh_config_instance
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients()

        assert mock_sh_config_instance.sh_client_id == "env-id-2"

    def test_explicit_credentials_are_not_overridden_by_env(self, monkeypatch):
        """When credentials ARE passed explicitly, env vars must be ignored
        entirely — this is the whole point of Task 1/2 (Colab support)."""
        monkeypatch.setenv("SH_CLIENT_ID", "env-id-should-be-ignored")
        creds = SentinelCredentials(sh_client_id="explicit-id-wins")

        with patch("aquamatch.sentinel_data.SHConfig") as mock_sh_config_cls, patch(
            "aquamatch.sentinel_data.SentinelHubCatalog"
        ), patch("aquamatch.sentinel_data.Client") as mock_client_cls, patch(
            "aquamatch.sentinel_data.boto3"
        ) as mock_boto3:
            mock_sh_config_instance = MagicMock()
            mock_sh_config_cls.return_value = mock_sh_config_instance
            mock_client_cls.open.return_value = MagicMock()
            mock_boto3.resource.return_value = MagicMock()

            build_clients(creds)

        assert mock_sh_config_instance.sh_client_id == "explicit-id-wins"


# ---------------------------------------------------------------------------
# Regression — module-level catalog/client/s3 still exist after import
# ---------------------------------------------------------------------------


class TestModuleLevelClientsStillExist:
    """
    build_clients() replacing the old inline construction must not remove
    the module-level catalog/client/s3 names that existing tests patch
    (e.g. patch("aquamatch.sentinel_data.catalog")).
    """

    def test_module_level_catalog_exists(self):
        import aquamatch.sentinel_data as sd

        assert hasattr(sd, "catalog")

    def test_module_level_client_exists(self):
        import aquamatch.sentinel_data as sd

        assert hasattr(sd, "client")

    def test_module_level_s3_exists(self):
        import aquamatch.sentinel_data as sd

        assert hasattr(sd, "s3")

    def test_build_clients_is_importable(self):
        from aquamatch.sentinel_data import build_clients  # noqa: F401
