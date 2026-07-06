"""
Unit tests for aquamatch.sentinel_data.build_catalog() — Task 5.

build_catalog() must accept an optional `credentials` parameter:
  - credentials=None (default): behaviour unchanged — search_images() is
    called with catalog=None, client=None, and falls back to its own
    module-level resolution (Task 3 default).
  - credentials=<SentinelCredentials>: build_clients(credentials) is
    called exactly ONCE (not once per field date/row), and the resulting
    catalog/client are passed explicitly into every search_images() call
    for this build_catalog() invocation.

All I/O (CSV reading, JSON writing) uses real tmp_path files, matching
the existing test_sentinel_data.py / test_sentinel_data_tile.py conventions.
Only search_images() and build_clients() are mocked.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from aquamatch.credentials import SentinelCredentials
from aquamatch.sentinel_data import build_catalog

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_csv(tmp_path: Path, n_dates: int = 1) -> Path:
    csv_file = tmp_path / "campaigns.csv"
    dates = [f"2025-08-0{i+1}" for i in range(n_dates)]
    pd.DataFrame(
        {
            "date": dates,
            "longitud": [-56.5] * n_dates,
            "latitud": [-32.85] * n_dates,
        }
    ).to_csv(csv_file, index=False)
    return csv_file


def _fake_image(date="2025-08-01"):
    return {
        "id": f"S2A_{date.replace('-', '')}T101031",
        "datetime": f"{date}T10:10:31.000Z",
        "cloud_cover": 5,
        "href": "https://fake-link.com/product",
        "delta_days": 0,
        "l2a_scl": None,
    }


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------


class TestBuildCatalogSignature:

    def test_has_credentials_param(self):
        sig = inspect.signature(build_catalog)
        assert "credentials" in sig.parameters

    def test_credentials_defaults_to_none(self):
        sig = inspect.signature(build_catalog)
        assert sig.parameters["credentials"].default is None

    def test_original_params_unchanged(self):
        sig = inspect.signature(build_catalog)
        names = list(sig.parameters.keys())
        assert names[:2] == ["csv_file", "output_json"]
        assert "time_delta" in names
        assert "cloud_cover" in names


# ---------------------------------------------------------------------------
# credentials=None — unchanged behaviour
# ---------------------------------------------------------------------------


class TestBuildCatalogCredentialsNone:

    def test_build_clients_not_called_when_credentials_none(self, tmp_path):
        csv_file = _make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[_fake_image()],
        ), patch("aquamatch.sentinel_data.build_clients") as mock_build_clients:
            build_catalog(csv_file, output_json)

        mock_build_clients.assert_not_called()

    def test_search_images_called_with_none_catalog_and_client(self, tmp_path):
        csv_file = _make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[_fake_image()],
        ) as mock_search:
            build_catalog(csv_file, output_json)

        _, kwargs = mock_search.call_args
        assert kwargs["catalog"] is None
        assert kwargs["client"] is None

    def test_output_json_still_written(self, tmp_path):
        csv_file = _make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[_fake_image()],
        ):
            build_catalog(csv_file, output_json)

        assert output_json.exists()
        data = json.loads(output_json.read_text())
        assert len(data) == 1


# ---------------------------------------------------------------------------
# credentials=<SentinelCredentials> — explicit client construction
# ---------------------------------------------------------------------------


class TestBuildCatalogCredentialsExplicit:

    def test_build_clients_called_with_credentials(self, tmp_path):
        csv_file = _make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        creds = SentinelCredentials(sh_client_id="explicit-id")

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[_fake_image()],
        ), patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(MagicMock(), MagicMock(), MagicMock()),
        ) as mock_build_clients:
            build_catalog(csv_file, output_json, credentials=creds)

        mock_build_clients.assert_called_once_with(creds)

    def test_search_images_receives_explicit_catalog_and_client(self, tmp_path):
        csv_file = _make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        creds = SentinelCredentials(sh_client_id="explicit-id")

        fake_catalog = MagicMock(name="fake_catalog")
        fake_client = MagicMock(name="fake_client")
        fake_s3 = MagicMock(name="fake_s3")

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[_fake_image()],
        ) as mock_search, patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(fake_catalog, fake_client, fake_s3),
        ):
            build_catalog(csv_file, output_json, credentials=creds)

        _, kwargs = mock_search.call_args
        assert kwargs["catalog"] is fake_catalog
        assert kwargs["client"] is fake_client

    def test_build_clients_called_exactly_once_across_multiple_dates(self, tmp_path):
        """Efficiency requirement: build_clients() must be called ONCE per
        build_catalog() invocation, not once per unique (date, location) row."""
        csv_file = _make_csv(tmp_path, n_dates=3)
        output_json = tmp_path / "catalog.json"
        creds = SentinelCredentials(sh_client_id="explicit-id")

        with patch(
            "aquamatch.sentinel_data.search_images",
            side_effect=lambda *a, **kw: [_fake_image(date=a[1])],
        ), patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(MagicMock(), MagicMock(), MagicMock()),
        ) as mock_build_clients:
            build_catalog(csv_file, output_json, credentials=creds)

        mock_build_clients.assert_called_once()

    def test_same_catalog_and_client_reused_across_multiple_dates(self, tmp_path):
        """The same catalog/client objects (not rebuilt) must be passed to
        every search_images() call within a single build_catalog() run."""
        csv_file = _make_csv(tmp_path, n_dates=3)
        output_json = tmp_path / "catalog.json"
        creds = SentinelCredentials(sh_client_id="explicit-id")

        fake_catalog = MagicMock(name="fake_catalog")
        fake_client = MagicMock(name="fake_client")

        with patch(
            "aquamatch.sentinel_data.search_images",
            side_effect=lambda *a, **kw: [_fake_image(date=a[1])],
        ) as mock_search, patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(fake_catalog, fake_client, MagicMock()),
        ):
            build_catalog(csv_file, output_json, credentials=creds)

        assert mock_search.call_count == 3
        for call in mock_search.call_args_list:
            _, kwargs = call
            assert kwargs["catalog"] is fake_catalog
            assert kwargs["client"] is fake_client

    def test_output_json_written_with_explicit_credentials(self, tmp_path):
        csv_file = _make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        creds = SentinelCredentials(sh_client_id="explicit-id")

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[_fake_image()],
        ), patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(MagicMock(), MagicMock(), MagicMock()),
        ):
            build_catalog(csv_file, output_json, credentials=creds)

        assert output_json.exists()
        data = json.loads(output_json.read_text())
        assert len(data) == 1

    def test_third_build_clients_return_value_s3_is_unused_here(self, tmp_path):
        """build_catalog() only needs (catalog, client); the s3 element of
        build_clients()'s 3-tuple is irrelevant to this function and must
        not be passed anywhere or cause an error if it's None-ish."""
        csv_file = _make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        creds = SentinelCredentials(sh_client_id="explicit-id")

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[_fake_image()],
        ), patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(MagicMock(), MagicMock(), None),
        ):
            # Should not raise despite s3 being None
            build_catalog(csv_file, output_json, credentials=creds)

        assert output_json.exists()


# ---------------------------------------------------------------------------
# Regression — tile filtering / bucketing logic unaffected by credentials
# ---------------------------------------------------------------------------


class TestBuildCatalogRegressionWithCredentials:
    """Confirms credentials wiring doesn't disturb existing catalog-building
    logic (tile filtering, bucketing) — mirrors a subset of
    tests/test_sentinel_data_tile.py, but with credentials explicitly set."""

    def _fake_image_with_tile(self, tile="21HUD"):
        scene_id = f"S2A_MSIL1C_20250801T101031_N0500_R024_T{tile}_20230919T094731"
        return {
            "id": scene_id,
            "datetime": "2025-08-01T10:10:31.000Z",
            "cloud_cover": 5,
            "href": "https://eodata.dataspace.copernicus.eu/eodata/fake/path",
            "delta_days": 0,
            "l2a_scl": None,
        }

    def test_tile_filtering_still_works_with_explicit_credentials(self, tmp_path):
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {
                "date": ["2025-08-01"],
                "longitud": [-56.5],
                "latitud": [-32.85],
                "s2_tile": ["21HUD"],
            }
        ).to_csv(csv_file, index=False)
        output_json = tmp_path / "catalog.json"
        creds = SentinelCredentials(sh_client_id="explicit-id")

        with patch(
            "aquamatch.sentinel_data.search_images",
            return_value=[
                self._fake_image_with_tile("21HUD"),
                self._fake_image_with_tile("21HVD"),
            ],
        ), patch(
            "aquamatch.sentinel_data.build_clients",
            return_value=(MagicMock(), MagicMock(), MagicMock()),
        ):
            build_catalog(csv_file, output_json, credentials=creds)

        data = json.loads(output_json.read_text())
        images = (
            data[0]["images_found"]["same_day"]
            + data[0]["images_found"]["previous"]
            + data[0]["images_found"]["posterior"]
        )
        assert len(images) == 1
        assert "T21HUD" in images[0]["id"]
