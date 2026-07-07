"""
Unit tests for aquamatch.sentinel_data.search_images() — Task 3.

search_images() must accept optional `catalog` and `client` overrides,
falling back to the module-level `catalog`/`client` globals (built by
build_clients()) when neither is provided. This is the credential/client
injection seam needed for build_catalog() (Task 5) and, transitively,
run_sentinel_pipeline(credentials=...) (Task 7).

All SentinelHubCatalog / pystac_client.Client interactions are mocked —
no real network access or credentials are used.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from aquamatch.sentinel_data import create_bbox_from_point, search_images

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fake_l1c_item(date="2025-08-01", cloud=5, item_id=None):
    return {
        "id": item_id or f"S2A_{date.replace('-', '')}T101031",
        "properties": {
            "datetime": f"{date}T10:10:31.000Z",
            "eo:cloud_cover": cloud,
        },
        "assets": {
            "data": {"href": "https://eodata.dataspace.copernicus.eu/eodata/fake/path"}
        },
    }


def _make_fake_l2a_item(scl_href="https://fake-l2a-link.com/SCL.tif"):
    mock_item = MagicMock()
    mock_scl = MagicMock()
    mock_scl.href = scl_href
    mock_item.assets = {"scl": mock_scl}
    return mock_item


def _make_fake_catalog(l1c_items):
    """A fake SentinelHubCatalog-like object with a .search() method."""
    fake = MagicMock()
    fake.search.return_value = iter(l1c_items)
    return fake


def _make_fake_client(l2a_items):
    """A fake pystac_client.Client-like object with a .search().items() chain."""
    fake = MagicMock()
    mock_search = MagicMock()
    mock_search.items.return_value = l2a_items
    fake.search.return_value = mock_search
    return fake


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------


class TestSearchImagesSignature:

    def test_has_catalog_and_client_params(self):
        sig = inspect.signature(search_images)
        assert "catalog" in sig.parameters
        assert "client" in sig.parameters

    def test_catalog_and_client_default_to_none(self):
        sig = inspect.signature(search_images)
        assert sig.parameters["catalog"].default is None
        assert sig.parameters["client"].default is None

    def test_original_positional_params_unchanged(self):
        """bbox_geometry, date, time_delta, cloud_cover must still be
        positional — existing call sites must not break."""
        sig = inspect.signature(search_images)
        names = list(sig.parameters.keys())
        assert names[:4] == ["bbox_geometry", "date", "time_delta", "cloud_cover"]


# ---------------------------------------------------------------------------
# Default behaviour — falls back to module-level catalog/client
# ---------------------------------------------------------------------------


class TestSearchImagesDefaultsToModuleGlobals:
    """When catalog/client are not passed, the module-level globals
    (aquamatch.sentinel_data.catalog / .client) must be used — this is
    the existing behaviour and must not regress."""

    def test_uses_module_level_catalog_when_not_provided(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        fake_catalog = _make_fake_catalog([_make_fake_l1c_item()])
        fake_client = _make_fake_client([_make_fake_l2a_item()])

        with patch("aquamatch.sentinel_data.catalog", fake_catalog), patch(
            "aquamatch.sentinel_data.client", fake_client
        ):
            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)

        fake_catalog.search.assert_called_once()
        assert len(result) == 1

    def test_uses_module_level_client_when_not_provided(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        fake_catalog = _make_fake_catalog([_make_fake_l1c_item()])
        fake_client = _make_fake_client([_make_fake_l2a_item()])

        with patch("aquamatch.sentinel_data.catalog", fake_catalog), patch(
            "aquamatch.sentinel_data.client", fake_client
        ):
            search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)

        fake_client.search.assert_called_once()

    def test_explicit_none_falls_back_to_module_globals(self):
        """Passing catalog=None / client=None explicitly must behave
        identically to omitting them entirely."""
        bbox = create_bbox_from_point(-56.5, -32.85)
        fake_catalog = _make_fake_catalog([_make_fake_l1c_item()])
        fake_client = _make_fake_client([_make_fake_l2a_item()])

        with patch("aquamatch.sentinel_data.catalog", fake_catalog), patch(
            "aquamatch.sentinel_data.client", fake_client
        ):
            result = search_images(
                bbox,
                "2025-08-01",
                time_delta=1,
                cloud_cover=10,
                catalog=None,
                client=None,
            )

        fake_catalog.search.assert_called_once()
        fake_client.search.assert_called_once()
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Explicit overrides take precedence
# ---------------------------------------------------------------------------


class TestSearchImagesExplicitOverrides:

    def test_explicit_catalog_used_instead_of_module_level(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        module_catalog = _make_fake_catalog(
            [_make_fake_l1c_item(item_id="SHOULD_NOT_APPEAR")]
        )
        explicit_catalog = _make_fake_catalog(
            [_make_fake_l1c_item(item_id="EXPLICIT_WINS")]
        )
        fake_client = _make_fake_client([_make_fake_l2a_item()])

        with patch("aquamatch.sentinel_data.catalog", module_catalog), patch(
            "aquamatch.sentinel_data.client", fake_client
        ):
            result = search_images(
                bbox,
                "2025-08-01",
                time_delta=1,
                cloud_cover=10,
                catalog=explicit_catalog,
            )

        module_catalog.search.assert_not_called()
        explicit_catalog.search.assert_called_once()
        assert result[0]["id"] == "EXPLICIT_WINS"

    def test_explicit_client_used_instead_of_module_level(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        fake_catalog = _make_fake_catalog([_make_fake_l1c_item()])
        module_client = _make_fake_client(
            [_make_fake_l2a_item("https://module-level.com/SCL.tif")]
        )
        explicit_client = _make_fake_client(
            [_make_fake_l2a_item("https://explicit.com/SCL.tif")]
        )

        with patch("aquamatch.sentinel_data.catalog", fake_catalog), patch(
            "aquamatch.sentinel_data.client", module_client
        ):
            result = search_images(
                bbox,
                "2025-08-01",
                time_delta=1,
                cloud_cover=10,
                client=explicit_client,
            )

        module_client.search.assert_not_called()
        explicit_client.search.assert_called_once()
        assert result[0]["l2a_scl"] == ["https://explicit.com/SCL.tif"]

    def test_both_explicit_overrides_used_together(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        module_catalog = _make_fake_catalog([_make_fake_l1c_item()])
        module_client = _make_fake_client([_make_fake_l2a_item()])
        explicit_catalog = _make_fake_catalog(
            [_make_fake_l1c_item(item_id="BOTH_EXPLICIT")]
        )
        explicit_client = _make_fake_client([_make_fake_l2a_item()])

        with patch("aquamatch.sentinel_data.catalog", module_catalog), patch(
            "aquamatch.sentinel_data.client", module_client
        ):
            result = search_images(
                bbox,
                "2025-08-01",
                time_delta=1,
                cloud_cover=10,
                catalog=explicit_catalog,
                client=explicit_client,
            )

        module_catalog.search.assert_not_called()
        module_client.search.assert_not_called()
        explicit_catalog.search.assert_called_once()
        explicit_client.search.assert_called_once()
        assert result[0]["id"] == "BOTH_EXPLICIT"

    def test_explicit_catalog_search_receives_correct_query_params(self):
        """Sanity check: overriding the client object must not change the
        query parameters passed to .search() (bbox/time/cloud filter)."""
        bbox = create_bbox_from_point(-56.5, -32.85)
        explicit_catalog = _make_fake_catalog([])  # empty -> short-circuits early
        fake_client = _make_fake_client([])

        with patch("aquamatch.sentinel_data.client", fake_client):
            search_images(
                bbox,
                "2025-08-01",
                time_delta=2,
                cloud_cover=15,
                catalog=explicit_catalog,
            )

        _, kwargs = explicit_catalog.search.call_args
        assert kwargs["bbox"] == bbox
        assert kwargs["filter"] == "eo:cloud_cover < 15"
        assert kwargs["time"] == ("2025-07-30", "2025-08-03")


# ---------------------------------------------------------------------------
# Regression — return shape unaffected by the new params
# ---------------------------------------------------------------------------


class TestSearchImagesReturnShapeUnaffected:

    def test_returns_list_of_dicts_with_expected_keys(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        explicit_catalog = _make_fake_catalog([_make_fake_l1c_item()])
        explicit_client = _make_fake_client([_make_fake_l2a_item()])

        result = search_images(
            bbox,
            "2025-08-01",
            time_delta=1,
            cloud_cover=10,
            catalog=explicit_catalog,
            client=explicit_client,
        )

        assert isinstance(result, list)
        assert len(result) == 1
        for key in ("id", "datetime", "cloud_cover", "href", "delta_days", "l2a_scl"):
            assert key in result[0]

    def test_empty_l1c_results_returns_empty_list_with_explicit_catalog(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        explicit_catalog = _make_fake_catalog([])
        explicit_client = _make_fake_client([])

        result = search_images(
            bbox,
            "2025-08-01",
            time_delta=1,
            cloud_cover=10,
            catalog=explicit_catalog,
            client=explicit_client,
        )

        assert result == []
        # client.search must never be reached when there are no L1C results
        explicit_client.search.assert_not_called()
