import json
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

import geopandas as gpd
import pandas as pd
import pytest
from sentinelhub import BBox, CRS
from shapely.geometry import Polygon

from rionegromatchup.sentinel_pipeline import (
    create_bbox_from_point,
    search_images,
    build_catalog,
    run_download,
    get_download_status,
    download_scl_asset,
)
from rionegromatchup.Organizing import (
    setup_names,
    clean_campaigns,
    merge_stations_campaigns,
    clean_value,
)

# ==============================================================================
# sentinel_pipeline.py tests
# ==============================================================================


class TestCreateBboxFromPoint:
    """Tests for create_bbox_from_point."""

    def test_returns_bbox_instance(self):
        bbox = create_bbox_from_point(lon=-56.5, lat=-32.85)
        assert isinstance(bbox, BBox)

    def test_crs_is_wgs84(self):
        bbox = create_bbox_from_point(lon=-56.5, lat=-32.85)
        assert bbox.crs == CRS.WGS84

    def test_default_buffer_expands_correctly(self):
        lon, lat, buffer = -56.5, -32.85, 0.01
        bbox = create_bbox_from_point(lon=lon, lat=lat, buffer_degrees=buffer)
        min_lon, min_lat, max_lon, max_lat = list(bbox)
        assert min_lon == pytest.approx(lon - buffer)
        assert min_lat == pytest.approx(lat - buffer)
        assert max_lon == pytest.approx(lon + buffer)
        assert max_lat == pytest.approx(lat + buffer)

    def test_custom_buffer(self):
        lon, lat, buffer = -56.5, -32.85, 0.05
        bbox = create_bbox_from_point(lon=lon, lat=lat, buffer_degrees=buffer)
        min_lon, min_lat, max_lon, max_lat = list(bbox)
        assert min_lon == pytest.approx(lon - buffer)
        assert max_lon == pytest.approx(lon + buffer)


class TestSearchImages:
    """Tests for search_images."""

    def _make_fake_l1c_item(self, date="2025-08-01", cloud=5):
        return {
            "id": f"S2A_{date.replace('-', '')}T101031",
            "properties": {
                "datetime": f"{date}T10:10:31.000Z",
                "eo:cloud_cover": cloud,
            },
            "assets": {
                "data": {
                    "href": "https://eodata.dataspace.copernicus.eu/eodata/fake/path"
                }
            },
        }

    def _make_fake_l2a_item(self):
        mock_item = MagicMock()
        mock_scl = MagicMock()
        mock_scl.href = "https://fake-l2a-link.com/SCL.tif"
        mock_item.assets = {"scl": mock_scl}
        return mock_item

    def test_returns_list(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        with patch("rionegromatchup.sentinel_pipeline.catalog") as mock_catalog, patch(
            "rionegromatchup.sentinel_pipeline.client"
        ) as mock_client:

            mock_catalog.search.return_value = iter([self._make_fake_l1c_item()])
            mock_search = MagicMock()
            mock_search.items.return_value = [self._make_fake_l2a_item()]
            mock_client.search.return_value = mock_search

            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert isinstance(result, list)

    def test_returns_correct_keys(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        with patch("rionegromatchup.sentinel_pipeline.catalog") as mock_catalog, patch(
            "rionegromatchup.sentinel_pipeline.client"
        ) as mock_client:

            mock_catalog.search.return_value = iter([self._make_fake_l1c_item()])
            mock_search = MagicMock()
            mock_search.items.return_value = [self._make_fake_l2a_item()]
            mock_client.search.return_value = mock_search

            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert len(result) == 1
            for key in [
                "id",
                "datetime",
                "cloud_cover",
                "href",
                "delta_days",
                "l2a_cls",
            ]:
                assert key in result[0]

    def test_delta_days_computed_correctly(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        field_date = "2025-08-01"
        acquisition_date = "2025-08-02"

        with patch("rionegromatchup.sentinel_pipeline.catalog") as mock_catalog, patch(
            "rionegromatchup.sentinel_pipeline.client"
        ) as mock_client:

            mock_catalog.search.return_value = iter(
                [self._make_fake_l1c_item(date=acquisition_date)]
            )
            mock_search = MagicMock()
            mock_search.items.return_value = [self._make_fake_l2a_item()]
            mock_client.search.return_value = mock_search

            result = search_images(bbox, field_date, time_delta=2, cloud_cover=10)
            assert result[0]["delta_days"] == 1

    def test_returns_empty_when_no_l1c_found(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        with patch("rionegromatchup.sentinel_pipeline.catalog") as mock_catalog:
            mock_catalog.search.return_value = iter([])
            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert result == []

    def test_l2a_cls_is_none_when_no_l2a_found(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        with patch("rionegromatchup.sentinel_pipeline.catalog") as mock_catalog, patch(
            "rionegromatchup.sentinel_pipeline.client"
        ) as mock_client:

            mock_catalog.search.return_value = iter([self._make_fake_l1c_item()])
            mock_search = MagicMock()
            mock_search.items.return_value = []  # no L2A found
            mock_client.search.return_value = mock_search

            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert result[0]["l2a_cls"] is None


class TestBuildCatalog:
    """Tests for build_catalog — Point 2 fix."""

    def _make_csv(self, tmp_path) -> Path:
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {
                "date": ["2025-08-01", "2025-08-02"],
                "longitud": [-56.5, -56.5],
                "latitud": [-32.85, -32.85],
            }
        ).to_csv(csv_file, index=False, sep=";")
        return csv_file

    def test_creates_json_output(self, tmp_path):
        csv_file = self._make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        fake_image = {
            "id": "S2A_20250801T101031",
            "datetime": "2025-08-01T10:10:31.000Z",
            "cloud_cover": 5,
            "href": "https://fake-link.com/product",
            "delta_days": 0,
            "l2a_cls": "https://fake-link.com/SCL.tif",
        }
        with patch(
            "rionegromatchup.sentinel_pipeline.search_images", return_value=[fake_image]
        ):
            build_catalog(csv_file, output_json, time_delta=1, cloud_cover=10)

        assert output_json.exists()

    def test_output_has_correct_structure(self, tmp_path):
        csv_file = self._make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        fake_image = {
            "id": "S2A_20250801T101031",
            "datetime": "2025-08-01T10:10:31.000Z",
            "cloud_cover": 5,
            "href": "https://fake-link.com/product",
            "delta_days": 0,
            "l2a_cls": "https://fake-link.com/SCL.tif",
        }
        with patch(
            "rionegromatchup.sentinel_pipeline.search_images", return_value=[fake_image]
        ):
            build_catalog(csv_file, output_json, time_delta=1, cloud_cover=10)

        with open(output_json) as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 2  # two unique date/location rows
        for entry in data:
            assert "field_date" in entry
            assert "images_found" in entry
            assert isinstance(entry["images_found"], list)

    def test_raises_on_missing_date_column(self, tmp_path):
        csv_file = tmp_path / "bad.csv"
        pd.DataFrame({"longitud": [-56.5], "latitud": [-32.85]}).to_csv(
            csv_file, index=False, sep=";"
        )
        with pytest.raises(ValueError, match="date"):
            build_catalog(csv_file, tmp_path / "out.json")

    def test_raises_on_missing_coordinate_columns(self, tmp_path):
        csv_file = tmp_path / "bad.csv"
        pd.DataFrame({"date": ["2025-08-01"]}).to_csv(csv_file, index=False, sep=";")
        with pytest.raises(ValueError, match="longitud"):
            build_catalog(csv_file, tmp_path / "out.json")

    def test_deduplicates_same_scene_across_stations(self, tmp_path):
        """Same scene returned for two different station points on the same date
        should appear only once in the catalog."""
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {
                "date": ["2025-08-01", "2025-08-01"],  # same date
                "longitud": [-56.5, -56.6],  # different stations
                "latitud": [-32.85, -32.90],
            }
        ).to_csv(csv_file, index=False, sep=";")

        output_json = tmp_path / "catalog.json"

        # Both stations return the same scene ID
        fake_image = {
            "id": "S2A_20250801T101031",  # same ID
            "datetime": "2025-08-01T10:10:31.000Z",
            "cloud_cover": 5,
            "href": "https://fake-link.com/product",
            "delta_days": 0,
            "l2a_cls": "https://fake-link.com/SCL.tif",
        }

        with patch(
            "rionegromatchup.sentinel_pipeline.search_images", return_value=[fake_image]
        ):
            build_catalog(csv_file, output_json, time_delta=1, cloud_cover=10)

        with open(output_json) as f:
            data = json.load(f)

        assert len(data) == 1  # one date entry
        assert len(data[0]["images_found"]) == 1  # scene appears only once
        assert data[0]["images_found"][0]["id"] == "S2A_20250801T101031"


class TestGetDownloadStatus:
    """Tests for get_download_status."""

    def test_safe_folder_exists_and_not_empty(self, tmp_path):
        product_id = "S2A_MSIL1C_20250801"
        safe_folder = tmp_path / product_id
        safe_folder.mkdir()
        (safe_folder / "dummy.xml").write_text("x")

        status = get_download_status(product_id, tmp_path, download_scl=False)
        assert status["safe_exists"] is True
        assert status["all_downloaded"] is True

    def test_safe_not_downloaded(self, tmp_path):
        status = get_download_status(
            "S2A_MSIL1C_20250801", tmp_path, download_scl=False
        )
        assert status["safe_exists"] is False
        assert status["all_downloaded"] is False

    def test_scl_check_when_required(self, tmp_path):
        product_id = "S2A_MSIL1C_20250801.SAFE"
        product_core_id = product_id.split(".")[0]
        safe_file = tmp_path / product_id
        safe_file.mkdir()
        (safe_file / "dummy.xml").write_text("x")
        scl_file = tmp_path / f"{product_core_id}_SCL.tif"
        scl_file.write_bytes(b"fake")

        status = get_download_status(product_id, tmp_path, download_scl=True)
        assert status["scl_exists"] is True
        assert status["all_downloaded"] is True

    def test_all_downloaded_false_when_scl_missing(self, tmp_path):
        product_id = "S2A_MSIL1C_20250801"
        safe_folder = tmp_path / product_id
        safe_folder.mkdir()
        (safe_folder / "dummy.xml").write_text("x")

        status = get_download_status(product_id, tmp_path, download_scl=True)
        assert status["safe_exists"] is True
        assert status["scl_exists"] is False
        assert status["all_downloaded"] is False


class TestRunDownload:
    """Tests for run_download."""

    def _make_catalog(self, tmp_path) -> Path:
        catalog_data = [
            {
                "field_date": "2025-08-01",
                "images_found": [
                    {
                        "id": "IMG1",
                        "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG1/path",
                        "l2a_cls": "https://fake.com/IMG1_SCL.tif",
                    },
                ],
            },
            {
                "field_date": "2025-08-02",
                "images_found": [
                    {
                        "id": "IMG2",
                        "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG2/path",
                        "l2a_cls": "https://fake.com/IMG2_SCL.tif",
                    },
                    {
                        "id": "IMG3",
                        "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG3/path",
                        "l2a_cls": "https://fake.com/IMG3_SCL.tif",
                    },
                ],
            },
        ]
        catalog_json = tmp_path / "catalog.json"
        with open(catalog_json, "w") as f:
            json.dump(catalog_data, f)
        return catalog_json

    def test_only_first_downloads_one_per_date(self, tmp_path):
        catalog_json = self._make_catalog(tmp_path)
        with patch(
            "rionegromatchup.sentinel_pipeline.download_product"
        ) as mock_dl, patch(
            "rionegromatchup.sentinel_pipeline.download_scl_asset"
        ), patch(
            "rionegromatchup.sentinel_pipeline.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):

            run_download(catalog_json, tmp_path, only_first=True, download_scl=False)
            assert mock_dl.call_count == 2  # one per date

    def test_all_images_downloaded_when_not_only_first(self, tmp_path):
        catalog_json = self._make_catalog(tmp_path)
        with patch(
            "rionegromatchup.sentinel_pipeline.download_product"
        ) as mock_dl, patch(
            "rionegromatchup.sentinel_pipeline.download_scl_asset"
        ), patch(
            "rionegromatchup.sentinel_pipeline.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):

            run_download(catalog_json, tmp_path, only_first=False, download_scl=False)
            assert mock_dl.call_count == 3  # IMG1 + IMG2 + IMG3

    def test_skips_already_downloaded(self, tmp_path):
        catalog_json = self._make_catalog(tmp_path)
        with patch(
            "rionegromatchup.sentinel_pipeline.download_product"
        ) as mock_dl, patch(
            "rionegromatchup.sentinel_pipeline.get_download_status",
            return_value={
                "safe_exists": True,
                "scl_exists": True,
                "all_downloaded": True,
            },
        ):

            run_download(catalog_json, tmp_path, only_first=True, download_scl=True)
            mock_dl.assert_not_called()


# ==============================================================================
# Organizing.py tests
# ==============================================================================


class TestSetupNames:
    """Tests for setup_names."""

    def test_extracts_source_and_station(self, tmp_path):
        fake_file = tmp_path / "Descarga_Blanvira_2025_Boya.xlsx"
        fake_file.write_text("")
        source, station = setup_names(fake_file)
        assert source == "Blanvira"
        assert station == "Boya"

    def test_warns_on_unexpected_filename(self, tmp_path):
        fake_file = tmp_path / "unexpected.xlsx"
        fake_file.write_text("")
        source, station = setup_names(fake_file)
        assert source == "Unknown"


class TestCleanValue:
    """Tests for clean_value."""

    def test_removes_less_than_sign(self):
        assert clean_value("<0.5") == pytest.approx(0.5)

    def test_replaces_comma_with_dot(self):
        assert clean_value("1,23") == pytest.approx(1.23)

    def test_returns_none_for_nan(self):
        assert clean_value(float("nan")) is None

    def test_returns_none_for_invalid_string(self):
        assert clean_value("nd") is None

    def test_handles_normal_float_string(self):
        assert clean_value("3.14") == pytest.approx(3.14)


class TestCleanCampaigns:
    """Tests for clean_campaigns."""

    def _make_df(self):
        return pd.DataFrame(
            {
                "fecha_muestra": ["2025-01-15", "2025-02-20"],
                "valor_original": ["<LD", "1,5"],
                "limite_deteccion": [0.1, 0.1],
                "limite_cuantificacion": [0.2, 0.2],
            }
        )

    def test_renames_fecha_to_date(self):
        df = clean_campaigns(self._make_df())
        assert "date" in df.columns
        assert "fecha_muestra" not in df.columns

    def test_replaces_LD_with_limite_deteccion(self):
        df = clean_campaigns(self._make_df())
        assert df.loc[0, "organized_value"] == pytest.approx(0.1)

    def test_replaces_LC_with_limite_cuantificacion(self):
        df = clean_campaigns(
            pd.DataFrame(
                {
                    "fecha_muestra": ["2025-03-10"],
                    "valor_original": ["<LC"],
                    "limite_deteccion": [0.1],
                    "limite_cuantificacion": [0.2],
                }
            )
        )
        assert df.loc[0, "organized_value"] == pytest.approx(0.2)

    def test_parses_comma_decimal(self):
        df = clean_campaigns(self._make_df())
        assert df.loc[1, "organized_value"] == pytest.approx(1.5)

    def test_replaces_LD_between_LC_with_limite_cuantificacion(self):
        df = clean_campaigns(
            pd.DataFrame(
                {
                    "fecha_muestra": ["2025-04-01"],
                    "valor_original": ["LD<X<LC"],
                    "limite_deteccion": [0.1],
                    "limite_cuantificacion": [0.2],
                }
            )
        )
        assert df.loc[0, "organized_value"] == pytest.approx(0.2)

    def test_strips_less_than_numeric(self):
        df = clean_campaigns(
            pd.DataFrame(
                {
                    "fecha_muestra": ["2025-05-01"],
                    "valor_original": ["<1,0"],
                    "limite_deteccion": [None],
                    "limite_cuantificacion": [None],
                }
            )
        )
        assert df.loc[0, "organized_value"] == pytest.approx(1.0)

    def test_strips_greater_than_numeric(self):
        df = clean_campaigns(
            pd.DataFrame(
                {
                    "fecha_muestra": ["2025-06-01"],
                    "valor_original": [">2000"],
                    "limite_deteccion": [None],
                    "limite_cuantificacion": [None],
                }
            )
        )
        assert df.loc[0, "organized_value"] == pytest.approx(2000.0)


class TestMergeStationsCampaigns:
    """Tests for merge_stations_campaigns."""

    def test_merge_adds_coordinates(self):
        stations = pd.DataFrame(
            {
                "codigo_pto": ["P1"],
                "id_estacion": ["E1"],
                "latitud": [-32.85],
                "longitud": [-56.5],
            }
        )
        campaigns = pd.DataFrame(
            {
                "codigo_pto": ["P1"],
                "id_estacion": ["E1"],
                "valor_original": ["1.5"],
            }
        )
        merged = merge_stations_campaigns(stations, campaigns)
        assert "latitud" in merged.columns
        assert "longitud" in merged.columns
        assert merged.loc[0, "latitud"] == pytest.approx(-32.85)

    def test_unmatched_campaign_gets_null_coords(self):
        stations = pd.DataFrame(
            {
                "codigo_pto": ["P1"],
                "id_estacion": ["E1"],
                "latitud": [-32.85],
                "longitud": [-56.5],
            }
        )
        campaigns = pd.DataFrame(
            {
                "codigo_pto": ["P99"],  # no match
                "id_estacion": ["E99"],
                "valor_original": ["1.5"],
            }
        )
        merged = merge_stations_campaigns(stations, campaigns)
        assert pd.isna(merged.loc[0, "latitud"])
