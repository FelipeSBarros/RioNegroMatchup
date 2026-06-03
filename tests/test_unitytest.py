import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
from sentinelhub import BBox, CRS

from rionegromatchup.insitu_data import (
    setup_names,
    clean_campaigns,
    merge_stations_campaigns,
    clean_value,
)
from rionegromatchup.sentinel_data import (
    create_bbox_from_point,
    search_images,
    build_catalog,
    run_download,
    get_download_status,
    get_scl_path,
    SCL_SUBDIR,
)

# ==============================================================================
# sentinel_data.py tests
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
        with patch("rionegromatchup.sentinel_data.catalog") as mock_catalog, patch(
            "rionegromatchup.sentinel_data.client"
        ) as mock_client:
            mock_catalog.search.return_value = iter([self._make_fake_l1c_item()])
            mock_search = MagicMock()
            mock_search.items.return_value = [self._make_fake_l2a_item()]
            mock_client.search.return_value = mock_search

            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert isinstance(result, list)

    def test_returns_correct_keys(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        with patch("rionegromatchup.sentinel_data.catalog") as mock_catalog, patch(
            "rionegromatchup.sentinel_data.client"
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

        with patch("rionegromatchup.sentinel_data.catalog") as mock_catalog, patch(
            "rionegromatchup.sentinel_data.client"
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
        with patch("rionegromatchup.sentinel_data.catalog") as mock_catalog:
            mock_catalog.search.return_value = iter([])
            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert result == []

    def test_l2a_cls_is_none_when_no_l2a_found(self):
        bbox = create_bbox_from_point(-56.5, -32.85)
        with patch("rionegromatchup.sentinel_data.catalog") as mock_catalog, patch(
            "rionegromatchup.sentinel_data.client"
        ) as mock_client:
            mock_catalog.search.return_value = iter([self._make_fake_l1c_item()])
            mock_search = MagicMock()
            mock_search.items.return_value = []
            mock_client.search.return_value = mock_search

            result = search_images(bbox, "2025-08-01", time_delta=1, cloud_cover=10)
            assert result[0]["l2a_cls"] is None


class TestBuildCatalog:
    """Tests for build_catalog."""

    def _make_csv(self, tmp_path) -> Path:
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {
                "date": ["2025-08-01", "2025-08-02"],
                "longitud": [-56.5, -56.5],
                "latitud": [-32.85, -32.85],
            }
        ).to_csv(csv_file, index=False)
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
            "rionegromatchup.sentinel_data.search_images", return_value=[fake_image]
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
            "rionegromatchup.sentinel_data.search_images", return_value=[fake_image]
        ):
            build_catalog(csv_file, output_json, time_delta=1, cloud_cover=10)

        with open(output_json) as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 2
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
        pd.DataFrame({"date": ["2025-08-01"], "code": 42}).to_csv(
            csv_file, index=False, sep=","
        )
        with pytest.raises(ValueError, match="longitud"):
            build_catalog(csv_file, tmp_path / "out.json")

    def test_deduplicates_same_scene_across_stations(self, tmp_path):
        csv_file = tmp_path / "campaigns.csv"
        pd.DataFrame(
            {
                "date": ["2025-08-01", "2025-08-01"],
                "longitud": [-56.5, -56.6],
                "latitud": [-32.85, -32.90],
            }
        ).to_csv(csv_file, index=False, sep=";")

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
            "rionegromatchup.sentinel_data.search_images", return_value=[fake_image]
        ):
            build_catalog(csv_file, output_json, time_delta=1, cloud_cover=10)

        with open(output_json) as f:
            data = json.load(f)

        assert len(data) == 1
        assert len(data[0]["images_found"]) == 1
        assert data[0]["images_found"][0]["id"] == "S2A_20250801T101031"

    def test_reads_comma_separated_csv(self, tmp_path):
        csv_file = tmp_path / "campaigns_comma.csv"
        pd.DataFrame(
            {
                "date": ["2025-08-01"],
                "longitud": [-56.5],
                "latitud": [-32.85],
            }
        ).to_csv(csv_file, index=False)
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
            "rionegromatchup.sentinel_data.search_images", return_value=[fake_image]
        ):
            build_catalog(csv_file, output_json, time_delta=1, cloud_cover=10)
        assert output_json.exists()

    def test_entry_created_when_no_images_found(self, tmp_path):
        csv_file = self._make_csv(tmp_path)
        output_json = tmp_path / "catalog.json"
        with patch("rionegromatchup.sentinel_data.search_images", return_value=[]):
            build_catalog(csv_file, output_json, time_delta=1, cloud_cover=10)
        with open(output_json) as f:
            data = json.load(f)
        assert len(data) == 0
        assert data == []


class TestGetSclPath:
    """Tests for the get_scl_path helper."""

    def test_returns_path_under_scl_subdir(self, tmp_path):
        path = get_scl_path("S2A_MSIL1C_20250801", tmp_path)
        assert path.parent == tmp_path / SCL_SUBDIR
        assert path.name == "S2A_MSIL1C_20250801_SCL.tif"

    def test_strips_safe_extension(self, tmp_path):
        path = get_scl_path("S2A_MSIL1C_20250801.SAFE", tmp_path)
        assert path.name == "S2A_MSIL1C_20250801_SCL.tif"

    def test_consistent_with_download_scl_asset(self, tmp_path):
        """get_scl_path and download_scl_asset must agree on the file location."""
        product_id = "S2A_MSIL1C_20250801.SAFE"
        product_core_id = product_id.split(".")[0]
        expected = get_scl_path(product_id, tmp_path)

        # Simulate what download_scl_asset would write
        scl_dir = tmp_path / SCL_SUBDIR
        scl_dir.mkdir()
        actual = scl_dir / f"{product_core_id}_SCL.tif"
        actual.write_bytes(b"fake")

        assert expected == actual
        assert expected.exists()


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
        """SCL is now expected under {output_dir}/scl/."""
        product_id = "S2A_MSIL1C_20250801.SAFE"
        product_core_id = product_id.split(".")[0]

        safe_file = tmp_path / product_id
        safe_file.mkdir()
        (safe_file / "dummy.xml").write_text("x")

        # Create SCL in the correct subdirectory
        scl_dir = tmp_path / SCL_SUBDIR
        scl_dir.mkdir()
        (scl_dir / f"{product_core_id}_SCL.tif").write_bytes(b"fake")

        status = get_download_status(product_id, tmp_path, download_scl=True)
        assert status["scl_exists"] is True
        assert status["all_downloaded"] is True

    def test_all_downloaded_false_when_scl_missing(self, tmp_path):
        """SCL missing from scl/ subdir → all_downloaded must be False."""
        product_id = "S2A_MSIL1C_20250801"
        safe_folder = tmp_path / product_id
        safe_folder.mkdir()
        (safe_folder / "dummy.xml").write_text("x")

        # No SCL file anywhere
        status = get_download_status(product_id, tmp_path, download_scl=True)
        assert status["safe_exists"] is True
        assert status["scl_exists"] is False
        assert status["all_downloaded"] is False

    def test_scl_not_found_in_old_flat_location(self, tmp_path):
        """A SCL file placed in the old flat location must NOT satisfy the check."""
        product_id = "S2A_MSIL1C_20250801"
        safe_folder = tmp_path / product_id
        safe_folder.mkdir()
        (safe_folder / "dummy.xml").write_text("x")

        # Place SCL in the old flat location (should no longer be recognised)
        (tmp_path / f"{product_id}_SCL.tif").write_bytes(b"old")

        status = get_download_status(product_id, tmp_path, download_scl=True)
        assert status["scl_exists"] is False


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
        with patch("rionegromatchup.sentinel_data.download_product") as mock_dl, patch(
            "rionegromatchup.sentinel_data.download_scl_asset"
        ), patch(
            "rionegromatchup.sentinel_data.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):
            run_download(catalog_json, tmp_path, only_first=True, download_scl=False)
            assert mock_dl.call_count == 2

    def test_all_images_downloaded_when_not_only_first(self, tmp_path):
        catalog_json = self._make_catalog(tmp_path)
        with patch("rionegromatchup.sentinel_data.download_product") as mock_dl, patch(
            "rionegromatchup.sentinel_data.download_scl_asset"
        ), patch(
            "rionegromatchup.sentinel_data.get_download_status",
            return_value={
                "safe_exists": False,
                "scl_exists": False,
                "all_downloaded": False,
            },
        ):
            run_download(catalog_json, tmp_path, only_first=False, download_scl=False)
            assert mock_dl.call_count == 3

    def test_skips_already_downloaded(self, tmp_path):
        catalog_json = self._make_catalog(tmp_path)
        with patch("rionegromatchup.sentinel_data.download_product") as mock_dl, patch(
            "rionegromatchup.sentinel_data.get_download_status",
            return_value={
                "safe_exists": True,
                "scl_exists": True,
                "all_downloaded": True,
            },
        ):
            run_download(catalog_json, tmp_path, only_first=True, download_scl=True)
            mock_dl.assert_not_called()


# ==============================================================================
# insitu_data.py tests
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
                "codigo_pto": ["P99"],
                "id_estacion": ["E99"],
                "valor_original": ["1.5"],
            }
        )
        merged = merge_stations_campaigns(stations, campaigns)
        assert pd.isna(merged.loc[0, "latitud"])


"""
Unit tests for acolite_spec.py — Step 5: polygon_clip field and validation.
"""

import pytest
from rionegromatchup.acolite_spec import IOConfig


class TestIOConfigPolygonClip:
    """Tests for the polygon_clip field and its validation."""

    # --- Default value ---

    def test_polygon_clip_defaults_to_false(self):
        io = IOConfig(inputfile="", output="")
        assert io.polygon_clip is False

    # --- Validation ---

    def test_polygon_clip_true_with_polygon_passes(self, tmp_path):
        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=str(polygon_file),
            polygon_clip=True,
        )
        # Should not raise
        io.validate()

    def test_polygon_clip_true_without_polygon_raises(self, tmp_path):
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=None,
            polygon_clip=True,
        )
        with pytest.raises(
            ValueError, match="polygon_clip=True requires a valid polygon path"
        ):
            io.validate()

    def test_polygon_clip_false_without_polygon_passes(self, tmp_path):
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=None,
            polygon_clip=False,
        )
        # Should not raise
        io.validate()

    def test_polygon_clip_false_with_polygon_passes(self, tmp_path):
        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=str(polygon_file),
            polygon_clip=False,
        )
        # polygon_clip=False — no constraint on polygon
        io.validate()

    def test_limit_and_polygon_still_mutually_exclusive(self, tmp_path):
        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            limit=(-33.0, -57.0, -32.5, -56.0),
            polygon=str(polygon_file),
        )
        with pytest.raises(ValueError, match="either `limit` or `polygon`"):
            io.validate()

    # --- Serialisation ---

    def test_polygon_clip_true_appears_in_settings_dict(self, tmp_path):
        from rionegromatchup.acolite_spec import AcoliteConfig

        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")

        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(
                inputfile="",
                output="",
                polygon=str(polygon_file),
                polygon_clip=True,
            ),
        )
        settings = cfg.to_settings_dict()
        assert settings.get("polygon_clip") == "true"
        assert settings.get("polygon") == str(polygon_file)

    def test_polygon_clip_false_absent_from_settings_dict(self):
        from rionegromatchup.acolite_spec import AcoliteConfig

        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(inputfile="", output="", polygon_clip=False),
        )
        settings = cfg.to_settings_dict()
        assert "polygon_clip" not in settings


"""
Unit tests for acolite_spec.py — Step 5: polygon_clip field and validation.
"""

import pytest
from rionegromatchup.acolite_spec import IOConfig


class TestIOConfigPolygonClip:
    """Tests for the polygon_clip field and its validation."""

    # --- Default value ---

    def test_polygon_clip_defaults_to_false(self):
        io = IOConfig(inputfile="", output="")
        assert io.polygon_clip is False

    # --- Validation ---

    def test_polygon_clip_true_with_polygon_passes(self, tmp_path):
        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=str(polygon_file),
            polygon_clip=True,
        )
        # Should not raise
        io.validate()

    def test_polygon_clip_true_without_polygon_raises(self, tmp_path):
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=None,
            polygon_clip=True,
        )
        with pytest.raises(
            ValueError, match="polygon_clip=True requires a valid polygon path"
        ):
            io.validate()

    def test_polygon_clip_false_without_polygon_passes(self, tmp_path):
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=None,
            polygon_clip=False,
        )
        # Should not raise
        io.validate()

    def test_polygon_clip_false_with_polygon_passes(self, tmp_path):
        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            polygon=str(polygon_file),
            polygon_clip=False,
        )
        # polygon_clip=False — no constraint on polygon
        io.validate()

    def test_limit_and_polygon_still_mutually_exclusive(self, tmp_path):
        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")
        safe = tmp_path / "scene.SAFE"
        safe.mkdir()

        io = IOConfig(
            inputfile=str(safe),
            output=str(tmp_path),
            limit=(-33.0, -57.0, -32.5, -56.0),
            polygon=str(polygon_file),
        )
        with pytest.raises(ValueError, match="either `limit` or `polygon`"):
            io.validate()

    # --- Serialisation ---

    def test_polygon_clip_true_appears_in_settings_dict(self, tmp_path):
        from rionegromatchup.acolite_spec import AcoliteConfig

        polygon_file = tmp_path / "water.geojson"
        polygon_file.write_text("{}")

        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(
                inputfile="",
                output="",
                polygon=str(polygon_file),
                polygon_clip=True,
            ),
        )
        settings = cfg.to_settings_dict()
        assert settings.get("polygon_clip") == "true"
        assert settings.get("polygon") == str(polygon_file)

    def test_polygon_clip_false_absent_from_settings_dict(self):
        from rionegromatchup.acolite_spec import AcoliteConfig

        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(inputfile="", output="", polygon_clip=False),
        )
        settings = cfg.to_settings_dict()
        assert "polygon_clip" not in settings


# ---------------------------------------------------------------------------
# with_scl_polygon — Step 6 tests
# ---------------------------------------------------------------------------

import numpy as np
import rasterio
from rasterio.transform import from_bounds
from rionegromatchup.acolite_spec import AcoliteConfig
from rionegromatchup.scl_water import SCL_WATER_CLASS, GEOJSON_SUBDIR

# Reuse same synthetic raster helper pattern from test_scl_water.py
_TEST_CRS = "EPSG:32721"
_W, _S, _E, _N = 500_000.0, 6_350_000.0, 500_300.0, 6_350_300.0


def _make_scl(tmp_path, name="S2A_MSIL1C_20250801T101031_N0500_R024_T21HUD_SCL.tif"):
    data = np.full((30, 30), 4, dtype=np.uint8)
    data[5:25, 5:25] = SCL_WATER_CLASS
    transform = from_bounds(_W, _S, _E, _N, 30, 30)
    path = tmp_path / name
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=30,
        width=30,
        count=1,
        dtype=np.uint8,
        crs=_TEST_CRS,
        transform=transform,
    ) as dst:
        dst.write(data, 1)
    return path


def _make_cfg(tmp_path):
    return AcoliteConfig(
        acolite_executable="/fake/acolite",
        io=IOConfig(inputfile="", output=str(tmp_path)),
    )


class TestWithSclPolygon:
    """Tests for AcoliteConfig.with_scl_polygon()."""

    # --- Return type and immutability ---

    def test_returns_acolite_config(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert isinstance(result, AcoliteConfig)

    def test_returns_new_instance(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert result is not cfg

    def test_original_config_not_mutated(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        cfg.with_scl_polygon(scl, min_area_m2=0)
        assert cfg.io.polygon is None
        assert cfg.io.polygon_clip is False

    # --- polygon and polygon_clip wired correctly ---

    def test_polygon_set_to_geojson_path(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert result.io.polygon is not None
        assert result.io.polygon.endswith(".geojson")

    def test_polygon_clip_is_true(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert result.io.polygon_clip is True

    def test_limit_cleared(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = AcoliteConfig(
            acolite_executable="/fake/acolite",
            io=IOConfig(
                inputfile="",
                output=str(tmp_path),
                limit=(-33.0, -57.0, -32.5, -56.0),
            ),
        )
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert result.io.limit is None

    # --- GeoJSON file location ---

    def test_geojson_written_to_default_subdir(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        expected_dir = tmp_path / GEOJSON_SUBDIR
        assert Path(result.io.polygon).parent == expected_dir

    def test_geojson_written_to_custom_dir(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        custom_dir = tmp_path / "custom_geojson"
        result = cfg.with_scl_polygon(scl, geojson_output_dir=custom_dir, min_area_m2=0)
        assert Path(result.io.polygon).parent == custom_dir

    def test_geojson_file_exists_on_disk(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        assert Path(result.io.polygon).exists()

    # --- Idempotency ---

    def test_reuses_existing_geojson_when_overwrite_false(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result1 = cfg.with_scl_polygon(scl, min_area_m2=0)
        mtime = Path(result1.io.polygon).stat().st_mtime
        result2 = cfg.with_scl_polygon(scl, overwrite=False, min_area_m2=0)
        assert Path(result2.io.polygon).stat().st_mtime == mtime

    # --- Validation passes after with_scl_polygon ---

    def test_resulting_config_passes_io_validation(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        # Should not raise — polygon exists, polygon_clip=True, limit=None
        result.io.validate()

    # --- Serialisation ---

    def test_polygon_clip_in_settings_dict(self, tmp_path):
        scl = _make_scl(tmp_path)
        cfg = _make_cfg(tmp_path)
        result = cfg.with_scl_polygon(scl, min_area_m2=0)
        settings = result.to_settings_dict()
        assert settings.get("polygon_clip") == "true"
        assert "polygon" in settings

    # --- Error propagation ---

    def test_raises_if_scl_not_found(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        with pytest.raises(FileNotFoundError):
            cfg.with_scl_polygon(tmp_path / "nonexistent.tif")

    def test_raises_if_no_water_pixels(self, tmp_path):
        # Raster with no water
        data = np.full((30, 30), 4, dtype=np.uint8)
        transform = from_bounds(_W, _S, _E, _N, 30, 30)
        scl = tmp_path / "no_water_SCL.tif"
        with rasterio.open(
            scl,
            "w",
            driver="GTiff",
            height=30,
            width=30,
            count=1,
            dtype=np.uint8,
            crs=_TEST_CRS,
            transform=transform,
        ) as dst:
            dst.write(data, 1)

        cfg = _make_cfg(tmp_path)
        with pytest.raises(ValueError, match="No water pixels"):
            cfg.with_scl_polygon(scl)
