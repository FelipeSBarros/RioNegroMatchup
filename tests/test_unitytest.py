import json
from datetime import datetime
from unittest.mock import patch, MagicMock

import geopandas as gpd
import pandas as pd
from sentinelhub import BBox, CRS
from shapely.geometry import Polygon

from rionegromatchup.sentinel_pipeline import (
    load_area,
    search_images,
    build_catalog,
    run_download,
)


def test_load_area_creates_bbox(tmp_path):
    """Testa se load_area gera corretamente um BBox a partir de um GeoJSON."""

    # Criar GeoJSON de teste
    poly = Polygon([(-56.6, -32.9), (-56.4, -32.9), (-56.4, -32.8), (-56.6, -32.8)])
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[poly], crs="EPSG:4326")
    geojson_file = tmp_path / "test_area.geojson"
    gdf.to_file(geojson_file, driver="GeoJSON")

    # Rodar função
    bbox = load_area(geojson_file)

    # Conferir tipo e CRS
    assert isinstance(bbox, BBox)
    assert bbox.crs == CRS.WGS84

    # Conferir limites
    expected_bounds = [-56.6, -32.9, -56.4, -32.8]  # xmin, ymin, xmax, ymax
    assert list(bbox) == expected_bounds


def test_search_images_returns_correct_structure():
    bbox = BBox([-56.6, -32.9, -56.4, -32.8], crs="EPSG:4326")
    field_date = "2025-08-01"
    time_delta = 1
    cloud_cover = 10

    # Criar item falso do STAC
    fake_item = {
        "id": "S2A_20250801T101031",
        "properties": {"datetime": "2025-08-02T10:10:31.000Z", "eo:cloud_cover": 5},
        "assets": {"data": {"href": "https://fake-link.com/product"}},
    }

    # Mock do catalog.search para retornar um iterável com nosso item falso
    with patch("rionegromatchup.sentinel_pipeline.catalog") as mock_catalog:
        mock_search = MagicMock()
        mock_search.__iter__.return_value = [fake_item]
        mock_catalog.search.return_value = mock_search

        result = search_images(bbox, field_date, time_delta, cloud_cover)

        # Verificações básicas
        assert isinstance(result, list)
        assert len(result) == 1

        img = result[0]
        for key in ["id", "datetime", "cloud_cover", "href", "delta_days"]:
            assert key in img

        # Conferir delta_days
        expected_delta = abs(
            (
                datetime.fromisoformat("2025-08-02")
                - datetime.fromisoformat(field_date)
            ).days
        )
        assert img["delta_days"] == expected_delta


def test_build_catalog_creates_json(tmp_path):
    # --- Criar CSV temporário ---
    csv_file = tmp_path / "dates.csv"
    pd.DataFrame({"date": ["2025-08-01", "2025-08-02"]}).to_csv(csv_file, index=False)

    # --- Criar GeoJSON temporário ---
    geojson_file = tmp_path / "area.geojson"
    poly = Polygon([(-56.6, -32.9), (-56.4, -32.9), (-56.4, -32.8), (-56.6, -32.8)])
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[poly], crs="EPSG:4326")
    gdf.to_file(geojson_file, driver="GeoJSON")

    # --- Arquivo JSON de saída ---
    output_json = tmp_path / "catalog.json"

    # --- Mockar search_images ---
    fake_image = {
        "id": "S2A_20250801T101031",
        "datetime": "2025-08-01T10:10:31.000Z",
        "cloud_cover": 5,
        "href": "https://fake-link.com/product",
        "delta_days": 0,
    }

    with patch(
        "rionegromatchup.sentinel_pipeline.search_images", return_value=[fake_image]
    ):
        # Rodar build_catalog
        build_catalog(csv_file, geojson_file, output_json, time_delta=1, cloud_cover=10)

    # --- Validar JSON criado ---
    with open(output_json, "r") as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 2  # duas datas no CSV
    for entry in data:
        assert "field_date" in entry
        assert "images_found" in entry
        assert isinstance(entry["images_found"], list)
        assert entry["images_found"][0]["id"] == "S2A_20250801T101031"


def test_run_download_calls_download_product(tmp_path):
    # --- JSON de catálogo fictício ---
    catalog_data = [
        {
            "field_date": "2025-08-01",
            "images_found": [
                {
                    "id": "IMG1",
                    "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG1/path",
                }
            ],
        },
        {
            "field_date": "2025-08-02",
            "images_found": [
                {
                    "id": "IMG2",
                    "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG2/path",
                },
                {
                    "id": "IMG3",
                    "href": "https://eodata.dataspace.copernicus.eu/eodata/IMG3/path",
                },
            ],
        },
    ]
    catalog_json = tmp_path / "catalog.json"
    with open(catalog_json, "w") as f:
        json.dump(catalog_data, f)

    output_dir = tmp_path / "downloads"

    # --- Mockar download_product ---
    with patch("rionegromatchup.sentinel_pipeline.download_product") as mock_download:
        run_download(catalog_json, output_dir, only_first=True)

        # Deve chamar apenas a primeira imagem de cada data
        expected_calls = 2  # 2025-08-01 -> IMG1, 2025-08-02 -> IMG2 (only_first=True)
        assert mock_download.call_count == expected_calls

        # Conferir argumentos do primeiro chamado
        first_call_args = mock_download.call_args_list[0][0]
        bucket_arg, product_arg, target_arg = first_call_args
        assert str(output_dir) == str(target_arg)
        assert "IMG1" in product_arg

        # Conferir argumentos do segundo chamado
        second_call_args = mock_download.call_args_list[1][0]
        _, product_arg2, _ = second_call_args
        assert "IMG2" in product_arg2
