import os
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import geopandas as gpd
from sentinelhub import CRS, BBox, DataCollection, SHConfig, SentinelHubCatalog

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = SHConfig()
config.sh_client_id = os.getenv("SH_CLIENT_ID")
config.sh_client_secret = os.getenv("SH_CLIENT_SECRET")
config.sh_base_url = "https://sh.dataspace.copernicus.eu"
config.sh_token_url = (
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
)

catalog = SentinelHubCatalog(config=config)


def load_area(geojson_path: Path) -> BBox:
    gdf = gpd.read_file(geojson_path)
    return BBox(list(gdf.to_crs(4326).total_bounds), crs=CRS.WGS84)


def search_images(bbox, date: str, time_delta: int = 1, cloud_cover: int = 10):
    """Busca imagens Sentinel-2 L1C ± time_delta dias da data de campo."""
    date_obj = datetime.fromisoformat(date)
    start = (date_obj - timedelta(days=time_delta)).strftime("%Y-%m-%d")
    end = (date_obj + timedelta(days=time_delta)).strftime("%Y-%m-%d")

    logger.info(f"Buscando imagens entre {start} e {end} (cloud < {cloud_cover})")

    search = catalog.search(
        DataCollection.SENTINEL2_L1C,
        bbox=bbox,
        time=(start, end),
        filter=f"eo:cloud_cover < {cloud_cover}",
    )

    items = []
    for item in search:
        items.append(
            {
                "id": item["id"],
                "datetime": item["properties"]["datetime"],
                "cloud_cover": item["properties"]["eo:cloud_cover"],
                "href": item["assets"]["data"]["href"],
            }
        )

    return items


def build_catalog(csv_file: Path, geojson_file: Path, output_json: Path, time_delta=1):
    df = pd.read_csv(csv_file)
    bbox = load_area(geojson_file)

    catalog_data = []
    for date in df["date"].unique():
        logger.info(f"Processando data de campo: {date}")
        images = search_images(bbox, date, time_delta=time_delta)
        catalog_data.append(
            {"field_date": date, "images_found": images}
        )

    with open(output_json, "w") as f:
        json.dump(catalog_data, f, indent=2)

    logger.info(f"Catálogo salvo em {output_json}")


if __name__ == "__main__":
    CSV_FILE = Path("./datos/mediciones/mediciones_campo.csv")
    GEOJSON_FILE = Path("./datos/bbox_rincon.geojson")
    OUTPUT_JSON = Path("./datos/sentinel_catalog.json")

    build_catalog(CSV_FILE, GEOJSON_FILE, OUTPUT_JSON, time_delta=2)
