import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

import pandas as pd
import geopandas as gpd
import boto3
from sentinelhub import CRS, BBox, DataCollection, SHConfig, SentinelHubCatalog

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

config = SHConfig()
config.sh_client_id = os.getenv("SH_CLIENT_ID")
config.sh_client_secret = os.getenv("SH_CLIENT_SECRET")
config.sh_base_url = "https://sh.dataspace.copernicus.eu"
config.sh_token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

catalog = SentinelHubCatalog(config=config)

# AWS Dataspace
s3 = boto3.resource(
    "s3",
    endpoint_url="https://eodata.dataspace.copernicus.eu",
    aws_access_key_id=os.getenv("DATASPACE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("DATASPACE_SECRET_KEY"),
    region_name="default",
)


def load_area(geojson_path: Path) -> BBox:
    gdf = gpd.read_file(geojson_path)
    return BBox(list(gdf.to_crs(4326).total_bounds), crs=CRS.WGS84)


def search_images(bbox, date: str, time_delta: int, cloud_cover: int):
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
        img_date = item["properties"]["datetime"][:10]
        delta_days = abs((datetime.fromisoformat(img_date) - date_obj).days)
        items.append(
            {
                "id": item["id"],
                "datetime": item["properties"]["datetime"],
                "cloud_cover": item["properties"]["eo:cloud_cover"],
                "href": item["assets"]["data"]["href"],
                "delta_days": delta_days,
            }
        )
    return items


def build_catalog(
    csv_file: Path, geojson_file: Path, output_json: Path, time_delta=1, cloud_cover=10
):
    df = pd.read_csv(csv_file, sep=";")
    if "date" not in df.columns:
        raise ValueError("date column not found in CSV")

    bbox = load_area(geojson_file)

    catalog_data = []
    for date in df["date"].unique():
        logger.info(f"Processando data de campo: {date}")
        images = search_images(bbox, date, time_delta, cloud_cover)
        catalog_data.append({"field_date": date, "images_found": images})

    with open(output_json, "w") as f:
        json.dump(catalog_data, f, indent=2)

    logger.info(f"Catálogo salvo em {output_json}")


def download_product(bucket, product: str, target: Path):
    """Baixa todos os arquivos de um produto Sentinel."""
    files = bucket.objects.filter(Prefix=product)
    if not list(files):
        raise FileNotFoundError(f"Nenhum arquivo encontrado para {product}")

    for file in files:
        local_file = target / file.key
        if not local_file.exists():
            os.makedirs(local_file.parent, exist_ok=True)
            bucket.download_file(file.key, str(local_file))
            logger.info(f"Baixado: {local_file}")
        else:
            logger.info(f"Já existe: {local_file}")


def run_download(catalog_json: Path, output_dir: Path, only_first=True):
    with open(catalog_json, "r") as f:
        catalog_data = json.load(f)

    for entry in catalog_data:
        field_date = entry["field_date"]
        images = entry["images_found"]

        if not images:
            logger.warning(f"Nenhuma imagem para {field_date}")
            continue

        to_download = images[:1] if only_first else images
        for img in to_download:
            product_path = "/".join(img["href"].split("/")[3:])
            logger.info(f"[{field_date}] Baixando {img['id']}...")
            download_product(s3.Bucket("eodata"), product_path, output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline Sentinel-2 (catalogar e baixar imagens)"
    )
    parser.add_argument(
        "--mode",
        choices=["catalog", "download", "all"],
        required=True,
        help="Modo de operação: catalog, download ou all",
    )
    parser.add_argument(
        "--csv", type=Path, required=True, help="CSV com datas de campo"
    )
    parser.add_argument(
        "--geojson",
        type=Path,
        required=True,
        help="Arquivo GeoJSON da área de interesse",
    )
    parser.add_argument("--output", type=Path, required=True, help="Diretório de saída")
    parser.add_argument(
        "--catalog-json",
        type=Path,
        default=Path("sentinel_catalog.json"),
        help="Arquivo JSON de catálogo",
    )
    parser.add_argument(
        "--time-delta", type=int, default=1, help="Intervalo de dias para busca"
    )
    parser.add_argument("--cloud-cover", type=int, default=10, help="Nuvem máxima (%)")
    parser.add_argument(
        "--only-first",
        action="store_true",
        help="Baixar apenas a primeira imagem encontrada",
    )

    args = parser.parse_args()

    if args.mode in ("catalog", "all"):
        build_catalog(
            args.csv,
            args.geojson,
            args.catalog_json,
            time_delta=args.time_delta,
            cloud_cover=args.cloud_cover,
        )

    if args.mode in ("download", "all"):
        run_download(args.catalog_json, args.output, only_first=args.only_first)
