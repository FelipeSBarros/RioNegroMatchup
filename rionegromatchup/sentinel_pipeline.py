import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from pystac_client import Client

import requests
from dotenv import load_dotenv

import pandas as pd
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

earthsearch_catalog_url = "https://earth-search.aws.element84.com/v1"
client = Client.open(earthsearch_catalog_url)

# AWS Dataspace
s3 = boto3.resource(
    "s3",
    endpoint_url="https://eodata.dataspace.copernicus.eu",
    aws_access_key_id=os.getenv("DATASPACE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("DATASPACE_SECRET_KEY"),
    region_name="default",
)


def create_bbox_from_point(lon: float, lat: float, buffer_degrees=0.01):
    """Cria um BBox com buffer em torno de um ponto (lon, lat)."""
    return BBox(
        [
            lon - buffer_degrees,  # min_lon
            lat - buffer_degrees,  # min_lat
            lon + buffer_degrees,  # max_lon
            lat + buffer_degrees,  # max_lat
        ],
        crs=CRS.WGS84,
    )


def search_images(bbox_geometry, date: str, time_delta: int, cloud_cover: int):
    """Busca imagens Sentinel-2 L1C ± time_delta dias da data de campo."""
    date_obj = datetime.fromisoformat(date)
    start = (date_obj - timedelta(days=time_delta)).strftime("%Y-%m-%d")
    end = (date_obj + timedelta(days=time_delta)).strftime("%Y-%m-%d")

    logger.info(f"Buscando imagens entre {start} e {end} (cloud < {cloud_cover})")

    search_l1c = catalog.search(
        DataCollection.SENTINEL2_L1C,
        bbox=bbox_geometry,
        time=(start, end),
        filter=f"eo:cloud_cover < {cloud_cover}",
    )
    items = []
    search_l1c = list(search_l1c)
    if search_l1c:
        for item in search_l1c:
            # item = search_l1c[0]

            search_l2a = client.search(
                collections=["sentinel-2-l2a"],  # "sentinel-s2-l2a-cogs"],
                bbox=bbox_geometry,
                datetime=(start, end),
                query={"eo:cloud_cover": {"lt": cloud_cover}},
            )
            search_l2a = list(search_l2a.items())
            items.append(
                {
                    "id": item["id"],
                    "datetime": item["properties"]["datetime"],
                    "cloud_cover": item["properties"]["eo:cloud_cover"],
                    "href": item["assets"]["data"]["href"],
                    "l2a_cls": search_l2a[0].assets.get("scl").href,
                }
            )

    return items


def build_catalog(csv_file: Path, output_json: Path, time_delta=1, cloud_cover=10):
    df = pd.read_csv(csv_file, sep=";")
    if "date" not in df.columns:
        raise ValueError("date column not found in CSV")
    if "longitud" not in df.columns or "latitud" not in df.columns:
        raise ValueError("longitud or latitud columns not found in CSV")

    unique_dates_places = df[["date", "longitud", "latitud"]].drop_duplicates()

    catalog_data = []
    for idx, row in unique_dates_places.iterrows():
        # idx, row = list(unique_dates_places.iterrows())[0]
        date = row["date"]
        bbox_geometry = create_bbox_from_point(row["longitud"], row["latitud"])

        logger.info(f"Processando data de campo: {date}")
        images = search_images(bbox_geometry, date, time_delta, cloud_cover)
        catalog_data.append({"field_date": date, "images_found": images})

    with open(output_json, "w") as f:
        json.dump(catalog_data, f, indent=2)

    logger.info(f"Catálogo salvo em {output_json}")


def download_product(bucket, product: str, output_dir: Path):
    """Baixa todos os arquivos de um produto Sentinel."""
    files = bucket.objects.filter(Prefix=product)
    if not list(files):
        raise FileNotFoundError(f"Nenhum arquivo encontrado para {product}")

    for file in files:
        local_file = output_dir / file.key
        if not local_file.exists():
            os.makedirs(local_file.parent, exist_ok=True)
            bucket.download_file(file.key, str(local_file))
            logger.info(f"Baixado: {local_file}")
        else:
            logger.info(f"Já existe: {local_file}")


def download_scl_asset(output_dir: Path, id: str, scl_asset_href: str):
    """Baixa o asset SCL de uma cena Sentinel-2 L2A"""
    resp = requests.get(scl_asset_href, stream=True)
    resp.raise_for_status()
    with open(f'{output_dir}/{id}_SCL.tif', "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info(f"Asset SCL salvo")
    return None

def check_safe_downloaded(product_id: str, output_dir: Path) -> bool:
    """Verifica se o produto SAFE já foi baixado"""
    # O produto SAFE geralmente cria uma pasta com o nome do product_id
    safe_folder = Path(output_dir, product_id)
    if safe_folder.exists() and safe_folder.is_dir():
        # Verifica se a pasta não está vazia
        if any(safe_folder.iterdir()):
            return True

    # Alternativa: verifica por arquivo .SAFE
    safe_file = output_dir / f"{product_id}.SAFE"
    if safe_file.exists():
        return True

    return False


def check_scl_downloaded(product_id: str, output_dir: Path) -> bool:
    """Verifica se o arquivo SCL já foi baixado"""
    scl_files = Path(output_dir, f"{product_id}_SCL.tif")
    return scl_files.exists()


def check_product_downloaded(
    product_id: str, output_dir: Path, download_scl: bool
) -> tuple[bool, bool]:
    """Verifica o status de download de SAFE e SCL"""
    safe_downloaded = check_safe_downloaded(product_id, output_dir)
    id = product_id.split('.')[0]
    scl_downloaded = (
        check_scl_downloaded(id, output_dir) if download_scl else True
    )

    return safe_downloaded, scl_downloaded


def run_download(
    catalog_json: Path, output_dir: Path, only_first=True, download_scl=True
):
    with open(catalog_json, "r") as f:
        catalog_data = json.load(f)

    stats = {
        "total_processed": 0,
        "already_downloaded": 0,
        "safe_downloaded": 0,
        "scl_downloaded": 0,
        "errors": 0,
    }

    for entry in catalog_data:
        field_date = entry["field_date"]
        images = entry["images_found"]

        if not images:
            logger.warning(f"Nenhuma imagem para {field_date}")
            continue

        to_download = images[:1] if only_first else images
        for img in to_download:
            stats["total_processed"] += 1
            product_id = img["id"]
            product_path = "/".join(img["href"].split("/")[9:])

            # Verifica status de download
            safe_downloaded, scl_downloaded = check_product_downloaded(
                product_id, output_dir, download_scl
            )

            # Se ambos já foram baixados, pula
            if safe_downloaded:
                logger.info(f"[{field_date}] {product_id} já baixado - pulando")
                stats["already_downloaded"] += 1
            if scl_downloaded:
                logger.info(f" SCL [{field_date}] {product_id} já baixado - pulando")
                stats["already_downloaded"] += 1
                continue

            logger.info(f"[{field_date}] Baixando {product_id}...")

            try:
                # Baixa o produto SAFE se necessário
                if not safe_downloaded:
                    download_product(s3.Bucket("eodata"), product_path, output_dir)
                    stats["safe_downloaded"] += 1
                    logger.info(f"✓ SAFE baixado: {product_id}")
                else:
                    logger.info(f"✓ SAFE já existe: {product_id}")

                # Baixa o asset SCL se necessário
                if download_scl and not scl_downloaded:
                    id = product_id.split('.')[0]
                    if download_scl_asset(output_dir, id, img["l2a_cls"]):
                        stats["scl_downloaded"] += 1
                        logger.info(f"✓ SCL baixado: {id}")
                elif download_scl:
                    logger.info(f"✓ SCL já existe: {id}")

            except Exception as e:
                logger.error(f"✗ Erro ao baixar {id}: {e}")
                stats["errors"] += 1

    # Relatório final
    logger.info("\n" + "=" * 50)
    logger.info("RELATÓRIO DE DOWNLOAD")
    logger.info("=" * 50)
    logger.info(f"Total processado: {stats['total_processed']}")
    logger.info(f"Já baixados: {stats['already_downloaded']}")
    logger.info(f"SAFE baixados: {stats['safe_downloaded']}")
    logger.info(f"SCL baixados: {stats['scl_downloaded']}")
    logger.info(f"Erros: {stats['errors']}")
    logger.info("=" * 50)


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
        "--download-scl",
        action="store_true",
        help="Baixar asset SCL junto com produtos SAFE",
    )
    parser.add_argument(
        "--csv", type=Path, required=True, help="CSV com datas de campo"
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
            args.catalog_json,
            time_delta=args.time_delta,
            cloud_cover=args.cloud_cover,
        )

    if args.mode in ("download", "all"):
        run_download(
            args.catalog_json,
            args.output,
            only_first=args.only_first,
            download_scl=args.download_scl,
        )
