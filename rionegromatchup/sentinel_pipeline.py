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
    """
    Busca imagens Sentinel-2 L1C ± time_delta dias da data de campo,
    e para cada cena L1C encontrada, busca a cena L2A correspondente
    pela mesma data de aquisição.
    """
    date_obj = datetime.fromisoformat(date)
    start = (date_obj - timedelta(days=time_delta)).strftime("%Y-%m-%d")
    end = (date_obj + timedelta(days=time_delta)).strftime("%Y-%m-%d")

    logger.info(f"Buscando imagens entre {start} e {end} (cloud < {cloud_cover}%)")

    # Point 3 fix: convert to list once, with a clear variable name
    l1c_results = list(
        catalog.search(
            DataCollection.SENTINEL2_L1C,
            bbox=bbox_geometry,
            time=(start, end),
            filter=f"eo:cloud_cover < {cloud_cover}",
        )
    )

    if not l1c_results:
        logger.info("Nenhuma cena L1C encontrada.")
        return []

    items = []
    for item in l1c_results:
        item_id = item["id"]

        # Point 4 fix: extract the acquisition date from this specific L1C item
        # and use a narrow ±0 day window to find its L2A counterpart
        acquisition_datetime = item["properties"][
            "datetime"
        ]  # e.g. "2025-08-01T10:10:31Z"
        acquisition_date = acquisition_datetime[:10]  # e.g. "2025-08-01"

        logger.info(
            f"  Buscando L2A correspondente para {item_id} ({acquisition_date})"
        )

        l2a_results = list(
            client.search(
                collections=["sentinel-2-l2a"],
                bbox=bbox_geometry,
                datetime=f"{acquisition_date}/{acquisition_date}",
                query={"eo:cloud_cover": {"lt": cloud_cover}},
            ).items()
        )

        scl_href = None
        if l2a_results:
            scl_asset = l2a_results[0].assets.get("scl")
            scl_href = scl_asset.href if scl_asset else None
            if not scl_href:
                logger.warning(f"  SCL asset não encontrado para {item_id}")
        else:
            logger.warning(
                f"  Nenhuma cena L2A encontrada para {item_id} em {acquisition_date}"
            )

        delta_days = abs((datetime.fromisoformat(acquisition_date) - date_obj).days)

        items.append(
            {
                "id": item_id,
                "datetime": acquisition_datetime,
                "cloud_cover": item["properties"]["eo:cloud_cover"],
                "href": item["assets"]["data"]["href"],
                "delta_days": delta_days,
                "l2a_cls": scl_href,  # may be None if no L2A match found
            }
        )

    logger.info(f"Total de cenas encontradas: {len(items)}")
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
        catalog_data.append(
            {"field_date": date, "images_found": images}
        )  # todo creo que va un if aca

    with open(output_json, "w") as f:
        json.dump(catalog_data, f, indent=2)

    logger.info(f"Catálogo salvo em {output_json}")


def download_product(bucket, product: str, output_dir: Path):
    """
    Baixa todos os arquivos de um produto Sentinel do bucket S3.

    Args:
        bucket: boto3 Bucket resource object
        product: S3 prefix path to the product
        output_dir: local directory to save downloaded files
    """
    # Point 6 fix: materialize once to avoid exhausting the iterator on the existence check
    files = list(bucket.objects.filter(Prefix=product))

    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para o produto: {product}")

    logger.info(f"Encontrados {len(files)} arquivos para {product}")

    for obj in files:
        local_file = output_dir / obj.key
        if local_file.exists():
            logger.info(f"Já existe: {local_file}")
            continue

        os.makedirs(local_file.parent, exist_ok=True)

        try:
            # Point 6 fix: correct boto3 API — download via the Object resource
            bucket.Object(obj.key).download_file(str(local_file))
            logger.info(f"Baixado: {local_file}")
        except Exception as e:
            logger.error(f"Erro ao baixar {obj.key}: {e}")
            raise


def download_scl_asset(output_dir: Path, id: str, scl_asset_href: str):
    """Baixa o asset SCL de uma cena Sentinel-2 L2A"""
    resp = requests.get(scl_asset_href, stream=True)
    resp.raise_for_status()
    with open(f"{output_dir}/{id}_SCL.tif", "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info(f"Asset SCL salvo")
    return None


def get_download_status(product_id: str, output_dir: Path, download_scl: bool) -> dict:
    """
    Retorna o status de download para SAFE e SCL (se aplicável)

    Returns:
        dict: {
            'safe_exists': bool,
            'scl_exists': bool (ou None se download_scl=False),
            'all_downloaded': bool  # True se tudo que precisa já foi baixado
        }
    """
    # Verifica SAFE
    safe_folder = Path(output_dir, product_id)
    safe_file = Path(output_dir) / f"{product_id}.SAFE"
    safe_exists = (
        safe_folder.exists() and safe_folder.is_dir() and any(safe_folder.iterdir())
    ) or safe_file.exists()

    # Verifica SCL apenas se necessário
    scl_exists = None
    if download_scl:
        product_core_id = product_id.split(".")[0]
        scl_path = Path(output_dir) / f"{product_core_id}_SCL.tif"
        scl_exists = scl_path.exists()

    # Determina se tudo que precisamos já foi baixado
    if download_scl:
        all_downloaded = safe_exists and scl_exists
    else:
        all_downloaded = safe_exists

    return {
        "safe_exists": safe_exists,
        "scl_exists": scl_exists,
        "all_downloaded": all_downloaded,
    }


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
        "skipped_no_need": 0,
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
            product_path = "/".join(img["href"].split("/")[3:])
            product_core_id = product_id.split(".")[0]

            # Verifica status atual
            status = get_download_status(product_id, output_dir, download_scl)

            # Se tudo já foi baixado, pula
            if status["all_downloaded"]:
                logger.info(f"[{field_date}] {product_id} - tudo já baixado, pulando")
                stats["already_downloaded"] += 1
                continue

            try:
                # Baixa o produto SAFE se necessário
                if not status["safe_exists"]:
                    logger.info(f"[{field_date}] Baixando {product_id}...")
                    download_product(s3.Bucket("eodata"), product_path, output_dir)
                    stats["safe_downloaded"] += 1
                    logger.info(f"✓ SAFE baixado: {product_id}")
                else:
                    logger.info(f"✓ SAFE já existe: {product_id}")

                # Baixa o asset SCL se necessário
                if download_scl and not status["scl_exists"]:
                    logger.info(f"  Baixando SCL...")
                    download_scl_asset(output_dir, product_core_id, img["l2a_cls"])
                    stats["scl_downloaded"] += 1
                    logger.info(f"  ✓ SCL baixado")
                elif download_scl and status["scl_exists"]:
                    logger.info(f"  ✓ SCL já existia")
                elif not download_scl:
                    stats["skipped_no_need"] += 1
                    logger.info(f"  ℹ SCL não solicitado para download")

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
    parser.add_argument("--csv", type=Path, help="CSV com datas de campo")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/sentinel_downloads"),
        help="Diretório de saída",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("data/sentinel_downloads/sentinel_catalog.json"),
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
            args.output_json,
            time_delta=args.time_delta,
            cloud_cover=args.cloud_cover,
        )

    if args.mode in ("download", "all"):
        run_download(
            args.output_json,
            args.output,
            only_first=args.only_first,
            download_scl=args.download_scl,
        )
