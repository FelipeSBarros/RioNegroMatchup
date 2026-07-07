import argparse
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd
import requests
from pystac_client import Client
from sentinelhub import CRS, BBox, DataCollection, SHConfig, SentinelHubCatalog

from aquamatch.credentials import SentinelCredentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

earthsearch_catalog_url = "https://earth-search.aws.element84.com/v1"

# Subdirectory name used for all SCL GeoTIFF files under the download root.
SCL_SUBDIR = "scl"


def _build_sh_catalog(creds: SentinelCredentials) -> SentinelHubCatalog:
    """Build a SentinelHubCatalog from resolved credentials. No network call —
    SHConfig/SentinelHubCatalog construction is purely local."""
    sh_config = SHConfig()
    sh_config.sh_client_id = creds.sh_client_id
    sh_config.sh_client_secret = creds.sh_client_secret
    sh_config.sh_base_url = creds.sh_base_url
    sh_config.sh_token_url = creds.sh_token_url
    return SentinelHubCatalog(config=sh_config)


def _build_stac_client() -> Client:
    """Open the EarthSearch STAC client. NOTE: this performs a real HTTP
    request to fetch the catalog root document — call only when a STAC
    client is actually needed (search_images path), not just to reach s3."""
    return Client.open(earthsearch_catalog_url)


def _build_s3_resource(creds: SentinelCredentials):
    """Build the Dataspace S3 resource from resolved credentials. No
    network call — boto3.resource() construction is lazy/local."""
    return boto3.resource(
        "s3",
        endpoint_url="https://eodata.dataspace.copernicus.eu",
        aws_access_key_id=creds.dataspace_access_key,
        aws_secret_access_key=creds.dataspace_secret_key,
        region_name="default",
    )


def build_clients(credentials: Optional[SentinelCredentials] = None):
    """Build (catalog, client, s3) from explicit credentials, or fall back to env vars.

    Kept as a single combinator for the module-level default construction
    and for build_catalog(), which genuinely needs both catalog and client.
    Callers that only need one of the three (e.g. run_download() only
    needs s3) should call the specific _build_*() helper directly instead,
    to avoid _build_stac_client()'s network round-trip when it isn't needed.
    """
    creds = credentials or SentinelCredentials.from_env()
    catalog_ = _build_sh_catalog(creds)
    client_ = _build_stac_client()
    s3_ = _build_s3_resource(creds)
    return catalog_, client_, s3_


catalog, client, s3 = build_clients()


def _tile_from_scene_id(scene_id: str) -> str | None:
    """
    Extract the 5-character MGRS tile code from a Sentinel-2 scene ID or href.

    Two formats are supported:

    Pattern 1 — ``_T21HUD_`` style, used in L1C/L2A product IDs and SAFE paths:
        ``S2A_MSIL1C_20170713T135111_N0500_R024_T21HUD_20230919T094731``

    Pattern 2 — ``/21/H/UD/`` style, used in EarthSearch S3 asset hrefs:
        ``https://sentinel-cogs.s3.us-west-2.amazonaws.com/.../21/H/UD/2020/5/.../SCL.tif``

    Returns the 5-character tile string (e.g. ``'21HUD'``), or ``None``
    if no match is found.
    """
    # Pattern 1: _T21HUD_ style (scene IDs, SAFE paths)
    match = re.search(r"_T([0-9]{2}[A-Z]{3})(?:_|\.SAFE|$)", scene_id)
    if match:
        return match.group(1)

    # Pattern 2: /21/H/UD/ style (EarthSearch S3 hrefs)
    match = re.search(r"/(\d{2})/([A-Z])/([A-Z]{2})/", scene_id)
    if match:
        return match.group(1) + match.group(2) + match.group(3)

    return None


def _temporal_bucket(acquisition_date: str, field_date: str) -> str:
    """
    Classify an image acquisition relative to its field date.

    Parameters
    ----------
    acquisition_date:
        Scene acquisition date as ``YYYY-MM-DD``.
    field_date:
        Field sampling date as ``YYYY-MM-DD``.

    Returns
    -------
    str
        One of ``"same_day"``, ``"previous"``, or ``"posterior"``.
    """
    acq = datetime.fromisoformat(acquisition_date)
    fld = datetime.fromisoformat(field_date)
    diff = (acq - fld).days
    if diff == 0:
        return "same_day"
    elif diff < 0:
        return "previous"
    else:
        return "posterior"


def _empty_buckets() -> dict:
    """Return an empty images_found bucket dict."""
    return {"same_day": [], "previous": [], "posterior": []}


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


def search_images(
    bbox_geometry,
    date: str,
    time_delta: int,
    cloud_cover: int,
    catalog=None,
    client=None,
):
    """
    Busca imagens Sentinel-2 L1C ± time_delta dias da data de campo,
    e para cada cena L1C encontrada, busca a cena L2A correspondente
    pela mesma data de aquisição.

    Parameters
    ----------
    catalog:
        Optional SentinelHubCatalog instance. Defaults to the module-level
        `catalog` (built from env vars or a previously-passed
        SentinelCredentials via build_clients()).
    client:
        Optional pystac_client.Client instance. Defaults to the
        module-level `client`.
    """
    # Resolve via globals() rather than a bound default, so patches to the
    # module-level attribute (aquamatch.sentinel_data.catalog / .client)
    # are still picked up at call time when no explicit override is given.
    _catalog = catalog if catalog is not None else globals()["catalog"]
    _client = client if client is not None else globals()["client"]

    date_obj = datetime.fromisoformat(date)
    start = (date_obj - timedelta(days=time_delta)).strftime("%Y-%m-%d")
    end = (date_obj + timedelta(days=time_delta)).strftime("%Y-%m-%d")

    logger.info(f"Buscando imagens entre {start} e {end} (cloud < {cloud_cover}%)")

    l1c_results = list(
        _catalog.search(
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
        acquisition_datetime = item["properties"]["datetime"]
        acquisition_date = acquisition_datetime[:10]

        logger.info(
            f"  Buscando L2A correspondente para {item_id} ({acquisition_date})"
        )

        l2a_results = list(
            _client.search(
                collections=["sentinel-2-l2a"],
                bbox=bbox_geometry,
                datetime=f"{acquisition_date}/{acquisition_date}",
                query={"eo:cloud_cover": {"lt": cloud_cover}},
            ).items()
        )

        scl_hrefs = []
        if l2a_results:
            for l2a_item in l2a_results:
                scl_asset = l2a_item.assets.get("scl")
                if scl_asset:
                    scl_hrefs.append(scl_asset.href)
            if not scl_hrefs:
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
                "l2a_scl": scl_hrefs,
            }
        )

    logger.info(f"Total de cenas encontradas: {len(items)}")
    return items


def build_catalog(
    csv_file: Path,
    output_json: Path,
    time_delta=1,
    cloud_cover=10,
    credentials: "Optional[SentinelCredentials]" = None,
):
    """
    Search for Sentinel-2 scenes matching each field date in *csv_file* and
    write a catalog JSON to *output_json*.

    Catalog schema
    --------------
    Each entry in the output list corresponds to one unique field date / location
    combination and has the form::

        {
            "field_date": "YYYY-MM-DD",
            "images_found": {
                "same_day":  [ <image>, ... ],   # delta_days == 0
                "previous":  [ <image>, ... ],   # acquired before field date
                "posterior": [ <image>, ... ]    # acquired after field date
            }
        }

    Within each bucket images are sorted by ``(delta_days, cloud_cover)``
    ascending, so the best candidate is always ``bucket[0]``.

    An *image* dict contains:
        - ``id``          — Sentinel-2 scene identifier
        - ``datetime``    — acquisition ISO-8601 datetime string
        - ``cloud_cover`` — cloud cover percentage
        - ``href``        — S3/CDSE download URL for the SAFE product
        - ``delta_days``  — absolute difference in days from the field date
        - ``l2a_scl``     — matching SCL asset URL, or ``None``

    Parameters
    ----------
    credentials:
        Optional SentinelCredentials for explicit client construction
        (e.g. Colab, or any environment where env vars aren't set).
        When provided, ``build_clients(credentials)`` is called once and
        the resulting catalog/client are reused for every field date in
        this call — not rebuilt per row. When ``None`` (default),
        :func:`search_images` falls back to its own resolution
        (module-level ``catalog``/``client`` globals), matching current
        behaviour exactly.
    """
    # Resolve once, up front — reused across every unique (date, location)
    # row below rather than rebuilt per row.
    _catalog = None
    _client = None
    if credentials is not None:
        _catalog, _client, _ = build_clients(credentials)

    df = pd.read_csv(csv_file, sep=None, engine="python")
    if "date" not in df.columns:
        raise ValueError("date column not found in CSV")
    if "longitud" not in df.columns or "latitud" not in df.columns:
        raise ValueError("longitud or latitud columns not found in CSV")

    # --- Tile filtering ---
    filter_by_tile = "s2_tile" in df.columns
    if not filter_by_tile:
        logger.warning(
            "Column 's2_tile' not found in CSV — tile filtering will be skipped. "
            "All scenes overlapping the search bbox will be included in the catalog. "
            "Re-run insitu_data.py in campaigns mode to generate a CSV with s2_tile."
        )

    unique_dates_places = df[["date", "longitud", "latitud"]].drop_duplicates()
    if filter_by_tile:
        unique_dates_places = df[
            ["date", "longitud", "latitud", "s2_tile"]
        ].drop_duplicates()

    # date → {scene_id → img}  (deduplication layer, same as before)
    scenes_by_date: dict[str, dict] = defaultdict(dict)

    for idx, row in unique_dates_places.iterrows():
        field_date = row["date"]
        expected_tile = row["s2_tile"] if filter_by_tile else None
        bbox_geometry = create_bbox_from_point(row["longitud"], row["latitud"])

        logger.info(
            f"Processando data {field_date} | lon={row['longitud']} lat={row['latitud']}"
            + (f" | tile={expected_tile}" if filter_by_tile else "")
        )
        images = search_images(
            bbox_geometry,
            field_date,
            time_delta,
            cloud_cover,
            catalog=_catalog,
            client=_client,
        )

        for img in images:
            scene_id = img["id"]

            # --- L1C tile filter ---
            if filter_by_tile:
                scene_tile = _tile_from_scene_id(scene_id)
                if scene_tile != expected_tile:
                    logger.debug(
                        f"  Discarding {scene_id}: tile {scene_tile} != "
                        f"expected {expected_tile}"
                    )
                    continue

            # --- SCL tile resolution ---
            scl_hrefs = img["l2a_scl"]  # list collected by search_images
            if scl_hrefs:
                if filter_by_tile:
                    matched_scl = next(
                        (
                            h
                            for h in scl_hrefs
                            if _tile_from_scene_id(h) == expected_tile
                        ),
                        None,
                    )
                    if matched_scl is None:
                        logger.warning(
                            f"  No SCL href matched tile {expected_tile} for "
                            f"{scene_id} — discarding SCL asset."
                        )
                else:
                    matched_scl = scl_hrefs[0]
            else:
                matched_scl = None
            img = {**img, "l2a_scl": matched_scl}

            if scene_id not in scenes_by_date[field_date]:
                scenes_by_date[field_date][scene_id] = {
                    **img,
                    "_field_date": field_date,  # carry field_date for bucketing
                }
                logger.info(f"  Nova cena adicionada: {scene_id}")
            else:
                logger.info(f"  Cena duplicada ignorada: {scene_id}")

    # --- Build bucketed output ---
    catalog_data = []
    for field_date, scenes in scenes_by_date.items():
        buckets = _empty_buckets()

        for img in scenes.values():
            acquisition_date = img["datetime"][:10]
            bucket = _temporal_bucket(acquisition_date, field_date)

            # Drop the internal helper key before storing
            clean_img = {k: v for k, v in img.items() if k != "_field_date"}
            buckets[bucket].append(clean_img)

        # Sort each bucket: closest first, then lowest cloud cover
        for bucket_name in buckets:
            buckets[bucket_name].sort(key=lambda x: (x["delta_days"], x["cloud_cover"]))

        catalog_data.append(
            {
                "field_date": field_date,
                "images_found": buckets,
            }
        )

    output_json.parent.mkdir(parents=True, exist_ok=True)
    with open(output_json, "w") as f:
        json.dump(catalog_data, f, indent=2)

    n_dates = len(catalog_data)
    n_images = sum(
        len(e["images_found"]["same_day"])
        + len(e["images_found"]["previous"])
        + len(e["images_found"]["posterior"])
        for e in catalog_data
    )
    logger.info(
        f"Catálogo salvo em {output_json} "
        f"({n_dates} datas processadas, {n_images} cenas no total)"
    )


def download_product(bucket, product: str, output_dir: Path):
    """
    Baixa todos os arquivos de um produto Sentinel do bucket S3.

    Args:
        bucket: boto3 Bucket resource object
        product: S3 prefix path to the product
        output_dir: local directory to save downloaded files
    """
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
            bucket.Object(obj.key).download_file(str(local_file))
            logger.info(f"Baixado: {local_file}")
        except Exception as e:
            logger.error(f"Erro ao baixar {obj.key}: {e}")
            raise


def download_scl_asset(output_dir: Path, id: str, scl_asset_href: str) -> Path:
    """
    Baixa o asset SCL de uma cena Sentinel-2 L2A.

    SCL files are saved under ``{output_dir}/scl/`` to keep them
    separate from the SAFE product folders.

    Args:
        output_dir: root download directory (same root used for SAFE products)
        id: product core identifier (without .SAFE), used as the filename stem
        scl_asset_href: URL of the SCL GeoTIFF asset

    Returns:
        Path to the downloaded SCL file.
    """
    scl_dir = Path(output_dir) / SCL_SUBDIR
    scl_dir.mkdir(parents=True, exist_ok=True)

    scl_path = scl_dir / f"{id}_SCL.tif"

    resp = requests.get(scl_asset_href, stream=True)
    resp.raise_for_status()
    with open(scl_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(f"Asset SCL salvo em {scl_path}")
    return scl_path


def get_scl_path(product_id: str, output_dir: Path) -> Path:
    """
    Returns the expected local path for an SCL file given a product ID
    and the root download directory.

    This is the single source of truth for SCL path resolution —
    used by both get_download_status() and resolve_scl_path().

    Args:
        product_id: product identifier (with or without .SAFE extension)
        output_dir: root download directory

    Returns:
        Expected Path to the SCL GeoTIFF (may or may not exist yet).
    """
    product_core_id = product_id.split(".")[0]
    return Path(output_dir) / SCL_SUBDIR / f"{product_core_id}_SCL.tif"


def get_download_status(product_id: str, output_dir: Path, download_scl: bool) -> dict:
    """
    Retorna o status de download para SAFE e SCL (se aplicável).

    SCL files are expected under ``{output_dir}/scl/`` (see SCL_SUBDIR).

    Returns:
        dict: {
            'safe_exists': bool,
            'scl_exists': bool (or None if download_scl=False),
            'all_downloaded': bool  # True if everything needed is already present
        }
    """
    # Verifica SAFE
    safe_folder = Path(output_dir) / product_id
    safe_file = Path(output_dir) / f"{product_id}.SAFE"
    safe_exists = (
        safe_folder.exists() and safe_folder.is_dir() and any(safe_folder.iterdir())
    ) or safe_file.exists()

    # Verifica SCL apenas se necessário
    scl_exists = None
    if download_scl:
        scl_path = get_scl_path(product_id, output_dir)
        scl_exists = scl_path.exists()

    if download_scl:
        all_downloaded = safe_exists and scl_exists
    else:
        all_downloaded = safe_exists

    return {
        "safe_exists": safe_exists,
        "scl_exists": scl_exists,
        "all_downloaded": all_downloaded,
    }


def _select_scenes(
    images_found: "dict | list",
    strategy: str = "best",
    max_per_date: int = 1,
    max_cloud_cover: "float | None" = None,
) -> list[dict]:
    """
    Select scenes to download from an ``images_found`` entry.

    Parameters
    ----------
    images_found:
        Either the bucketed dict ``{"same_day": [...], "previous": [...],
        "posterior": [...]}`` from the new catalog schema, or a flat list
        from the legacy schema.
    strategy:
        How to select scenes.

        * ``"best"``      — fill quota from ``same_day`` first, then
          ``previous``, then ``posterior``.
        * ``"all"``       — return every scene across all buckets.
        * ``"same_day"``  — return only scenes from the ``same_day``
          bucket; skip the date entirely if none are available.
        * ``"previous"``  — prefer ``same_day``; fall back to
          ``previous`` only.  Posterior scenes are never included.
        * ``"posterior"`` — prefer ``same_day``; fall back to
          ``posterior`` only.  Previous scenes are never included.

        Within each bucket scenes are already sorted by
        ``(delta_days, cloud_cover)`` ascending, so ``bucket[0]`` is
        always the best candidate.
    max_per_date:
        Maximum number of scenes to return.  Ignored when
        ``strategy="all"``.  Defaults to ``1``.
    max_cloud_cover:
        Optional cloud cover ceiling applied as a pre-filter before
        strategy selection.  ``None`` means no additional filtering
        (the search already applied its own ceiling).

    Returns
    -------
    list[dict]
        Ordered list of image dicts ready for download.
    """
    # --- Normalise legacy flat-list catalogs ---
    if isinstance(images_found, list):
        pool = images_found
        if max_cloud_cover is not None:
            pool = [img for img in pool if img.get("cloud_cover", 0) <= max_cloud_cover]
        if strategy == "all":
            return pool
        return pool[:max_per_date]

    # --- Bucketed schema ---
    same_day = images_found.get("same_day", [])
    previous = images_found.get("previous", [])
    posterior = images_found.get("posterior", [])

    # Apply optional cloud cover pre-filter to each bucket
    if max_cloud_cover is not None:
        same_day = [
            img for img in same_day if img.get("cloud_cover", 0) <= max_cloud_cover
        ]
        previous = [
            img for img in previous if img.get("cloud_cover", 0) <= max_cloud_cover
        ]
        posterior = [
            img for img in posterior if img.get("cloud_cover", 0) <= max_cloud_cover
        ]

    if strategy == "all":
        return same_day + previous + posterior

    if strategy == "same_day":
        return same_day[:max_per_date]

    if strategy == "previous":
        # same_day preferred; fall back to previous only — never posterior
        buckets = (same_day, previous)
    elif strategy == "posterior":
        # same_day preferred; fall back to posterior only — never previous
        buckets = (same_day, posterior)
    else:
        # "best": fill quota across all three buckets in priority order
        buckets = (same_day, previous, posterior)

    selected: list[dict] = []
    remaining = max_per_date
    for bucket in buckets:
        if remaining <= 0:
            break
        take = bucket[:remaining]
        selected.extend(take)
        remaining -= len(take)

    return selected


def run_download(
    catalog_json: Path,
    output_dir: Path,
    strategy: str = "best",
    max_per_date: int = 1,
    max_cloud_cover: "int | None" = None,
    download_scl: bool = True,
    s3=None,
    credentials: "Optional[SentinelCredentials]" = None,
):
    """
    ...
    Parameters
    ----------
    ...
    credentials:
        Optional SentinelCredentials. Used to build an S3 resource via
        _build_s3_resource() directly — NOT via build_clients() — since
        run_download() never needs a STAC client, and building one would
        cost an unnecessary network round-trip to EarthSearch. Ignored
        if `s3` is given.
    """
    if s3 is not None:
        _s3 = s3
    elif credentials is not None:
        _s3 = _build_s3_resource(credentials)
    else:
        _s3 = globals()["s3"]

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
        images_found = entry["images_found"]

        to_download = _select_scenes(
            images_found,
            strategy=strategy,
            max_per_date=max_per_date,
            max_cloud_cover=max_cloud_cover,
        )

        if not to_download:
            logger.warning(f"Nenhuma imagem para {field_date}")
            continue

        for img in to_download:
            stats["total_processed"] += 1
            product_id = img["id"]
            product_path = "/".join(img["href"].split("/")[3:])
            product_core_id = product_id.split(".")[0]

            status = get_download_status(product_id, output_dir, download_scl)

            if status["all_downloaded"]:
                logger.info(f"[{field_date}] {product_id} - tudo já baixado, pulando")
                stats["already_downloaded"] += 1
                continue

            try:
                if not status["safe_exists"]:
                    logger.info(f"[{field_date}] Baixando {product_id}...")
                    download_product(_s3.Bucket("eodata"), product_path, output_dir)
                    stats["safe_downloaded"] += 1
                    logger.info(f"✓ SAFE baixado: {product_id}")
                else:
                    logger.info(f"✓ SAFE já existe: {product_id}")

                if download_scl and not status["scl_exists"]:
                    logger.info(f"  Baixando SCL para {product_core_id}...")
                    scl_path = download_scl_asset(
                        output_dir, product_core_id, img["l2a_scl"]
                    )
                    stats["scl_downloaded"] += 1
                    logger.info(f"  ✓ SCL baixado: {scl_path}")
                elif download_scl and status["scl_exists"]:
                    logger.info(
                        f"  ✓ SCL já existia: {get_scl_path(product_id, output_dir)}"
                    )
                elif not download_scl:
                    stats["skipped_no_need"] += 1
                    logger.info(f"  ℹ SCL não solicitado para download")

            except Exception as e:
                logger.error(f"✗ Erro ao baixar {product_id}: {e}")
                stats["errors"] += 1

    logger.info("\n" + "=" * 50)
    logger.info("RELATÓRIO DE DOWNLOAD")
    logger.info("=" * 50)
    logger.info(f"Total processado: {stats['total_processed']}")
    logger.info(f"Já baixados: {stats['already_downloaded']}")
    logger.info(f"SAFE baixados: {stats['safe_downloaded']}")
    logger.info(f"SCL baixados: {stats['scl_downloaded']}")
    logger.info(f"Erros: {stats['errors']}")
    logger.info("=" * 50)
    return stats


# ---------------------------------------------------------------------------
# Public pipeline wrapper
# ---------------------------------------------------------------------------


def run_sentinel_pipeline(
    csv: "Path | str | None" = None,
    catalog_json: "Path | str | None" = None,
    output_dir: "Path | str | None" = None,
    time_delta: int | None = None,
    cloud_cover: int | None = None,
    strategy: "str | None" = None,
    max_per_date: "int | None" = None,
    max_cloud_cover: "int | None" = None,
    download_scl: bool | None = None,
    mode: str = "all",
    credentials: "SentinelCredentials | dict | None" = None,
) -> dict:
    """
    Run the Sentinel-2 catalog and/or download pipeline and return a status dict.

    Parameters
    ----------
    csv:
        Path to the deduplicated in situ CSV.
        Defaults to ``data/monitoring_data/campaigns_unique_data.csv``.
    catalog_json:
        Path to the catalog JSON file.
        Defaults to ``data/sentinel_downloads/sentinel_catalog.json``.
    output_dir:
        Root directory for downloaded SAFE products and SCL files.
        Defaults to ``data/sentinel_downloads``.
    time_delta:
        Search window in days around each field date (±N days).
        Defaults to ``1``.
    cloud_cover:
        Maximum cloud cover percentage for scene selection.
        Defaults to ``10``.
    strategy:
        Download selection strategy forwarded to :func:`run_download`.
        One of ``"best"``, ``"all"``, ``"same_day"``, ``"previous"``,
        ``"posterior"``.  Defaults to ``"best"``.
    max_per_date:
        Maximum number of scenes to download per field date.
        Ignored when ``strategy="all"``.  Defaults to ``1``.
    max_cloud_cover:
        Optional secondary cloud cover ceiling applied at download time.
        ``None`` means no additional filtering.
    download_scl:
        If ``True``, download the SCL GeoTIFF alongside each SAFE product.
        Defaults to ``True``.
    mode:
        Which pipeline stages to run.  One of ``"all"``, ``"catalog"``,
        or ``"download"``.  Defaults to ``"all"``.
    credentials:
        Optional SentinelCredentials instance, or a plain dict with the
        same field names (e.g. {"sh_client_id": "...", "dataspace_access_key": "..."}),
        for explicit client construction (e.g. Colab). Forwarded to both
        build_catalog() and run_download() as-is. When None (default),
        both fall back to their own module-level client resolution —
        unchanged behaviour.

    Returns
    -------
    dict
        ``{"step": "sentinel", "status": "success", "outputs": {...},
        "error": None, "elapsed_seconds": float}``
    """
    import time

    valid_steps = {"all", "catalog", "download"}
    if mode not in valid_steps:
        raise ValueError(
            f"Invalid mode value '{mode}'. Must be one of: {sorted(valid_steps)}"
        )

    from aquamatch.pipeline_config import DownloadSection, SentinelSection

    _s = SentinelSection()
    _d = DownloadSection()

    # --- Normalise credentials: accept a SentinelCredentials instance,
    # a plain dict, or None. A dict is converted once here so the exact
    # same SentinelCredentials object is reused by both build_catalog()
    # and run_download() below, rather than each call constructing its own.
    _credentials = None
    if credentials is not None:
        _credentials = (
            credentials
            if isinstance(credentials, SentinelCredentials)
            else SentinelCredentials(**credentials)
        )

    unique_csv_path = Path(csv) if csv is not None else Path(_s.csv)
    catalog_json_path = (
        Path(catalog_json) if catalog_json is not None else Path(_s.catalog_json)
    )
    output_dir_path = (
        Path(output_dir) if output_dir is not None else Path(_d.output_dir)
    )
    time_delta = time_delta if time_delta is not None else _s.time_delta
    cloud_cover = cloud_cover if cloud_cover is not None else _s.cloud_cover
    _strategy = strategy if strategy is not None else _d.strategy
    _max_per_date = max_per_date if max_per_date is not None else _d.max_per_date
    _max_cloud_cover = (
        max_cloud_cover if max_cloud_cover is not None else _d.max_cloud_cover
    )
    _download_scl = download_scl if download_scl is not None else _d.download_scl

    t0 = time.monotonic()
    outputs = {}

    try:
        if mode in ("catalog", "all"):
            logger.info(
                f"[sentinel] Building catalog from {unique_csv_path} "
                f"→ {catalog_json_path}"
            )
            build_catalog(
                csv_file=unique_csv_path,
                output_json=catalog_json_path,
                time_delta=time_delta,
                cloud_cover=cloud_cover,
                credentials=_credentials,
            )
            outputs["catalog_json"] = catalog_json_path

        if mode in ("download", "all"):
            logger.info(
                f"[sentinel] Downloading products from {catalog_json_path} "
                f"→ {output_dir_path}"
            )
            download_stats = run_download(
                catalog_json=catalog_json_path,
                output_dir=output_dir_path,
                strategy=_strategy,
                max_per_date=_max_per_date,
                max_cloud_cover=_max_cloud_cover,
                download_scl=_download_scl,
                credentials=_credentials,
            )
            outputs["output_dir"] = output_dir_path
            outputs["download_stats"] = download_stats

        return {
            "step": "sentinel",
            "status": "success",
            "outputs": outputs,
            "error": None,
            "elapsed_seconds": round(time.monotonic() - t0, 2),
        }

    except Exception as exc:
        logger.error(f"run_sentinel_pipeline failed: {exc}")
        return {
            "step": "sentinel",
            "status": "error",
            "outputs": outputs,
            "error": str(exc),
            "elapsed_seconds": round(time.monotonic() - t0, 2),
        }


def _build_sentinel_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m aquamatch.sentinel_data",
        description="Pipeline Sentinel-2 (catalogar e baixar imagens)",
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
        default=True,
        help="Baixar asset SCL junto com produtos SAFE",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("./data/monitoring_data/campaigns_unique_data.csv"),
        help="CSV com datas de campo",
    )
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
    parser.add_argument("--cloud-cover", type=int, default=10, help="Nuvem máxima (%%)")
    parser.add_argument(
        "--strategy",
        default="best",
        choices=["best", "all", "same_day", "previous", "posterior"],
        help=(
            "Download selection strategy. "
            "best: same_day → previous → posterior (default). "
            "all: download every scene found."
        ),
    )
    parser.add_argument(
        "--max-per-date",
        type=int,
        default=1,
        help="Maximum number of scenes to download per field date (ignored for 'all').",
    )
    parser.add_argument(
        "--max-cloud-cover",
        type=int,
        default=None,
        help="Optional secondary cloud cover ceiling applied at download time.",
    )

    # --- Credentials overrides (Task 8) ---
    # All default to None. None here means "not overridden on the CLI";
    # the actual env-var fallback happens in _credentials_from_cli_args(),
    # not in argparse itself.
    parser.add_argument(
        "--sh-client-id",
        default=None,
        help="Sentinel Hub client ID. Overrides SH_CLIENT_ID from .env when set.",
    )
    parser.add_argument(
        "--sh-client-secret",
        default=None,
        help="Sentinel Hub client secret. Overrides SH_CLIENT_SECRET from .env when set.",
    )
    parser.add_argument(
        "--dataspace-access-key",
        default=None,
        help=(
            "Copernicus Dataspace S3 access key. "
            "Overrides DATASPACE_ACCESS_KEY from .env when set."
        ),
    )
    parser.add_argument(
        "--dataspace-secret-key",
        default=None,
        help=(
            "Copernicus Dataspace S3 secret key. "
            "Overrides DATASPACE_SECRET_KEY from .env when set."
        ),
    )
    return parser


def _credentials_from_cli_args(
    args: "argparse.Namespace",
) -> "Optional[SentinelCredentials]":
    """
    Build a SentinelCredentials from parsed CLI args, merged on top of
    the environment — or None if no credential flag was passed at all.

    Design rationale: a CLI user typically wants to override just ONE
    secret (e.g. --sh-client-id for a quick test) while still relying on
    .env for everything else. Returning a SentinelCredentials with the
    other three fields left at None would silently blank those out once
    build_clients() sees credentials is not None (it then skips
    from_env() entirely — see Task 6's explicit-wins precedence). So:
    start from from_env(), then overlay only the CLI-provided fields.

    Returns None (not a SentinelCredentials with all-None fields) when
    no --sh-client-id / --sh-client-secret / --dataspace-access-key /
    --dataspace-secret-key flag was given, so the rest of the pipeline
    falls back to its existing module-level client resolution — fully
    unchanged behaviour for users who don't touch these new flags.
    """
    cli_fields = {
        "sh_client_id": args.sh_client_id,
        "sh_client_secret": args.sh_client_secret,
        "dataspace_access_key": args.dataspace_access_key,
        "dataspace_secret_key": args.dataspace_secret_key,
    }
    if all(v is None for v in cli_fields.values()):
        return None

    base = SentinelCredentials.from_env()
    merged = {
        k: (v if v is not None else getattr(base, k)) for k, v in cli_fields.items()
    }
    return SentinelCredentials(
        sh_base_url=base.sh_base_url,
        sh_token_url=base.sh_token_url,
        **merged,
    )


if __name__ == "__main__":
    _parser = _build_sentinel_parser()
    args = _parser.parse_args()

    result = run_sentinel_pipeline(
        csv=args.csv,
        catalog_json=args.output_json,
        output_dir=args.output,
        time_delta=args.time_delta,
        cloud_cover=args.cloud_cover,
        strategy=args.strategy,
        max_per_date=args.max_per_date,
        max_cloud_cover=args.max_cloud_cover,
        download_scl=args.download_scl,
        mode=args.mode,
        credentials=_credentials_from_cli_args(args),
    )
    if result["status"] != "success":
        logger.error(f"Pipeline failed: {result['error']}")
