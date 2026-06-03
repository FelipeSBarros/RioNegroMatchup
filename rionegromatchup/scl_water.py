"""
scl_water.py
============
Extract water body polygons from Sentinel-2 Scene Classification Layer
(SCL) GeoTIFF files and persist them as GeoJSON / GeoPackage vector datasets.

SCL class values
----------------
0  No data
1  Saturated / defective
2  Dark area pixels
3  Cloud shadows
4  Vegetation
5  Not vegetated
6  Water  ← used here
7  Unclassified
8  Cloud medium probability
9  Cloud high probability
10 Thin cirrus
11 Snow / ice

Typical output layout (relative to the root download directory)
---------------------------------------------------------------
{output_dir}/scl/                               ← SCL GeoTIFFs (from sentinel_data)
{output_dir}/geojson/                           ← water polygon GeoJSON files (this module)
{output_dir}/water_polygons.gpkg                ← persistent datacube (Feature 2)
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# SCL pixel value that represents water
SCL_WATER_CLASS = 6

# Subdirectory (relative to output_dir) where GeoJSON files are written
GEOJSON_SUBDIR = "geojson"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_date_from_filename(filename: str) -> Optional[str]:
    """
    Try to extract an acquisition date (YYYY-MM-DD) from a Sentinel-2
    filename.  Two formats are supported:

    Compact  : ``S2A_MSIL1C_20250801T101031_...``  → ``2025-08-01``
    Separated: ``S2A_MSI_2017_07_13_14_01_45_...`` → ``2017-07-13``

    Returns None (with a warning) if no pattern matches — never raises.
    """
    # Compact: 8 consecutive digits that look like YYYYMMDD
    match = re.search(r"_(\d{8})T\d{6}_", filename)
    if match:
        raw = match.group(1)
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"

    # Separated: YYYY_MM_DD embedded in underscored components
    match = re.search(r"_(\d{4})_(\d{2})_(\d{2})_\d{2}_\d{2}_\d{2}_", filename)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    logger.warning(
        f"Could not parse acquisition date from filename '{filename}'. "
        "Setting date to None."
    )
    return None


# ---------------------------------------------------------------------------
# Feature 1 — SCL water extraction
# ---------------------------------------------------------------------------


def scl_water_to_geojson(
    scl_path: Path | str,
    output_path: Path | str,
    scene_date: Optional[str] = None,
    scene_id: Optional[str] = None,
    min_area_m2: float = 5_000,
    simplify_tolerance: float = 20,
    buffer_m: float = 0,
    overwrite: bool = False,
) -> Path:
    """
    Extract water pixels (SCL class 6) from a Sentinel-2 SCL GeoTIFF and
    write a GeoJSON FeatureCollection containing a single MultiPolygon
    feature in EPSG:4326.

    Parameters
    ----------
    scl_path:
        Path to the input SCL GeoTIFF file.
    output_path:
        Destination GeoJSON file path.  The parent directory is created
        automatically if it does not exist.
    scene_date:
        Acquisition date as ``YYYY-MM-DD``.  Auto-detected from the
        filename when not provided.
    scene_id:
        Scene identifier string.  Defaults to the SCL filename stem.
    min_area_m2:
        Minimum polygon area in square metres.  Polygons below this
        threshold are discarded (removes noise / isolated pixels).
        Default: 5 000 m².
    simplify_tolerance:
        Douglas-Peucker simplification tolerance in metres.
        Default: 20 m (≈ 1 SCL pixel).
    buffer_m:
        Optional outward buffer in metres applied after simplification.
        Use a positive value (e.g. 30–60 m) for turbid / shallow water
        systems where shoreline pixels are mis-classified as land.
        Default: 0 m (no buffer).
    overwrite:
        If ``False`` (default) and ``output_path`` already exists, return
        the existing path without reprocessing (idempotent).
        If ``True``, overwrite the existing file.

    Returns
    -------
    Path
        Path to the written GeoJSON file.

    Raises
    ------
    FileNotFoundError
        If ``scl_path`` does not exist.
    ValueError
        If the SCL raster contains no water pixels (class 6).
    """
    try:
        import rasterio
        from rasterio.features import shapes
        import geopandas as gpd
        from shapely.geometry import MultiPolygon, shape
        from shapely.ops import unary_union
    except ImportError as exc:
        raise ImportError(
            "scl_water_to_geojson requires rasterio, geopandas, and shapely.\n"
            f"Original error: {exc}"
        ) from exc

    scl_path = Path(scl_path)
    output_path = Path(output_path)

    if not scl_path.exists():
        raise FileNotFoundError(f"SCL file not found: {scl_path}")

    # --- Idempotency check ---
    if output_path.exists() and not overwrite:
        logger.info(f"GeoJSON already exists, skipping: {output_path}")
        return output_path

    # --- Resolve metadata ---
    if scene_id is None:
        scene_id = scl_path.stem
    if scene_date is None:
        scene_date = _parse_date_from_filename(scl_path.name)

    logger.info(
        f"Processing SCL: {scl_path.name} | scene={scene_id} | date={scene_date}"
    )

    # --- Read SCL raster ---
    with rasterio.open(scl_path) as src:
        scl_data = src.read(1)
        scl_crs = src.crs
        scl_transform = src.transform

    # --- Identify water pixels ---
    water_mask = (scl_data == SCL_WATER_CLASS).astype(np.uint8)
    n_water_px = int(water_mask.sum())

    logger.info(f"Water pixels found: {n_water_px}")

    if n_water_px == 0:
        raise ValueError(
            f"No water pixels (SCL class {SCL_WATER_CLASS}) found in {scl_path.name}. "
            "Cannot generate water polygon."
        )

    # --- Vectorise water pixels ---
    raw_shapes = [
        shape(geom)
        for geom, val in shapes(water_mask, mask=water_mask, transform=scl_transform)
        if int(val) == 1
    ]

    logger.info(f"Raw polygons before filtering: {len(raw_shapes)}")

    # --- Project to metric CRS for area / simplification / buffer ---
    # estimate_utm_crs() reprojects to WGS84 internally before picking the
    # zone, so it works correctly regardless of the input CRS.
    gdf = gpd.GeoDataFrame(geometry=raw_shapes, crs=scl_crs)
    utm_crs = gdf.estimate_utm_crs()
    logger.info(f"Using metric CRS for filtering: {utm_crs}")
    gdf_utm = gdf.to_crs(utm_crs)

    # --- Area filter ---
    gdf_utm = gdf_utm[gdf_utm.geometry.area >= min_area_m2].copy()
    logger.info(f"Polygons after area filter (>= {min_area_m2} m²): {len(gdf_utm)}")

    if gdf_utm.empty:
        raise ValueError(
            f"All water polygons were removed by the area filter "
            f"(min_area_m2={min_area_m2}, n_water_px={n_water_px}). "
            "Try lowering min_area_m2 or check that the SCL covers the target water body."
        )

    # --- Simplification ---
    if simplify_tolerance > 0:
        gdf_utm["geometry"] = gdf_utm.geometry.simplify(
            simplify_tolerance, preserve_topology=True
        )

    # --- Optional buffer ---
    if buffer_m != 0:
        gdf_utm["geometry"] = gdf_utm.geometry.buffer(buffer_m)

    # --- Merge into a single MultiPolygon ---
    merged = unary_union(gdf_utm.geometry)
    # unary_union may return a Polygon if only one remains — normalise to Multi
    if merged.geom_type == "Polygon":
        merged = MultiPolygon([merged])

    # --- Reproject merged geometry to EPSG:4326 and attach properties ---
    gdf_out = gpd.GeoDataFrame(
        {
            "scene_id": [scene_id],
            "date": [scene_date],
            "scl_source": [str(scl_path)],
            "n_water_px": [n_water_px],
        },
        geometry=[merged],
        crs=utm_crs,
    ).to_crs("EPSG:4326")

    # --- Write output ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    gdf_out.to_file(output_path, driver="GeoJSON")

    logger.info(f"Water polygon GeoJSON written: {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Feature 2 — SCL path resolution and water polygon datacube
# ---------------------------------------------------------------------------


def resolve_scl_path(
    safe_path: Path | str,
    scl_dir: Path | str,
) -> Optional[Path]:
    """
    Derive the expected local SCL GeoTIFF path for a given SAFE folder.

    Resolution strategy
    -------------------
    1. Primary:  ``{scl_dir}/{stem}_SCL.tif``
       where ``stem`` is the SAFE folder name without the ``.SAFE`` extension.
       This matches the naming convention used by ``download_scl_asset``
       in ``sentinel_data.py``.
    2. Fallback: glob ``{scl_dir}/*{timestamp}*_SCL.tif``
       where ``timestamp`` is the compact acquisition datetime extracted
       from the SAFE stem (e.g. ``20170713T135111``).  Useful when the
       product baseline component of the filename differs between the
       SAFE folder and the downloaded SCL file.

    Returns ``None`` (with a warning) if neither strategy finds a file.

    Parameters
    ----------
    safe_path:
        Path to the Sentinel-2 SAFE folder
        (e.g. ``data/sentinel_downloads/S2A_MSIL1C_20170713T135111_...SAFE``).
    scl_dir:
        Directory containing SCL GeoTIFF files
        (typically ``{output_dir}/scl/``).

    Returns
    -------
    Path or None
        Resolved path to the SCL file, or None if not found.
    """
    safe_path = Path(safe_path)
    scl_dir = Path(scl_dir)

    stem = safe_path.stem  # strips .SAFE if present

    # --- Primary: exact name match ---
    primary = scl_dir / f"{stem}_SCL.tif"
    if primary.exists():
        logger.info(f"SCL resolved (primary): {primary}")
        return primary

    # --- Fallback: match on acquisition timestamp ---
    # Extract compact datetime component, e.g. "20170713T135111"
    ts_match = re.search(r"_(\d{8}T\d{6})_", safe_path.name)
    if ts_match:
        timestamp = ts_match.group(1)
        candidates = list(scl_dir.glob(f"*{timestamp}*_SCL.tif"))
        if candidates:
            fallback = candidates[0]
            logger.info(f"SCL resolved (fallback timestamp match): {fallback}")
            return fallback

    logger.warning(
        f"SCL file not found for {safe_path.name} in {scl_dir}. "
        "Download it first with sentinel_data.download_scl_asset()."
    )
    return None


def build_water_polygon_datacube(
    records: list[dict],
    output_path: Path | str,
    overwrite: bool = False,
    **scl_kwargs,
) -> Path:
    """
    Process a list of SCL scenes and accumulate their water body polygons
    into a single GeoPackage vector datacube.

    Each record in ``records`` must contain ``scl_path`` and may optionally
    include ``date`` and ``scene_id``.  Intermediate GeoJSON files are
    written to ``{output_path.parent}/geojson/`` and kept on disk.

    The function is **idempotent**: if ``output_path`` already exists,
    existing ``(scene_id, date)`` pairs are loaded and any duplicate
    scenes in the new batch are skipped with a warning.

    Parameters
    ----------
    records:
        List of dicts, each with:
            - ``scl_path`` (required): Path to the SCL GeoTIFF.
            - ``date`` (optional): Acquisition date ``YYYY-MM-DD``;
              auto-detected from filename if absent.
            - ``scene_id`` (optional): Scene identifier; defaults to the
              SCL filename stem.
    output_path:
        Destination GeoPackage file (``.gpkg``).  Created on first call;
        appended to on subsequent calls.
    overwrite:
        If ``True``, delete any existing GeoPackage and rebuild from
        scratch.  If ``False`` (default), append incrementally.
    **scl_kwargs:
        Extra keyword arguments forwarded to ``scl_water_to_geojson``
        (e.g. ``min_area_m2``, ``buffer_m``, ``simplify_tolerance``).

    Returns
    -------
    Path
        Path to the GeoPackage file.
    """
    try:
        import geopandas as gpd
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "build_water_polygon_datacube requires geopandas and pandas.\n"
            f"Original error: {exc}"
        ) from exc

    output_path = Path(output_path)
    geojson_dir = output_path.parent / GEOJSON_SUBDIR

    # --- Handle overwrite ---
    if overwrite and output_path.exists():
        output_path.unlink()
        logger.info(f"Existing datacube removed for overwrite: {output_path}")

    # --- Load existing datacube to check for duplicates ---
    existing_keys: set[tuple] = set()
    if output_path.exists():
        existing = gpd.read_file(output_path)
        for _, row in existing.iterrows():
            existing_keys.add((row["scene_id"], str(row["date"])[:10]))
        logger.info(
            f"Loaded existing datacube: {len(existing)} features, "
            f"{len(existing_keys)} unique (scene_id, date) pairs."
        )

    new_gdfs: list[gpd.GeoDataFrame] = []
    stats = {"processed": 0, "skipped_duplicate": 0, "skipped_no_water": 0, "errors": 0}

    for record in records:
        scl_path = Path(record["scl_path"])
        scene_date = record.get("date") or _parse_date_from_filename(scl_path.name)
        scene_id = record.get("scene_id") or scl_path.stem

        # --- Duplicate check ---
        key = (scene_id, str(scene_date)[:10] if scene_date else None)
        if key in existing_keys:
            logger.warning(
                f"Duplicate (scene_id={scene_id}, date={scene_date}) — skipping."
            )
            stats["skipped_duplicate"] += 1
            continue

        # --- Derive GeoJSON output path ---
        geojson_path = geojson_dir / f"{scene_id}_water.geojson"

        # --- Extract water polygon ---
        try:
            scl_water_to_geojson(
                scl_path=scl_path,
                output_path=geojson_path,
                scene_date=scene_date,
                scene_id=scene_id,
                **scl_kwargs,
            )
        except ValueError as exc:
            logger.warning(f"Skipping {scene_id}: {exc}")
            stats["skipped_no_water"] += 1
            continue
        except Exception as exc:
            logger.error(f"Error processing {scene_id}: {exc}")
            stats["errors"] += 1
            continue

        new_gdfs.append(gpd.read_file(geojson_path))
        existing_keys.add(key)
        stats["processed"] += 1

    # --- Append new features to GeoPackage ---
    if new_gdfs:
        new_combined = pd.concat(new_gdfs, ignore_index=True)

        if output_path.exists():
            existing_gdf = gpd.read_file(output_path)
            combined = pd.concat([existing_gdf, new_combined], ignore_index=True)
        else:
            combined = new_combined

        # Sort chronologically
        combined = combined.sort_values("date", ignore_index=True)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_file(output_path, driver="GPKG")
        logger.info(f"Datacube written: {output_path} ({len(combined)} total features)")
    else:
        logger.info("No new features to append.")

    logger.info(
        f"Summary — processed: {stats['processed']}, "
        f"duplicate skipped: {stats['skipped_duplicate']}, "
        f"no water: {stats['skipped_no_water']}, "
        f"errors: {stats['errors']}"
    )

    return output_path
