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
