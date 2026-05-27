"""
scl_utils.py
============
Utilities for extracting water-body polygons from Sentinel-2
Scene Classification Layer (SCL) assets and persisting them as
vector data for use in ACOLITE atmospheric correction workflows.

SCL pixel values (ESA Sen2Cor convention)
------------------------------------------
    0  No Data
    1  Saturated / Defective
    2  Dark Area Pixels
    3  Cloud Shadow
    4  Vegetation
    5  Not Vegetated
    6  Water                   ← used here
    7  Unclassified
    8  Cloud medium probability
    9  Cloud high probability
    10 Thin Cirrus
    11 Snow / Ice

Typical usage
-------------
Single scene — extract water polygon and wire it into AcoliteConfig::

    from rionegromatchup.scl_utils import scl_water_to_geojson
    from rionegromatchup.acolite_spec import AcoliteConfig, IOConfig

    geojson_path = scl_water_to_geojson(
        scl_path="data/sentinel_downloads/S2A_MSIL1C_20250801_SCL.tif",
        output_path="data/water_polygons/S2A_20250801_water.geojson",
    )

    cfg = AcoliteConfig(
        acolite_executable="/path/to/acolite",
        io=IOConfig(
            inputfile="data/sentinel_downloads/S2A_MSIL1C_20250801.SAFE",
            output="data/acolite_output",
        ),
    )
    cfg = cfg.with_scl_polygon(scl_path=geojson_path)
    result = cfg.run()

Building a persistent vector datacube across scenes::

    from rionegromatchup.scl_utils import build_water_polygon_datacube

    records = [
        {"date": "2025-08-01", "scene_id": "S2A_...", "scl_path": "...SCL.tif"},
        {"date": "2025-08-15", "scene_id": "S2B_...", "scl_path": "...SCL.tif"},
    ]
    build_water_polygon_datacube(
        records=records,
        output_path="data/water_polygons/water_extents.gpkg",
    )

Notes on SCL water detection quality
--------------------------------------
ESA's Sen2Cor classifier is conservative with class 6 (water).
Known limitations in turbid systems like the Río Negro / Río Uruguay:

- Highly turbid water may be labelled as "not vegetated" (class 5)
  due to high NIR backscatter.
- Shallow or partially vegetated margins may be missed entirely.
- Cloud shadows over water (class 3) reduce the detected water extent.

For production use consider combining SCL class 6 with an NDWI
threshold derived from the L1C bands as a secondary validation step.
See ``scl_water_to_geojson`` parameter ``ndwi_fallback_threshold``
for a lightweight implementation of this approach.
"""

from __future__ import annotations

import logging
import re
from datetime import date as DateType
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# SCL pixel value for water
SCL_WATER_VALUE = 6


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------

def scl_water_to_geojson(
    scl_path: str | Path,
    output_path: str | Path,
    scene_date: Optional[str | DateType] = None,
    scene_id: Optional[str] = None,
    min_area_m2: float = 5000.0,
    simplify_tolerance: float = 20.0,
    buffer_m: float = 0.0,
    overwrite: bool = False,
) -> Path:
    """
    Extract water-body polygons from a Sentinel-2 SCL GeoTIFF and save
    them as a GeoJSON file suitable for use as an ACOLITE polygon mask.

    The function vectorises all pixels with SCL value == 6 (water),
    optionally simplifies and buffers the result, and writes a single
    GeoJSON FeatureCollection.  The CRS is always reprojected to
    EPSG:4326 so the output is immediately usable by ACOLITE's
    ``polygon`` parameter.

    Parameters
    ----------
    scl_path:
        Path to the SCL GeoTIFF (e.g. ``S2A_MSIL1C_20250801_SCL.tif``).
        Must be a single-band raster with SCL class values.
    output_path:
        Destination path for the GeoJSON file.
        Parent directories are created if necessary.
    scene_date:
        Acquisition date as a string ``"YYYY-MM-DD"`` or a
        ``datetime.date`` object.  If ``None``, the function attempts
        to parse the date from ``scl_path`` filename.
        Stored as a ``date`` property on each GeoJSON feature.
    scene_id:
        Sentinel-2 scene identifier stored as a ``scene_id`` property
        on each GeoJSON feature.  Defaults to the SCL filename stem.
    min_area_m2:
        Minimum polygon area in square metres.  Polygons smaller than
        this threshold are discarded to remove noise from isolated water
        pixels and cloud-shadow speckle.
        Default: 5 000 m² (roughly 12 × 10 m pixels).
    simplify_tolerance:
        Tolerance in metres for polygon simplification (Douglas-Peucker).
        Reduces vertex count for complex coastlines and wetland edges.
        Set to 0 to disable simplification.
        Default: 20 m (two SCL pixels).
    buffer_m:
        Optional buffer in metres applied to the water polygons before
        export.  A small positive buffer (e.g. 30–60 m) captures
        shoreline pixels that may be mis-classified as land due to
        mixed-pixel effects at 20 m resolution.
        Set to 0 to disable buffering (default).
    overwrite:
        If ``False`` (default) and ``output_path`` already exists, the
        existing file is returned without reprocessing.

    Returns
    -------
    Path
        Resolved path to the written GeoJSON file.

    Raises
    ------
    ImportError
        If rasterio or geopandas are not installed.
    FileNotFoundError
        If ``scl_path`` does not exist.
    ValueError
        If the SCL raster contains no water pixels after filtering.

    Notes
    -----
    The SCL is at 20 m resolution.  ACOLITE internally clips its 10 m
    and 20 m band processing to the supplied polygon, so the slight
    resolution mismatch at polygon edges is acceptable in practice.
    """
    try:
        import rasterio
        from rasterio.features import shapes as rasterio_shapes
        import geopandas as gpd
        from shapely.geometry import shape, MultiPolygon
        from shapely.ops import unary_union
    except ImportError as exc:
        raise ImportError(
            "scl_water_to_geojson requires rasterio and geopandas.\n"
            "Install with: pip install rasterio geopandas\n"
            f"Original error: {exc}"
        ) from exc

    scl_path = Path(scl_path).resolve()
    output_path = Path(output_path).resolve()

    if not scl_path.exists():
        raise FileNotFoundError(f"SCL raster not found: {scl_path}")

    if output_path.exists() and not overwrite:
        logger.info(f"Water polygon already exists, skipping: {output_path}")
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # --- Resolve scene metadata ---
    if scene_id is None:
        scene_id = scl_path.stem

    if scene_date is None:
        scene_date = _parse_date_from_scl_path(scl_path)
        if scene_date is None:
            logger.warning(
                f"Could not parse date from SCL filename '{scl_path.name}'. "
                "Feature properties will have date=null."
            )

    date_str = str(scene_date) if scene_date is not None else None
    logger.info(
        f"Extracting water polygons | scene={scene_id} | date={date_str} | "
        f"min_area={min_area_m2} m² | simplify={simplify_tolerance} m"
    )

    # ------------------------------------------------------------------
    # 1.  Read SCL, create binary water mask
    # ------------------------------------------------------------------
    with rasterio.open(scl_path) as src:
        scl = src.read(1)
        src_crs = src.crs
        transform = src.transform
        nodata = src.nodata

        water_mask = (scl == SCL_WATER_VALUE).astype(np.uint8)
        total_water_px = int(water_mask.sum())

    logger.info(
        f"SCL read: {scl.shape} pixels | water pixels = {total_water_px}"
    )

    if total_water_px == 0:
        raise ValueError(
            f"No water pixels (SCL class {SCL_WATER_VALUE}) found in "
            f"{scl_path.name}. Check the SCL asset and scene coverage."
        )

    # ------------------------------------------------------------------
    # 2.  Vectorise the binary mask (rasterio.features.shapes)
    # ------------------------------------------------------------------
    # shapes() yields (geojson_geom_dict, pixel_value) for contiguous
    # regions.  We only keep value == 1 (water).
    raw_shapes = [
        shape(geom)
        for geom, val in rasterio_shapes(
            water_mask,
            mask=water_mask,       # only trace the "1" (water) regions
            transform=transform,
        )
        if val == 1
    ]

    logger.info(f"Vectorised into {len(raw_shapes)} raw polygon(s)")

    # ------------------------------------------------------------------
    # 3.  Build GeoDataFrame in the SCL native CRS for metric operations
    # ------------------------------------------------------------------
    gdf = gpd.GeoDataFrame(geometry=raw_shapes, crs=src_crs)

    # Ensure a projected (metric) CRS for area filtering / simplification
    if not gdf.crs.is_projected:
        # Auto-UTM from centroid
        centroid = gdf.union_all().centroid
        utm_crs = gdf.estimate_utm_crs()
        gdf = gdf.to_crs(utm_crs)
        logger.info(f"Reprojected to {utm_crs} for metric operations")
    else:
        utm_crs = gdf.crs

    # ------------------------------------------------------------------
    # 4.  Filter by minimum area
    # ------------------------------------------------------------------
    before = len(gdf)
    gdf = gdf[gdf.geometry.area >= min_area_m2].copy()
    after = len(gdf)
    logger.info(
        f"Area filter ({min_area_m2} m²): {before} → {after} polygon(s)"
    )

    if gdf.empty:
        raise ValueError(
            f"All water polygons are smaller than min_area_m2={min_area_m2} m². "
            "Lower the threshold or check the SCL asset."
        )

    # ------------------------------------------------------------------
    # 5.  Optional: simplify vertices
    # ------------------------------------------------------------------
    if simplify_tolerance > 0:
        gdf["geometry"] = gdf.geometry.simplify(
            tolerance=simplify_tolerance,
            preserve_topology=True,
        )
        logger.info(f"Simplified polygons with tolerance={simplify_tolerance} m")

    # ------------------------------------------------------------------
    # 6.  Optional: buffer
    # ------------------------------------------------------------------
    if buffer_m != 0:
        gdf["geometry"] = gdf.geometry.buffer(buffer_m)
        logger.info(f"Applied {buffer_m} m buffer")

    # ------------------------------------------------------------------
    # 7.  Merge into a single MultiPolygon feature, reproject to WGS84
    # ------------------------------------------------------------------
    merged = unary_union(gdf.geometry)
    if merged.geom_type == "Polygon":
        # Wrap in a list for consistent FeatureCollection output
        geometries = [merged]
    else:
        geometries = list(merged.geoms)

    result_gdf = gpd.GeoDataFrame(
        {
            "scene_id":    [scene_id] * len(geometries),
            "date":        [date_str] * len(geometries),
            "scl_source":  [str(scl_path)] * len(geometries),
            "n_water_px":  [total_water_px] * len(geometries),
        },
        geometry=geometries,
        crs=utm_crs,
    ).to_crs("EPSG:4326")

    # ------------------------------------------------------------------
    # 8.  Write GeoJSON
    # ------------------------------------------------------------------
    result_gdf.to_file(output_path, driver="GeoJSON")

    total_area_km2 = gdf.geometry.area.sum() / 1e6
    logger.info(
        f"Water polygon saved → {output_path} | "
        f"{len(result_gdf)} feature(s) | total area ≈ {total_area_km2:.2f} km²"
    )
    return output_path


# ---------------------------------------------------------------------------
# Vector datacube (GeoPackage with temporal index)
# ---------------------------------------------------------------------------

def build_water_polygon_datacube(
    records: list[dict],
    output_path: str | Path,
    overwrite: bool = False,
    **scl_kwargs,
) -> Path:
    """
    Extract water polygons for a list of SCL scenes and accumulate them
    into a single GeoPackage (vector datacube) with a ``date`` column.

    Each call to this function is idempotent with respect to the
    ``output_path``: if the file already exists and ``overwrite=False``,
    new records are appended rather than replacing existing ones.
    Duplicate dates from the same ``scene_id`` are skipped with a warning.

    The resulting GeoPackage can be opened in QGIS, loaded with
    geopandas, or queried with any OGR-compatible tool::

        import geopandas as gpd
        water = gpd.read_file("data/water_polygons/water_extents.gpkg")
        water.plot(column="date")

    Parameters
    ----------
    records:
        List of dicts, each describing one SCL scene.  Required keys:

        ``scl_path``  (str | Path)
            Path to the SCL GeoTIFF for this scene.

        Optional keys that override auto-detection:

        ``date``      (str)  — ``"YYYY-MM-DD"``
        ``scene_id``  (str)  — Sentinel-2 scene identifier

    output_path:
        Path to the output GeoPackage file (``*.gpkg``).
        A ``.geojson`` extension is also accepted but GeoPackage is
        recommended for large datasets.
    overwrite:
        If True, delete the existing file before processing.
        If False (default), append new records to the existing file.
    **scl_kwargs:
        Additional keyword arguments forwarded to ``scl_water_to_geojson``
        for every scene (e.g. ``min_area_m2``, ``simplify_tolerance``,
        ``buffer_m``).

    Returns
    -------
    Path
        Resolved path to the GeoPackage.

    Raises
    ------
    ImportError
        If geopandas is not installed.
    """
    try:
        import geopandas as gpd
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "build_water_polygon_datacube requires geopandas.\n"
            "Install with: pip install geopandas\n"
            f"Original error: {exc}"
        ) from exc

    output_path = Path(output_path).resolve()

    if overwrite and output_path.exists():
        output_path.unlink()
        logger.info(f"Removed existing datacube: {output_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing datacube if present
    if output_path.exists():
        existing = gpd.read_file(output_path)
        existing_keys = set(
            zip(existing["scene_id"].astype(str), existing["date"].astype(str))
        )
        logger.info(
            f"Existing datacube loaded: {len(existing)} feature(s) | "
            f"{existing['date'].nunique()} unique date(s)"
        )
    else:
        existing = None
        existing_keys = set()

    new_frames: list[gpd.GeoDataFrame] = []
    tmp_dir = output_path.parent / "_scl_tmp"
    tmp_dir.mkdir(exist_ok=True)

    for idx, record in enumerate(records):
        scl_path = Path(record["scl_path"])
        scene_id = record.get("scene_id", scl_path.stem)
        date_val = record.get("date")

        # Parse date if not provided
        if date_val is None:
            date_val = _parse_date_from_scl_path(scl_path)
            if date_val is not None:
                date_val = str(date_val)

        key = (str(scene_id), str(date_val))
        if key in existing_keys:
            logger.info(
                f"[{idx+1}/{len(records)}] Skipping duplicate: "
                f"scene_id={scene_id}, date={date_val}"
            )
            continue

        tmp_geojson = tmp_dir / f"{scl_path.stem}_water.geojson"

        try:
            scl_water_to_geojson(
                scl_path=scl_path,
                output_path=tmp_geojson,
                scene_date=date_val,
                scene_id=scene_id,
                overwrite=True,
                **scl_kwargs,
            )
            frame = gpd.read_file(tmp_geojson)
            new_frames.append(frame)
            logger.info(
                f"[{idx+1}/{len(records)}] Added: "
                f"scene_id={scene_id}, date={date_val}, "
                f"features={len(frame)}"
            )
        except (ValueError, FileNotFoundError) as exc:
            logger.warning(
                f"[{idx+1}/{len(records)}] Skipping {scl_path.name}: {exc}"
            )
            continue

    if not new_frames:
        logger.info("No new scenes to append — datacube unchanged.")
        return output_path

    combined_new = gpd.GeoDataFrame(
        pd.concat(new_frames, ignore_index=True),
        crs=new_frames[0].crs,
    )

    if existing is not None:
        # Ensure CRS consistency before concatenation
        if existing.crs != combined_new.crs:
            combined_new = combined_new.to_crs(existing.crs)
        combined = gpd.GeoDataFrame(
            pd.concat([existing, combined_new], ignore_index=True),
            crs=existing.crs,
        )
    else:
        combined = combined_new

    # Sort chronologically for tidy access patterns
    if "date" in combined.columns:
        combined = combined.sort_values("date").reset_index(drop=True)

    driver = "GPKG" if output_path.suffix.lower() == ".gpkg" else "GeoJSON"
    combined.to_file(output_path, driver=driver)

    logger.info(
        f"Vector datacube saved → {output_path} | "
        f"{len(combined)} total feature(s) | "
        f"{combined['date'].nunique() if 'date' in combined.columns else '?'} "
        f"unique date(s)"
    )

    # Clean up temp files
    import shutil
    shutil.rmtree(tmp_dir, ignore_errors=True)

    return output_path


# ---------------------------------------------------------------------------
# AcoliteConfig integration helper
# ---------------------------------------------------------------------------

def prepare_acolite_water_polygon(
    cfg: "AcoliteConfig",
    scl_path: str | Path,
    geojson_output_dir: Optional[str | Path] = None,
    overwrite: bool = False,
    **scl_kwargs,
) -> "AcoliteConfig":
    """
    Derive a water-body polygon from an SCL asset and wire it into an
    ``AcoliteConfig`` instance by setting ``io.polygon`` and adding
    ``polygon_clip=True`` to the settings.

    This is the recommended entry point when integrating SCL-derived
    water masks into the ACOLITE processing workflow.

    Parameters
    ----------
    cfg:
        An ``AcoliteConfig`` instance (from ``acolite_spec.py``).
        Its ``io.output`` directory is used as the default GeoJSON
        output location if ``geojson_output_dir`` is not provided.
    scl_path:
        Path to the SCL GeoTIFF for the scene being processed.
    geojson_output_dir:
        Directory where the GeoJSON water polygon will be saved.
        Defaults to ``<cfg.io.output>/water_polygons/``.
    overwrite:
        Overwrite the GeoJSON if it already exists.
    **scl_kwargs:
        Extra arguments forwarded to ``scl_water_to_geojson``
        (e.g. ``min_area_m2``, ``simplify_tolerance``, ``buffer_m``).

    Returns
    -------
    AcoliteConfig
        The **same** config object with ``io.polygon`` patched to the
        GeoJSON path.  A ``polygon_clip`` key is also injected into the
        settings serialisation via a monkey-patched ``to_settings_dict``.

    Example
    -------
        from rionegromatchup.scl_utils import prepare_acolite_water_polygon

        cfg = prepare_acolite_water_polygon(
            cfg=cfg,
            scl_path="data/sentinel_downloads/S2A_20250801_SCL.tif",
            buffer_m=30,
        )
        result = cfg.run()
    """
    from pathlib import Path as _Path

    scl_path = _Path(scl_path)
    scene_id = scl_path.stem.replace("_SCL", "")

    if geojson_output_dir is None:
        geojson_output_dir = _Path(cfg.io.output) / "water_polygons"
    else:
        geojson_output_dir = _Path(geojson_output_dir)

    geojson_path = geojson_output_dir / f"{scene_id}_water.geojson"

    scl_water_to_geojson(
        scl_path=scl_path,
        output_path=geojson_path,
        scene_id=scene_id,
        overwrite=overwrite,
        **scl_kwargs,
    )

    # Patch the config
    cfg.io.polygon = str(geojson_path)

    # Inject polygon_clip into serialisation by wrapping to_settings_dict
    _original_to_settings_dict = cfg.to_settings_dict.__func__  # unbound method

    def _patched_to_settings_dict(self) -> dict:
        d = _original_to_settings_dict(self)
        d["polygon_clip"] = "true"
        # Remove limit when polygon is set — they are mutually exclusive
        d.pop("limit", None)
        return d

    import types
    cfg.to_settings_dict = types.MethodType(_patched_to_settings_dict, cfg)

    logger.info(
        f"Water polygon wired into AcoliteConfig: {geojson_path} | "
        "polygon_clip=True"
    )
    return cfg


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_date_from_scl_path(scl_path: Path) -> Optional[DateType]:
    """
    Attempt to parse an acquisition date from an SCL filename.

    Handles common Sentinel-2 filename patterns:
        S2A_MSIL1C_20250801T...   → 2025-08-01
        S2A_MSIL1C_20250801_SCL   → 2025-08-01
        S2A_MSI_2017_07_13_..._SCL → 2017-07-13
    """
    import datetime

    name = scl_path.stem

    # Pattern 1: compact YYYYMMDD embedded in name
    match = re.search(r"[_T](\d{8})[_T]", name)
    if match:
        try:
            return datetime.date(
                int(match.group(1)[:4]),
                int(match.group(1)[4:6]),
                int(match.group(1)[6:8]),
            )
        except ValueError:
            pass

    # Pattern 2: YYYY_MM_DD separated
    match = re.search(r"(\d{4})_(\d{2})_(\d{2})", name)
    if match:
        try:
            return datetime.date(
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
            )
        except ValueError:
            pass

    return None