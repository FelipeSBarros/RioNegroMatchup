"""
utils.py
========
Utility functions for the aquamatch workflow.

Provides temporal tolerance analysis, campaign-level download auditing,
and L2W pixel-value extraction for satellite-vs-in-situ matchup analysis
of Sentinel-2 catalog data.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _flatten_images(images_found: dict | list) -> list[dict]:
    """
    Flatten a bucketed or legacy flat-list ``images_found`` entry into a
    single list of image dicts.

    Parameters
    ----------
    images_found:
        Either the bucketed dict ``{"same_day": [...], "previous": [...],
        "posterior": [...]}`` or a legacy flat list.

    Returns
    -------
    list[dict]
        All images across all buckets.
    """
    if isinstance(images_found, list):
        return images_found
    return (
        images_found.get("same_day", [])
        + images_found.get("previous", [])
        + images_found.get("posterior", [])
    )


def _iter_bucketed_images(images_found: dict | list):
    """
    Yield ``(bucket, image)`` pairs from a catalog entry's ``images_found``.

    Mirrors ``_flatten_images``'s bucket handling, but preserves which
    bucket each image came from (``audit_downloads`` needs to report it;
    ``_flatten_images`` intentionally discards it for its own callers).
    Legacy flat-list catalogs have no bucket information, so ``bucket``
    is ``None`` for every image in that case.

    Parameters
    ----------
    images_found:
        Either the bucketed dict ``{"same_day": [...], "previous": [...],
        "posterior": [...]}`` or a legacy flat list.

    Yields
    ------
    tuple[str | None, dict]
        ``(bucket_name_or_none, image_dict)``.
    """
    if isinstance(images_found, list):
        for img in images_found:
            yield None, img
        return
    for bucket in ("same_day", "previous", "posterior"):
        for img in images_found.get(bucket, []):
            yield bucket, img


# Mirrors aquamatch.sentinel_data.get_download_status()/get_scl_path().
# Duplicated (not imported) because sentinel_data.py performs a real
# network call and requires credentials at *module import time*
# (build_clients() at module scope) — importing it here would make a
# pure local filesystem check require network access. Keep in sync with
# the source of truth in sentinel_data.py if that logic changes.
_SCL_SUBDIR = "scl"


def _get_download_status(product_id: str, output_dir: Path, download_scl: bool) -> dict:
    """
    Local filesystem download status for a single scene.

    Parameters
    ----------
    product_id:
        Scene identifier (with or without ``.SAFE`` extension).
    output_dir:
        Root download directory.
    download_scl:
        Whether an SCL file is expected alongside the SAFE product.

    Returns
    -------
    dict
        ``{"safe_exists": bool, "scl_exists": bool | None,
        "all_downloaded": bool}``. ``scl_exists`` is ``None`` when
        ``download_scl`` is ``False``.
    """
    safe_folder = Path(output_dir) / product_id
    safe_file = Path(output_dir) / f"{product_id}.SAFE"
    safe_exists = (
        safe_folder.exists() and safe_folder.is_dir() and any(safe_folder.iterdir())
    ) or safe_file.exists()

    scl_exists = None
    if download_scl:
        product_core_id = product_id.split(".")[0]
        scl_path = Path(output_dir) / _SCL_SUBDIR / f"{product_core_id}_SCL.tif"
        scl_exists = scl_path.exists()

    all_downloaded = (safe_exists and scl_exists) if download_scl else safe_exists

    return {
        "safe_exists": safe_exists,
        "scl_exists": scl_exists,
        "all_downloaded": all_downloaded,
    }


def _best_image_within_tolerance(images: list[dict], max_delta: int) -> Optional[dict]:
    """
    Select the best image within ``max_delta`` days.

    Selection criteria (in order):
      1. Minimum ``delta_days``
      2. Minimum ``cloud_cover``

    Parameters
    ----------
    images:
        Flat list of image dicts, each with ``delta_days`` and
        ``cloud_cover`` keys.
    max_delta:
        Maximum allowed ``delta_days`` (inclusive).

    Returns
    -------
    dict or None
        Best image dict, or ``None`` if no image satisfies the tolerance.
    """
    candidates = [
        img for img in images if img.get("delta_days", float("inf")) <= max_delta
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda img: (img.get("delta_days", 999), img.get("cloud_cover", 999)),
    )


def _compute_metrics_for_tolerance(catalog_entries: list[dict], max_delta: int) -> dict:
    """
    Compute availability and cloud cover metrics for a single tolerance value.

    Parameters
    ----------
    catalog_entries:
        List of catalog entries, each with ``field_date`` and
        ``images_found``.
    max_delta:
        Temporal tolerance in days.

    Returns
    -------
    dict
        Keys: ``delta_days``, ``n_dates``, ``n_available``, ``availability``,
        ``opportunity_cost``, ``mean_cloud_cover``, ``median_cloud_cover``.
    """
    n_dates = len(catalog_entries)
    selected_images: list[dict] = []

    for entry in catalog_entries:
        images = _flatten_images(entry.get("images_found", []))
        best = _best_image_within_tolerance(images, max_delta)
        if best is not None:
            selected_images.append(best)

    n_available = len(selected_images)
    availability = n_available / n_dates if n_dates > 0 else 0.0
    opportunity_cost = 1.0 - availability

    cloud_covers = [
        img["cloud_cover"]
        for img in selected_images
        if img.get("cloud_cover") is not None
    ]

    mean_cc = sum(cloud_covers) / len(cloud_covers) if cloud_covers else None
    sorted_cc = sorted(cloud_covers)
    n = len(sorted_cc)
    if n == 0:
        median_cc = None
    elif n % 2 == 1:
        median_cc = sorted_cc[n // 2]
    else:
        median_cc = (sorted_cc[n // 2 - 1] + sorted_cc[n // 2]) / 2.0

    return {
        "delta_days": max_delta,
        "n_dates": n_dates,
        "n_available": n_available,
        "availability": round(availability * 100, 2),
        "opportunity_cost": round(opportunity_cost * 100, 2),
        "mean_cloud_cover": round(mean_cc, 2) if mean_cc is not None else None,
        "median_cloud_cover": round(median_cc, 2) if median_cc is not None else None,
    }


def _rio_prepare(da):
    """
    Set rioxarray spatial dims on ``da`` and confirm it has a CRS.

    Mirrors the x/y dimension discovery used in
    ``aquamatch.acolite_spec.append_l2w_to_datacube`` — duplicated here
    rather than imported, for the same reason ``audit_downloads`` avoids
    importing ``aquamatch.sentinel_data``: this module is kept free of
    heavy/side-effecting imports at module scope.

    Parameters
    ----------
    da:
        An ``xarray.DataArray`` that may or may not have recognizable
        spatial dimensions.

    Returns
    -------
    tuple
        ``(da, x_dim, y_dim)`` with spatial dims set via
        ``da.rio.set_spatial_dims``, or ``(None, None, None)`` if no
        recognizable x/y dimension pair exists, or the array has no CRS.
    """
    x_dim = next((d for d in da.dims if d in ("x", "lon", "longitude")), None)
    y_dim = next((d for d in da.dims if d in ("y", "lat", "latitude")), None)
    if x_dim is None or y_dim is None:
        return None, None, None
    da = da.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim)
    if da.rio.crs is None:
        return None, None, None
    return da, x_dim, y_dim


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def analyze_temporal_opportunity(
    catalog_json: Path | str,
    output_figure: Path | str,
    max_delta_days: int = 7,
    return_dataframe: bool = True,
    figure_dpi: int = 150,
) -> Optional[pd.DataFrame]:
    """
    Quantify the opportunity cost of requiring strict temporal coincidence
    between field measurements and Sentinel-2 acquisitions.

    For each temporal tolerance ``d`` from 0 to ``max_delta_days``, the
    function determines how many field dates have at least one image with
    ``delta_days <= d``, then computes:

    * **Availability** — percentage of field dates with a valid image.
    * **Opportunity cost** — ``1 - availability`` (percentage of dates
      with no valid image under that tolerance).
    * **Mean and median cloud cover** of the best image selected per date.

    The best image for each date is selected by minimum ``delta_days``,
    breaking ties by minimum ``cloud_cover``.

    A publication-quality three-panel PNG figure is written to
    ``output_figure`` showing availability, opportunity cost, and mean
    cloud cover against temporal tolerance.

    Parameters
    ----------
    catalog_json:
        Path to the catalog JSON produced by
        :func:`~aquamatch.sentinel_data.build_catalog`.
    output_figure:
        Destination path for the PNG figure.  Parent directories are
        created automatically.
    max_delta_days:
        Maximum temporal tolerance to evaluate (inclusive).
        Defaults to ``7``.
    return_dataframe:
        If ``True`` (default), return a ``pandas.DataFrame`` with one row
        per tolerance value.  If ``False``, return ``None``.
    figure_dpi:
        Resolution of the saved figure in dots per inch.  Defaults to
        ``150``.

    Returns
    -------
    pandas.DataFrame or None
        DataFrame with columns ``delta_days``, ``n_dates``,
        ``n_available``, ``availability``, ``opportunity_cost``,
        ``mean_cloud_cover``, ``median_cloud_cover`` — one row per
        tolerance value.  ``None`` when ``return_dataframe=False``.

    Raises
    ------
    FileNotFoundError
        If ``catalog_json`` does not exist.
    ValueError
        If ``max_delta_days`` is negative, or the catalog is empty.

    Examples
    --------
    >>> from aquamatch.utils import analyze_temporal_opportunity
    >>> df = analyze_temporal_opportunity(
    ...     catalog_json="data/sentinel_downloads/sentinel_catalog.json",
    ...     output_figure="reports/temporal_opportunity.png",
    ...     max_delta_days=5,
    ... )
    >>> print(df[["delta_days", "availability", "opportunity_cost"]])
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise ImportError(
            "analyze_temporal_opportunity requires matplotlib.\n"
            f"Original error: {exc}"
        ) from exc

    catalog_json = Path(catalog_json)
    output_figure = Path(output_figure)

    if not catalog_json.exists():
        raise FileNotFoundError(f"Catalog JSON not found: {catalog_json}")
    if max_delta_days < 0:
        raise ValueError(f"max_delta_days must be >= 0, got {max_delta_days}.")

    with catalog_json.open() as f:
        catalog_data: list[dict] = json.load(f)

    if not catalog_data:
        raise ValueError(f"Catalog is empty: {catalog_json}")

    logger.info(
        f"Analyzing temporal opportunity cost for {len(catalog_data)} field dates "
        f"(max_delta_days={max_delta_days})."
    )

    # --- Compute metrics for each tolerance ---
    rows = [
        _compute_metrics_for_tolerance(catalog_data, d)
        for d in range(0, max_delta_days + 1)
    ]
    df = pd.DataFrame(rows)

    logger.info(
        f"At d=0: availability={df.loc[0, 'availability']}%, "
        f"opportunity_cost={df.loc[0, 'opportunity_cost']}%"
    )
    logger.info(
        f"At d={max_delta_days}: availability={df.loc[max_delta_days, 'availability']}%, "
        f"opportunity_cost={df.loc[max_delta_days, 'opportunity_cost']}%"
    )

    # --- Build figure ---
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    fig.suptitle(
        "Sentinel-2 Temporal Opportunity Cost Analysis",
        fontsize=13,
        fontweight="bold",
        y=1.01,
    )

    x = df["delta_days"]

    # Panel 1 — Availability
    ax0 = axes[0]
    ax0.plot(x, df["availability"], marker="o", color="#2196F3", linewidth=2)
    ax0.fill_between(x, df["availability"], alpha=0.15, color="#2196F3")
    ax0.set_xlabel("Temporal tolerance (days)", fontsize=10)
    ax0.set_ylabel("Availability (%)", fontsize=10)
    ax0.set_title("Scene Availability", fontsize=11)
    ax0.set_ylim(0, 105)
    ax0.set_xticks(x)
    ax0.grid(True, linestyle="--", alpha=0.5)

    # Panel 2 — Opportunity cost
    ax1 = axes[1]
    ax1.plot(x, df["opportunity_cost"], marker="o", color="#F44336", linewidth=2)
    ax1.fill_between(x, df["opportunity_cost"], alpha=0.15, color="#F44336")
    ax1.set_xlabel("Temporal tolerance (days)", fontsize=10)
    ax1.set_ylabel("Opportunity cost (%)", fontsize=10)
    ax1.set_title("Opportunity Cost", fontsize=11)
    ax1.set_ylim(-5, 105)
    ax1.set_xticks(x)
    ax1.grid(True, linestyle="--", alpha=0.5)

    # Panel 3 — Mean cloud cover
    ax2 = axes[2]
    valid = df["mean_cloud_cover"].notna()
    ax2.plot(
        x[valid],
        df.loc[valid, "mean_cloud_cover"],
        marker="o",
        color="#4CAF50",
        linewidth=2,
        label="Mean",
    )
    ax2.plot(
        x[valid],
        df.loc[valid, "median_cloud_cover"],
        marker="s",
        color="#FF9800",
        linewidth=2,
        linestyle="--",
        label="Median",
    )
    ax2.set_xlabel("Temporal tolerance (days)", fontsize=10)
    ax2.set_ylabel("Cloud cover (%)", fontsize=10)
    ax2.set_title("Cloud Cover of Selected Scenes", fontsize=11)
    ax2.set_ylim(-2, 105)
    ax2.set_xticks(x)
    ax2.legend(fontsize=9)
    ax2.grid(True, linestyle="--", alpha=0.5)

    fig.tight_layout()

    output_figure.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_figure, dpi=figure_dpi, bbox_inches="tight")
    plt.close(fig)

    logger.info(f"Temporal opportunity cost figure saved: {output_figure}")

    return df if return_dataframe else None


def audit_downloads(
    catalog_json: Path | str,
    output_dir: Path | str,
    download_scl: bool = True,
) -> pd.DataFrame:
    """
    Audit which cataloged Sentinel-2 scenes are actually present on disk.

    Loads the catalog produced by
    :func:`~aquamatch.sentinel_data.build_catalog`, walks every image
    across the ``same_day``, ``previous``, and ``posterior`` buckets for
    each field date, and checks local download status for each scene.

    Parameters
    ----------
    catalog_json:
        Path to the catalog JSON produced by
        :func:`~aquamatch.sentinel_data.build_catalog`.
    output_dir:
        Root download directory — the same root used when downloading
        SAFE products (and, if applicable, SCL files under ``scl/``).
    download_scl:
        Whether SCL files are expected alongside SAFE products. Defaults
        to ``True``. When ``False``, ``scl_exists`` is ``None`` for
        every row.

    Returns
    -------
    pandas.DataFrame
        One row per cataloged scene, with columns:

        * ``field_date`` (str) — field sampling date.
        * ``scene_id`` (str) — Sentinel-2 scene identifier.
        * ``delta_days`` (int) — days from the field date.
        * ``cloud_cover`` (float) — scene cloud cover percentage.
        * ``safe_exists`` (bool) — SAFE folder present on disk.
        * ``scl_exists`` (bool or None) — SCL file present
          (``None`` when ``download_scl=False``).
        * ``all_downloaded`` (bool) — both SAFE and SCL (if required)
          present.
        * ``bucket`` (str or None) — ``"same_day"``, ``"previous"``,
          ``"posterior"``, or ``None`` for legacy flat-list catalogs
          with no bucket information.

    Raises
    ------
    FileNotFoundError
        If ``catalog_json`` does not exist.
    ValueError
        If the catalog is empty.

    Examples
    --------
    >>> from aquamatch.utils import audit_downloads
    >>> df = audit_downloads(
    ...     catalog_json="data/sentinel_downloads/sentinel_catalog.json",
    ...     output_dir="data/sentinel_downloads",
    ... )
    >>> df[~df["all_downloaded"]][["field_date", "scene_id", "bucket"]]
    """
    catalog_json = Path(catalog_json)
    output_dir = Path(output_dir)

    if not catalog_json.exists():
        raise FileNotFoundError(f"Catalog JSON not found: {catalog_json}")

    with catalog_json.open() as f:
        catalog_data: list[dict] = json.load(f)

    if not catalog_data:
        raise ValueError(f"Catalog is empty: {catalog_json}")

    logger.info(
        f"Auditing downloads for {len(catalog_data)} field dates "
        f"against output_dir={output_dir} (download_scl={download_scl})."
    )

    rows: list[dict] = []
    for entry in catalog_data:
        field_date = entry.get("field_date")
        images_found = entry.get("images_found", [])

        for bucket, img in _iter_bucketed_images(images_found):
            scene_id = img.get("id")
            status = _get_download_status(scene_id, output_dir, download_scl)
            rows.append(
                {
                    "field_date": field_date,
                    "scene_id": scene_id,
                    "delta_days": img.get("delta_days"),
                    "cloud_cover": img.get("cloud_cover"),
                    "safe_exists": status["safe_exists"],
                    "scl_exists": status["scl_exists"],
                    "all_downloaded": status["all_downloaded"],
                    "bucket": bucket,
                }
            )

    df = pd.DataFrame(
        rows,
        columns=[
            "field_date",
            "scene_id",
            "delta_days",
            "cloud_cover",
            "safe_exists",
            "scl_exists",
            "all_downloaded",
            "bucket",
        ],
    )

    n_downloaded = int(df["all_downloaded"].sum()) if len(df) else 0
    logger.info(f"Audit complete: {n_downloaded}/{len(df)} scenes fully downloaded.")

    return df


def extract_l2w_pixel_values(
    l2w_nc: Path | str,
    stations: pd.DataFrame,
    variables: Optional[list[str]] = None,
    window_size: int = 3,
    lat_col: str = "latitud",
    lon_col: str = "longitud",
) -> pd.DataFrame:
    """
    Extract mean L2W pixel values in a window around each station.

    For every row in ``stations``, finds the ACOLITE L2W raster pixel
    nearest that station's (``lat_col``, ``lon_col``) coordinate — assumed
    EPSG:4326/WGS84, matching the project's campaign CSVs — and computes
    the NaN-aware mean of a ``window_size`` x ``window_size`` pixel window
    centered on it, for every requested variable.

    This is step 1 of a future satellite-vs-in-situ matchup workflow: the
    input ``stations`` table is returned with new columns *appended*
    (never a fresh minimal DataFrame), so identifying columns (station
    id, date, etc.) survive for a later join against measured values.

    Stations outside the raster's bounding box entirely get NaN for every
    ``*_mean`` column, 0 for every ``*_n_valid_px`` column, and
    ``in_bounds=False`` — nearest-neighbor indexing would otherwise always
    return *some* pixel, however far away, silently producing a
    meaningless value.

    Parameters
    ----------
    l2w_nc:
        Path to a single ACOLITE L2W NetCDF file.
    stations:
        DataFrame with one row per station/date, containing at least
        ``lat_col`` and ``lon_col``. Returned unmodified except for
        appended columns.
    variables:
        L2W data variables to extract. If ``None``, auto-discovers all
        real data variables (excludes grid-mapping helpers, requires
        ``ndim >= 2``). If given, filtered to what's actually present.
    window_size:
        Side length of the square pixel window. Must be a positive odd
        integer. Defaults to ``3``.
    lat_col, lon_col:
        Station latitude/longitude column names. Default to
        ``"latitud"``/``"longitud"`` to match the project's real data.

    Returns
    -------
    pandas.DataFrame
        Copy of ``stations`` with, per extracted variable ``var``:

        * ``{var}_mean`` (float) — NaN-aware mean of the pixel window.
        * ``{var}_n_valid_px`` (int) — non-NaN pixels contributing to the
          mean (out of up to ``window_size**2``).

        Plus a shared ``in_bounds`` (bool) column.

    Raises
    ------
    FileNotFoundError
        If ``l2w_nc`` does not exist.
    ValueError
        If ``window_size`` isn't a positive odd integer; if
        ``lat_col``/``lon_col`` aren't columns of ``stations``; if the
        variable set (after filtering) is empty; or no variable has
        usable spatial dims/CRS.
    ImportError
        If xarray, rioxarray, or rasterio aren't installed.

    Examples
    --------
    >>> from aquamatch.utils import extract_l2w_pixel_values
    >>> stations = pd.read_csv("data/monitoring_data/campaigns_unique_data.csv")
    >>> result = extract_l2w_pixel_values(
    ...     l2w_nc="data/acolite_output/.../S2A_..._L2W.nc",
    ...     stations=stations,
    ...     variables=["chl_oc3"],
    ... )
    >>> result[["date", "latitud", "longitud",
    ...         "chl_oc3_mean", "chl_oc3_n_valid_px", "in_bounds"]]
    """
    try:
        import numpy as np
        import xarray as xr
        import rioxarray  # noqa: F401 - registers the .rio accessor
        import rasterio.warp
    except ImportError as exc:
        raise ImportError(
            "extract_l2w_pixel_values requires xarray, rioxarray, and rasterio.\n"
            f"Original error: {exc}"
        ) from exc

    l2w_nc = Path(l2w_nc)
    if not l2w_nc.exists():
        raise FileNotFoundError(f"L2W NetCDF not found: {l2w_nc}")

    if window_size < 1 or window_size % 2 == 0:
        raise ValueError(
            f"window_size must be a positive odd integer, got {window_size}."
        )
    if lat_col not in stations.columns:
        raise ValueError(f"lat_col '{lat_col}' not found in stations columns.")
    if lon_col not in stations.columns:
        raise ValueError(f"lon_col '{lon_col}' not found in stations columns.")

    # Duplicated from aquamatch.acolite_spec.append_l2w_to_datacube rather
    # than imported — see audit_downloads for why this module avoids
    # importing sibling modules with heavier import footprints.
    GRID_MAPPING_NAMES = {
        "transverse_mercator",
        "polar_stereographic",
        "lambert_conformal_conic",
        "spatial_ref",
        "crs",
        "grid_mapping",
    }

    ds = xr.open_dataset(l2w_nc, decode_coords="all")
    try:
        data_vars = [
            v for v in ds.data_vars if v not in GRID_MAPPING_NAMES and ds[v].ndim >= 2
        ]
        if variables is not None:
            data_vars = [v for v in variables if v in data_vars]
        if not data_vars:
            raise ValueError(f"No exportable variables found in {l2w_nc.name}.")

        out = stations.copy()

        if out.empty:
            for var in data_vars:
                out[f"{var}_mean"] = pd.Series(dtype="float64")
                out[f"{var}_n_valid_px"] = pd.Series(dtype="int64")
            out["in_bounds"] = pd.Series(dtype="bool")
            logger.info(
                "stations is empty — returning correctly-columned empty result."
            )
            return out

        # --- Establish pixel geometry from the first usable variable ---
        ref_var = ref_da = ref_x_dim = ref_y_dim = None
        for var in data_vars:
            da, x_dim, y_dim = _rio_prepare(ds[var])
            if da is not None:
                ref_var, ref_da, ref_x_dim, ref_y_dim = var, da, x_dim, y_dim
                break

        if ref_var is None:
            raise ValueError(
                f"No variables with valid spatial dimensions/CRS found in "
                f"{l2w_nc.name}."
            )

        raster_crs = ref_da.rio.crs
        n_rows = ref_da.sizes[ref_y_dim]
        n_cols = ref_da.sizes[ref_x_dim]
        minx, miny, maxx, maxy = ref_da.rio.bounds()

        logger.info(
            f"Extracting {len(data_vars)} variable(s) for {len(out)} station(s) "
            f"from {l2w_nc.name} (CRS={raster_crs}, grid={n_rows}x{n_cols}, "
            f"window_size={window_size})."
        )

        lons = out[lon_col].to_numpy(dtype="float64")
        lats = out[lat_col].to_numpy(dtype="float64")
        xs_native, ys_native = rasterio.warp.transform(
            "EPSG:4326", raster_crs, lons.tolist(), lats.tolist()
        )
        xs_native = np.asarray(xs_native)
        ys_native = np.asarray(ys_native)

        in_bounds = (
            (xs_native >= minx)
            & (xs_native <= maxx)
            & (ys_native >= miny)
            & (ys_native <= maxy)
        )
        n_oob = int((~in_bounds).sum())
        if n_oob:
            logger.info(
                f"{n_oob}/{len(out)} station(s) fall outside the raster's "
                "bounding box — will get NaN mean(s) and in_bounds=False."
            )

        x_index = ref_da.get_index(ref_x_dim)
        y_index = ref_da.get_index(ref_y_dim)
        col_pos = x_index.get_indexer(xs_native, method="nearest")
        row_pos = y_index.get_indexer(ys_native, method="nearest")

        half = window_size // 2

        for var in data_vars:
            da, x_dim, y_dim = _rio_prepare(ds[var])
            if da is None:
                logger.warning(f"Skipping '{var}': no recognizable x/y dims or CRS.")
                continue
            if da.sizes[y_dim] != n_rows or da.sizes[x_dim] != n_cols:
                logger.warning(
                    f"Skipping '{var}': grid shape {da.sizes[y_dim]}x{da.sizes[x_dim]} "
                    f"does not match reference grid {n_rows}x{n_cols}."
                )
                continue

            values = da.transpose(y_dim, x_dim, ...).values
            values = np.squeeze(values)
            if values.ndim != 2:
                logger.warning(
                    f"Skipping '{var}': data is not 2-D after squeezing "
                    "extra dimensions."
                )
                continue

            means = np.full(len(out), np.nan, dtype="float64")
            n_valid = np.zeros(len(out), dtype="int64")

            for i in range(len(out)):
                if not in_bounds[i]:
                    continue
                r, c = int(row_pos[i]), int(col_pos[i])
                r0, r1 = max(0, r - half), min(n_rows, r + half + 1)
                c0, c1 = max(0, c - half), min(n_cols, c + half + 1)
                window = values[r0:r1, c0:c1]
                valid = ~np.isnan(window)
                n = int(valid.sum())
                n_valid[i] = n
                if n > 0:
                    means[i] = float(np.nanmean(window))

            out[f"{var}_mean"] = means
            out[f"{var}_n_valid_px"] = n_valid

        out["in_bounds"] = in_bounds

        return out
    finally:
        ds.close()
