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
    product_core_id = product_id.split(".")[0]

    # SAFE folders are downloaded nested under the S3 key's full prefix
    # (e.g. Sentinel-2/MSI/{L1C|L1C_N0500}/{YYYY}/{MM}/{DD}/{scene}.SAFE),
    # and the baseline segment (L1C vs L1C_N0500) isn't reliably derivable
    # from the catalog's href — so search by name instead of reconstructing
    # the path. Mirrors the pattern already used in acolite_spec.py for the
    # same "find .SAFE folders regardless of nesting" problem.
    safe_matches = list(Path(output_dir).rglob(f"{product_core_id}.SAFE"))
    safe_exists = any(
        m.is_file() or (m.is_dir() and any(m.iterdir())) for m in safe_matches
    )

    scl_exists = None
    if download_scl:
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


def _windowed_nanmean(values, row_pos, col_pos, half, n_rows, n_cols):
    """
    NaN-aware windowed mean of a 2-D array at a set of pixel positions.

    For each ``(row_pos[i], col_pos[i])`` pair, computes the mean of the
    ``(2*half+1) x (2*half+1)`` window centered on it (clipped at the
    array edges), ignoring NaN pixels.

    Parameters
    ----------
    values:
        2-D array (rows, cols) to sample from.
    row_pos, col_pos:
        Integer pixel row/column indices, one per output position.
    half:
        Half the window side length (``window_size // 2``).
    n_rows, n_cols:
        Shape of ``values`` — passed explicitly rather than read from
        ``values.shape`` since callers already have them on hand.

    Returns
    -------
    tuple[numpy.ndarray, numpy.ndarray]
        ``(means, n_valid)`` — float64 NaN-aware means and int64 valid-pixel
        counts, one pair per input position.
    """
    import numpy as np

    n = len(row_pos)
    means = np.full(n, np.nan, dtype="float64")
    n_valid = np.zeros(n, dtype="int64")

    for i in range(n):
        r, c = int(row_pos[i]), int(col_pos[i])
        r0, r1 = max(0, r - half), min(n_rows, r + half + 1)
        c0, c1 = max(0, c - half), min(n_cols, c + half + 1)
        window = values[r0:r1, c0:c1]
        valid = ~np.isnan(window)
        n_pix = int(valid.sum())
        n_valid[i] = n_pix
        if n_pix > 0:
            means[i] = float(np.nanmean(window))

    return means, n_valid


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

            if in_bounds.any():
                sub_means, sub_valid = _windowed_nanmean(
                    values, row_pos[in_bounds], col_pos[in_bounds], half, n_rows, n_cols
                )
                means[in_bounds] = sub_means
                n_valid[in_bounds] = sub_valid

            out[f"{var}_mean"] = means
            out[f"{var}_n_valid_px"] = n_valid

        out["in_bounds"] = in_bounds

        return out
    finally:
        ds.close()


def extract_datacube_pixel_values(
    datacube_path: Path | str,
    stations: pd.DataFrame,
    variables: Optional[list[str]] = None,
    window_size: int = 3,
    lat_col: str = "latitud",
    lon_col: str = "longitud",
    date_col: str = "date",
    time_tolerance_days: int = 0,
    datacube_crs: str = "EPSG:4326",
) -> pd.DataFrame:
    """
    Extract mean pixel values from a multi-date L2W Zarr datacube, matched
    per station to its own sampling date.

    This is the multi-scene counterpart to :func:`extract_l2w_pixel_values`:
    instead of reading one L2W NetCDF for every station regardless of date,
    it reads a Zarr datacube built by
    :func:`~aquamatch.acolite_spec.append_l2w_to_datacube` (with a real
    ``time`` dimension) and, for every row of ``stations``, selects the
    pixel window at *that row's own* matched time step. Rows sharing the
    same matched date reuse a single load of that date's 2-D slice.

    Parameters
    ----------
    datacube_path:
        Path to the Zarr datacube (e.g. ``data/data_cube/l2w_datacube.zarr``).
    stations:
        DataFrame with one row per station/date, containing at least
        ``lat_col``, ``lon_col``, and ``date_col``. Returned unmodified
        except for appended columns.
    variables:
        Datacube variables to extract. If ``None``, auto-discovers all
        variables with ``time``, ``y``, and ``x`` dimensions. If given,
        filtered to what's actually present.
    window_size:
        Side length of the square pixel window. Must be a positive odd
        integer. Defaults to ``3``.
    lat_col, lon_col:
        Station latitude/longitude column names (assumed EPSG:4326).
        Default to ``"latitud"``/``"longitud"``.
    date_col:
        Station sampling-date column name. Defaults to ``"date"``.
    time_tolerance_days:
        Maximum allowed difference, in days, between a station's date and
        the nearest datacube time step. ``0`` (the default) requires an
        exact calendar-date match. Ties (equal distance to two time steps)
        break toward the earlier occurrence.
    datacube_crs:
        CRS of the datacube's ``x``/``y`` coordinates. Defaults to
        ``"EPSG:4326"``, matching :func:`~aquamatch.acolite_spec.append_l2w_to_datacube`'s
        own default. Pass the actual ``target_crs`` used when building the
        cube if it differs — this is not auto-detected from the store's
        grid-mapping metadata, which can be misleading (e.g. named
        ``transverse_mercator`` even after reprojection to EPSG:4326).

    Returns
    -------
    pandas.DataFrame
        Copy of ``stations`` with, per extracted variable ``var``:

        * ``{var}_mean`` (float) — NaN-aware mean of the pixel window.
        * ``{var}_n_valid_px`` (int) — non-NaN pixels contributing to the
          mean (out of up to ``window_size**2``).

        Plus ``in_bounds`` (bool, True only when both spatially inside the
        cube's grid and a time step was matched within tolerance) and
        ``matched_time_delta_days`` (float, NaN when unmatched) — the
        latter is a diagnostic for spotting silent date-matching mismatches.

    Raises
    ------
    FileNotFoundError
        If ``datacube_path`` does not exist.
    ValueError
        If ``window_size`` isn't a positive odd integer; if
        ``time_tolerance_days`` is negative; if ``lat_col``/``lon_col``/
        ``date_col`` aren't columns of ``stations``; if the variable set
        (after filtering) is empty; or the datacube has no time steps.
    ImportError
        If xarray or rasterio aren't installed.

    Examples
    --------
    >>> from aquamatch.utils import extract_datacube_pixel_values
    >>> stations = pd.read_csv("data/monitoring_data/campaigns_unique_data.csv")
    >>> result = extract_datacube_pixel_values(
    ...     datacube_path="data/data_cube/l2w_datacube.zarr",
    ...     stations=stations,
    ...     variables=["chl_oc3"],
    ... )
    >>> result[["date", "latitud", "longitud",
    ...         "chl_oc3_mean", "chl_oc3_n_valid_px", "in_bounds"]]
    """
    try:
        import numpy as np
        import xarray as xr
        import rasterio.warp
    except ImportError as exc:
        raise ImportError(
            "extract_datacube_pixel_values requires xarray and rasterio.\n"
            f"Original error: {exc}"
        ) from exc

    datacube_path = Path(datacube_path)
    if not datacube_path.exists():
        raise FileNotFoundError(f"Datacube not found: {datacube_path}")

    if window_size < 1 or window_size % 2 == 0:
        raise ValueError(
            f"window_size must be a positive odd integer, got {window_size}."
        )
    if time_tolerance_days < 0:
        raise ValueError(
            f"time_tolerance_days must be >= 0, got {time_tolerance_days}."
        )
    for col in (lat_col, lon_col, date_col):
        if col not in stations.columns:
            raise ValueError(f"Column '{col}' not found in stations columns.")

    ds = xr.open_zarr(datacube_path, consolidated=False)
    try:
        data_vars = [
            v for v in ds.data_vars if {"time", "y", "x"} <= set(ds[v].dims)
        ]
        if variables is not None:
            data_vars = [v for v in variables if v in data_vars]
        if not data_vars:
            raise ValueError(f"No exportable variables found in {datacube_path.name}.")

        out = stations.copy()

        if out.empty:
            for var in data_vars:
                out[f"{var}_mean"] = pd.Series(dtype="float64")
                out[f"{var}_n_valid_px"] = pd.Series(dtype="int64")
            out["in_bounds"] = pd.Series(dtype="bool")
            out["matched_time_delta_days"] = pd.Series(dtype="float64")
            logger.info(
                "stations is empty — returning correctly-columned empty result."
            )
            return out

        n_rows = ds.sizes["y"]
        n_cols = ds.sizes["x"]
        x_coords = ds["x"].values
        y_coords = ds["y"].values
        minx, maxx = float(x_coords.min()), float(x_coords.max())
        miny, maxy = float(y_coords.min()), float(y_coords.max())

        cube_times = pd.DatetimeIndex(ds["time"].values).normalize()
        if len(cube_times) == 0:
            raise ValueError(f"Datacube at {datacube_path} has no time steps.")

        logger.info(
            f"Extracting {len(data_vars)} variable(s) for {len(out)} station(s) "
            f"from {datacube_path.name} (CRS={datacube_crs}, grid={n_rows}x{n_cols}, "
            f"{len(cube_times)} time step(s), window_size={window_size}, "
            f"time_tolerance_days={time_tolerance_days})."
        )

        # --- Spatial matching ---
        lons = out[lon_col].to_numpy(dtype="float64")
        lats = out[lat_col].to_numpy(dtype="float64")
        xs_native, ys_native = rasterio.warp.transform(
            "EPSG:4326", datacube_crs, lons.tolist(), lats.tolist()
        )
        xs_native = np.asarray(xs_native)
        ys_native = np.asarray(ys_native)

        spatial_in_bounds = (
            (xs_native >= minx)
            & (xs_native <= maxx)
            & (ys_native >= miny)
            & (ys_native <= maxy)
        )

        col_pos = pd.Index(x_coords).get_indexer(xs_native, method="nearest")
        row_pos = pd.Index(y_coords).get_indexer(ys_native, method="nearest")

        # --- Temporal matching: each row gets its own matched time index ---
        station_dates = pd.to_datetime(out[date_col], errors="coerce").dt.normalize()
        time_idx = np.full(len(out), -1, dtype="int64")
        time_delta_days = np.full(len(out), np.nan, dtype="float64")
        for i, sdate in enumerate(station_dates):
            if pd.isna(sdate):
                continue
            deltas = np.abs((cube_times - sdate).days.to_numpy())
            best = int(np.argmin(deltas))
            if deltas[best] <= time_tolerance_days:
                time_idx[i] = best
                time_delta_days[i] = float(deltas[best])

        in_bounds = spatial_in_bounds & (time_idx >= 0)
        n_no_time_match = int(((time_idx < 0)).sum())
        if n_no_time_match:
            logger.info(
                f"{n_no_time_match}/{len(out)} station(s) have no datacube date "
                f"within {time_tolerance_days} day(s) — will get NaN mean(s) and "
                "in_bounds=False."
            )

        half = window_size // 2

        for var in data_vars:
            means = np.full(len(out), np.nan, dtype="float64")
            n_valid = np.zeros(len(out), dtype="int64")

            matched_times = sorted(set(time_idx[in_bounds].tolist()))
            for t in matched_times:
                mask = in_bounds & (time_idx == t)
                values = ds[var].isel(time=t).transpose("y", "x").values
                sub_means, sub_valid = _windowed_nanmean(
                    values, row_pos[mask], col_pos[mask], half, n_rows, n_cols
                )
                means[mask] = sub_means
                n_valid[mask] = sub_valid

            out[f"{var}_mean"] = means
            out[f"{var}_n_valid_px"] = n_valid

        out["in_bounds"] = in_bounds
        out["matched_time_delta_days"] = time_delta_days

        return out
    finally:
        ds.close()


def plot_satellite_vs_insitu(
    campaigns_csv: Path | str,
    datacube_path: Path | str,
    output_figure: Path | str,
    satellite_var: str = "chl_oc3",
    insitu_parametro: str = "Clorofila_a_(lab)",
    window_size: int = 3,
    time_tolerance_days: int = 0,
    return_dataframe: bool = True,
    figure_dpi: int = 150,
    lat_col: str = "latitud",
    lon_col: str = "longitud",
    date_col: str = "date",
    value_col: str = "organized_value",
    parametro_col: str = "parametro",
    datacube_crs: str = "EPSG:4326",
) -> Optional[tuple[pd.DataFrame, dict]]:
    """
    Join satellite-extracted pixel values against in-situ measurements and
    produce a validation scatter plot with correlation statistics.

    Filters ``campaigns_csv`` (a long/tidy table with one row per measured
    parameter per sample, e.g. ``data/monitoring_data/campaigns_organized.csv``)
    to ``insitu_parametro``, extracts the matching satellite pixel value for
    each remaining row via :func:`extract_datacube_pixel_values`, and plots
    in-situ (x-axis, reference) against satellite (y-axis, retrieved) values
    with a 1:1 reference line.

    In-situ chlorophyll-a is conventionally reported in µg/L, which is
    numerically identical to ``chl_oc3``'s mg/m³ — no unit conversion is
    applied or needed for that default pairing.

    Parameters
    ----------
    campaigns_csv:
        Path to the long/tidy in-situ measurements CSV.
    datacube_path:
        Path to the L2W Zarr datacube (see :func:`extract_datacube_pixel_values`).
    output_figure:
        Destination path for the PNG figure. Parent directories are
        created automatically.
    satellite_var:
        Datacube variable to compare against. Defaults to ``"chl_oc3"``.
    insitu_parametro:
        Value of ``parametro_col`` to filter ``campaigns_csv`` to. Defaults
        to ``"Clorofila_a_(lab)"``.
    window_size, time_tolerance_days, datacube_crs:
        Passed through to :func:`extract_datacube_pixel_values`.
    return_dataframe:
        If ``True`` (default), return ``(matched_df, stats)``. If ``False``,
        return ``None``.
    figure_dpi:
        Resolution of the saved figure in dots per inch. Defaults to ``150``.
    lat_col, lon_col, date_col:
        Station latitude/longitude/date column names. Default to
        ``"latitud"``/``"longitud"``/``"date"``.
    value_col:
        In-situ measurement value column. Defaults to ``"organized_value"``.
    parametro_col:
        Column identifying which parameter each row measures. Defaults to
        ``"parametro"``.

    Returns
    -------
    tuple[pandas.DataFrame, dict] or None
        ``(matched_df, stats)`` when ``return_dataframe=True``, else
        ``None``. ``matched_df`` is the subset of extracted rows with a
        valid satellite matchup (``in_bounds`` and non-NaN
        ``{satellite_var}_mean``). ``stats`` has keys ``n``, ``r``, ``r2``,
        ``rmse``, ``bias`` — all ``NaN`` except ``n`` when there are zero
        matchups.

    Raises
    ------
    FileNotFoundError
        If ``campaigns_csv`` or ``datacube_path`` does not exist.
    ValueError
        If required columns are missing from ``campaigns_csv``, or no rows
        remain after filtering to ``insitu_parametro`` with a non-null
        ``value_col``.
    ImportError
        If matplotlib, xarray, or rasterio aren't installed.

    Examples
    --------
    >>> from aquamatch.utils import plot_satellite_vs_insitu
    >>> matched, stats = plot_satellite_vs_insitu(
    ...     campaigns_csv="data/monitoring_data/campaigns_organized.csv",
    ...     datacube_path="data/data_cube/l2w_datacube.zarr",
    ...     output_figure="reports/satellite_vs_insitu_chl.png",
    ... )
    >>> print(stats)
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise ImportError(
            "plot_satellite_vs_insitu requires matplotlib.\n"
            f"Original error: {exc}"
        ) from exc

    import numpy as np

    campaigns_csv = Path(campaigns_csv)
    datacube_path = Path(datacube_path)
    output_figure = Path(output_figure)

    if not campaigns_csv.exists():
        raise FileNotFoundError(f"Campaigns CSV not found: {campaigns_csv}")
    if not datacube_path.exists():
        raise FileNotFoundError(f"Datacube not found: {datacube_path}")

    df = pd.read_csv(campaigns_csv)
    required_cols = {parametro_col, value_col, lat_col, lon_col, date_col}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"Column(s) {sorted(missing_cols)} not found in {campaigns_csv}."
        )

    subset = (
        df[df[parametro_col] == insitu_parametro]
        .dropna(subset=[value_col])
        .reset_index(drop=True)
    )
    if subset.empty:
        raise ValueError(
            f"No rows found for {parametro_col}='{insitu_parametro}' with "
            f"non-null {value_col} in {campaigns_csv}."
        )

    logger.info(
        f"Matching {len(subset)} in-situ '{insitu_parametro}' measurement(s) "
        f"against satellite variable '{satellite_var}'."
    )

    extracted = extract_datacube_pixel_values(
        datacube_path,
        subset,
        variables=[satellite_var],
        window_size=window_size,
        lat_col=lat_col,
        lon_col=lon_col,
        date_col=date_col,
        time_tolerance_days=time_tolerance_days,
        datacube_crs=datacube_crs,
    )

    sat_col = f"{satellite_var}_mean"
    matched = extracted[
        extracted["in_bounds"] & extracted[sat_col].notna()
    ].reset_index(drop=True)

    n = len(matched)
    if n == 0:
        logger.warning(
            "No satellite/in-situ matchups found — check time_tolerance_days, "
            "station coordinates, and datacube coverage. Stats will be NaN "
            "and the figure will show no points."
        )
        r = r2 = rmse = bias = float("nan")
    else:
        insitu_vals = matched[value_col].to_numpy("float64")
        sat_vals = matched[sat_col].to_numpy("float64")

        if n >= 2:
            r = float(np.corrcoef(sat_vals, insitu_vals)[0, 1])
            r2 = r**2
        else:
            logger.warning(f"Only {n} matchup(s) — correlation undefined (need N>=2).")
            r = r2 = float("nan")

        rmse = float(np.sqrt(np.mean((sat_vals - insitu_vals) ** 2)))
        bias = float(np.mean(sat_vals - insitu_vals))

        if n < 5:
            logger.warning(
                f"Small sample size (N={n}) — correlation statistics may be "
                "unreliable."
            )

    stats = {"n": n, "r": r, "r2": r2, "rmse": rmse, "bias": bias}

    # --- Build figure ---
    fig, ax = plt.subplots(figsize=(6.5, 6.5))
    fig.suptitle(
        f"Satellite vs. In-Situ — {satellite_var} vs. {insitu_parametro}",
        fontsize=13,
        fontweight="bold",
        y=1.01,
    )

    if n > 0:
        insitu_vals = matched[value_col].to_numpy("float64")
        sat_vals = matched[sat_col].to_numpy("float64")

        ax.scatter(
            insitu_vals, sat_vals, color="#2196F3", alpha=0.75, edgecolors="none"
        )

        lo = float(min(insitu_vals.min(), sat_vals.min()))
        hi = float(max(insitu_vals.max(), sat_vals.max()))
        pad = (hi - lo) * 0.05 if hi > lo else 1.0
        lo, hi = lo - pad, hi + pad
        ax.plot([lo, hi], [lo, hi], color="#F44336", linestyle="--", linewidth=2, label="1:1")
        ax.set_xlim(lo, hi)
        ax.set_ylim(lo, hi)

        stats_text = (
            f"N = {n}\n"
            f"r = {r:.3f}\n"
            f"R² = {r2:.3f}\n"
            f"RMSE = {rmse:.3f}\n"
            f"bias = {bias:.3f}"
        )
        ax.text(
            0.05,
            0.95,
            stats_text,
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment="top",
            bbox={
                "boxstyle": "round",
                "facecolor": "white",
                "edgecolor": "#4CAF50",
                "alpha": 0.9,
            },
        )
        ax.legend(fontsize=9, loc="lower right")
    else:
        ax.text(
            0.5,
            0.5,
            "No matchups found",
            transform=ax.transAxes,
            fontsize=12,
            ha="center",
            va="center",
        )

    ax.set_xlabel(f"In-situ {insitu_parametro}", fontsize=10)
    ax.set_ylabel(f"Satellite {satellite_var}", fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.5)

    fig.tight_layout()

    output_figure.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_figure, dpi=figure_dpi, bbox_inches="tight")
    plt.close(fig)

    logger.info(
        f"Satellite-vs-in-situ figure saved: {output_figure} (N={n}, r={r})"
    )

    if return_dataframe:
        return matched, stats
    return None
