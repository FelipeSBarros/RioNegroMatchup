"""
utils.py
========
Utility functions for the aquamatch workflow.

Currently provides temporal tolerance analysis for Sentinel-2 catalog
opportunity cost quantification.
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
