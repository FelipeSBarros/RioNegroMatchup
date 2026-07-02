"""
pipeline_config.py
==================
YAML-driven pipeline configuration for the aquamatch workflow.

A single YAML file drives the entire pipeline — one file per campaign,
version-controlled, self-documenting.

Usage
-----
Generate a template::

    python -m aquamatch.pipeline_config --generate campaign_2025.yaml

Run the full pipeline::

    python -m aquamatch.pipeline_config --run campaign_2025.yaml

Programmatic usage::

    cfg = PipelineConfig.from_yaml("campaign_2025.yaml")
    cfg.run()
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field, fields, asdict
from pathlib import Path
from typing import Any, Optional

from aquamatch.scl_water import build_water_polygon_datacube
from aquamatch.acolite_spec import append_l2w_to_datacube

logger = logging.getLogger(__name__)

# Valid values for DownloadSection.strategy — validated at load time.
VALID_DOWNLOAD_STRATEGIES = {"best", "all", "same_day", "previous", "posterior"}

# ---------------------------------------------------------------------------
# Section dataclasses
# ---------------------------------------------------------------------------


@dataclass
class InsituSection:
    enabled: bool = True
    stations_path: str = "data/original_data/estaciones-seleccionadas.xlsx"
    campaigns_path: str = "data/original_data/campaigns_sample.xlsx"
    output_campaigns_csv: str = "data/monitoring_data/campaigns_organized.csv"
    output_unique_csv: str = "data/monitoring_data/campaigns_unique_data.csv"
    skip_clean: bool = False


@dataclass
class SentinelSection:
    enabled: bool = True
    catalog_json: str = "data/sentinel_downloads/sentinel_catalog.json"
    csv: str = "data/monitoring_data/campaigns_unique_data.csv"
    time_delta: int = 1
    cloud_cover: int = 10


@dataclass
class DownloadSection:
    enabled: bool = True
    output_dir: str = "data/sentinel_downloads"
    catalog_json: str = "data/sentinel_downloads/sentinel_catalog.json"
    strategy: str = "best"
    max_per_date: int = 1
    max_cloud_cover: Optional[int] = None
    download_scl: bool = True


@dataclass
class SclSection:
    use_scl: bool = True
    min_area_m2: float = 5_000.0
    simplify_tolerance: float = 20.0
    buffer_m: float = 0.0
    # --- Water polygon datacube ---
    build_polygon_datacube: bool = False
    polygon_datacube_path: str = "data/water_polygons.gpkg"
    polygon_datacube_overwrite: bool = False


@dataclass
class AcoliteIOSection:
    output: str = "data/acolite_output"
    safe_dir: str = "data/sentinel_downloads"
    scl_dir: str = "data/sentinel_downloads/scl"
    limit: Optional[list[float]] = None  # [S, W, N, E]


@dataclass
class AcoliteRadCorSection:
    aerosol_correction: str = "dsf"
    dsf_path_reflectance: str = "tiled"
    dsf_tile_dimensions: list[int] = field(default_factory=lambda: [120, 120])
    dsf_minimum_tile_cover: float = 0.10
    ancillary_data: bool = True
    uoz: float = 0.3
    uwv: float = 1.5
    pressure: float = 1013.25


@dataclass
class AcoliteGlintSection:
    glint_correction: bool = True
    glint_method: str = "vanhellemont2019"
    glint_threshold: float = 0.01
    glint_mask_rhos: bool = True
    glint_mask_rhos_threshold: float = 0.15


@dataclass
class AcoliteL2WSection:
    l2w_parameters: list[str] = field(
        default_factory=lambda: [
            "t_nechad",
            "spm_nechad",
            "chl_oc3",
            "chl_re",
            "aphy_443",
            "fai",
            "ndwi",
            "ndvi",
        ]
    )
    l2w_mask: bool = True
    l2w_mask_negative_rhos: bool = True
    l2w_mask_cirrus: bool = True
    l2w_mask_high_toa: bool = True
    l2w_mask_high_toa_threshold: float = 0.3
    l2w_mask_water_expr: Optional[str] = "rhos_1600 < 0.0215"
    output_rhorc: bool = False
    output_rhos: bool = True
    # Extended masking detail (new)
    l2w_mask_wave: int = 1600
    l2w_mask_threshold: float = 0.0215
    l2w_mask_cirrus_threshold: float = 0.005
    l2w_mask_smooth: bool = True
    l2w_mask_smooth_sigma: int = 3


@dataclass
class AcoliteOutputSection:
    export_geotiff: bool = True
    export_geotiff_coordinates: bool = True
    export_cloud_optimized_geotiff: bool = False
    netcdf_compression: bool = True
    netcdf_compression_level: int = 4
    map_rgb: bool = False
    map_rgb_maxrange: float = 0.15
    # Extended output content (new)
    output_xy: bool = False
    output_geometry: bool = True
    l2w_export_geotiff: bool = False
    copy_datasets: str = "lon,lat,rhot_*"


# ---------------------------------------------------------------------------
# New pipeline config sections (Step B)
# ---------------------------------------------------------------------------


@dataclass
class AcoliteS2Section:
    """
    Sentinel-2 specific processing options.

    Controls target resolution, tile merging, per-pixel geometry, and
    blackfill handling.  All defaults match ACOLITE's own defaults for S2.
    """

    s2_target_res: int = 10
    """Target output resolution in metres.  One of 10, 20, or 60."""

    merge_tiles: bool = False
    """Merge adjacent SAFE tiles before processing."""

    merge_full_tiles: bool = False
    """Merge full (non-cropped) tiles."""

    extend_region: bool = False
    """Extend the cropped region to cover the full tile extent."""

    geometry_type: str = "grids_footprint"
    """Per-pixel geometry computation method."""

    geometry_res: int = 60
    """Resolution (m) at which geometry grids are computed."""

    blackfill_skip: bool = True
    """Skip scenes where the blackfill fraction exceeds ``blackfill_max``."""

    blackfill_max: float = 1.0
    """Maximum allowed blackfill fraction (0–1)."""

    blackfill_wave: int = 1600
    """Band (nm) used to detect blackfill pixels."""


@dataclass
class AcoliteDsfSection:
    """
    Dark Spectrum Fitting (DSF) fine-tuning.

    Most users should leave these at their defaults.  Adjust when
    processing very turbid water, small lakes, or scenes with bright
    non-water targets that confuse the standard dark-pixel retrieval.
    """

    dsf_aot_estimate: str = "tiled"
    """AOT estimation mode.  One of ``tiled``, ``fixed``, ``fixed_band``."""

    dsf_spectrum_option: str = "intercept"
    """Dark-spectrum selection.  One of ``intercept``, ``darkest``, ``percentile``."""

    dsf_nbands: int = 2
    """Number of bands used for AOT retrieval."""

    dsf_nbands_fit: int = 2
    """Number of bands used for the spectral fit."""

    dsf_filter_rhot: bool = False
    """Apply a percentile filter to TOA reflectance before DSF."""

    dsf_filter_percentile: int = 50
    """Percentile used when ``dsf_filter_rhot=True``."""

    dsf_smooth_aot: bool = False
    """Spatially smooth the retrieved AOT field."""

    dsf_fixed_aot: Optional[float] = None
    """Override AOT retrieval with a fixed value.  ``None`` = retrieve."""

    dsf_aot_most_common_model: bool = True
    """Use the most-common atmospheric model across tiles."""

    dsf_allow_lut_boundaries: bool = False
    """Allow AOT to reach the edges of the LUT."""

    dsf_min_tile_aot: float = 0.01
    """Minimum acceptable tile AOT."""

    dsf_max_tile_aot: float = 1.20
    """Maximum acceptable tile AOT."""


@dataclass
class AcoliteReprojectSection:
    """
    Optional output reprojection.

    Disabled by default.  Enable to reproject all ACOLITE outputs to a
    common projected CRS — useful for direct comparison with in situ
    station coordinates and for consistent spatial analysis.
    """

    reproject_outputs: bool = False
    """Set ``True`` to enable reprojection of all output files."""

    output_projection_epsg: Optional[int] = None
    """Target EPSG code (e.g. 32721 for UTM zone 21S)."""

    output_projection_resolution: Optional[float] = None
    """Target pixel size in metres.  ``None`` = native sensor resolution."""

    output_projection_resampling_method: str = "bilinear"
    """Resampling algorithm.  One of ``nearest``, ``bilinear``, ``cubic``."""


@dataclass
class DatacubeSection:
    """
    L2W product datacube configuration.

    Controls the aggregation of per-scene ACOLITE L2W NetCDF files into a
    single Zarr datacube using :func:`~aquamatch.acolite_spec.append_l2w_to_datacube`.

    Disabled by default (``enabled: false``) because it requires the optional
    ``xarray``, ``rioxarray``, and ``zarr`` dependencies and can produce large
    output files.  Enable explicitly once ACOLITE processing is complete.

    ``variables`` controls which L2W parameters are written to the datacube.
    Set to ``null`` to include every variable present in each L2W file, or
    list specific parameter names (e.g. ``[t_nechad, spm_nechad, ndwi]``) to
    restrict the datacube to a subset.  A warning is emitted for any listed
    variable that is absent from a particular L2W file — the file is still
    processed for the variables that are present.
    """

    enabled: bool = False
    output_path: str = "data/l2w_datacube.zarr"
    variables: Optional[list[str]] = None  # None = all variables in each L2W file
    target_crs: str = "EPSG:4326"
    target_resolution: float = 0.0001  # degrees (~10 m at equator)
    overwrite_date: bool = False
    zarr_chunks: dict = field(default_factory=lambda: {"time": 1, "y": 512, "x": 512})


@dataclass
class AcoliteSection:
    enabled: bool = True
    acolite_executable: str = "/path/to/acolite/acolite.py"
    low_memory: bool = False
    continue_on_error: bool = True
    skip_existing: bool = True
    io: AcoliteIOSection = field(default_factory=AcoliteIOSection)
    radcor: AcoliteRadCorSection = field(default_factory=AcoliteRadCorSection)
    glint: AcoliteGlintSection = field(default_factory=AcoliteGlintSection)
    l2w: AcoliteL2WSection = field(default_factory=AcoliteL2WSection)
    output_format: AcoliteOutputSection = field(default_factory=AcoliteOutputSection)
    scl: SclSection = field(default_factory=SclSection)
    # New sub-sections
    s2: AcoliteS2Section = field(default_factory=AcoliteS2Section)
    dsf: AcoliteDsfSection = field(default_factory=AcoliteDsfSection)
    reproject: AcoliteReprojectSection = field(default_factory=AcoliteReprojectSection)
    datacube: DatacubeSection = field(default_factory=DatacubeSection)


# ---------------------------------------------------------------------------
# Tile configuration
# ---------------------------------------------------------------------------


@dataclass
class TileEntry:
    """
    Spatial restriction for a single Sentinel-2 MGRS tile.

    Exactly one of ``polygon`` or ``limit`` may be set.
    """

    polygon: Optional[str] = None
    limit: Optional[list[float]] = None

    def validate(self, tile_id: str = "") -> None:
        context = f" (tile {tile_id!r})" if tile_id else ""

        if self.polygon is not None and self.limit is not None:
            raise ValueError(
                f"TileEntry{context}: specify either 'polygon' or 'limit', not both."
            )

        if self.limit is not None:
            if len(self.limit) != 4:
                raise ValueError(
                    f"TileEntry{context}: 'limit' must contain exactly 4 values "
                    f"[south, west, north, east], got {len(self.limit)}."
                )
            s, w, n, e = self.limit
            if s >= n:
                raise ValueError(
                    f"TileEntry{context}: limit south ({s}) must be < north ({n})."
                )
            if w >= e:
                raise ValueError(
                    f"TileEntry{context}: limit west ({w}) must be < east ({e})."
                )
            if not (-90 <= s <= 90 and -90 <= n <= 90):
                raise ValueError(
                    f"TileEntry{context}: latitude values must be in [-90, 90]."
                )
            if not (-180 <= w <= 180 and -180 <= e <= 180):
                raise ValueError(
                    f"TileEntry{context}: longitude values must be in [-180, 180]."
                )


@dataclass
class TilesSection:
    """Per-tile spatial restrictions, keyed by 5-character MGRS tile code."""

    entries: dict[str, TileEntry] = field(default_factory=dict)

    def get(self, tile_id: str) -> Optional[TileEntry]:
        return self.entries.get(tile_id)

    def validate(self) -> None:
        for tile_id, entry in self.entries.items():
            entry.validate(tile_id=tile_id)

    @classmethod
    def from_dict(cls, raw: dict) -> "TilesSection":
        entries: dict[str, TileEntry] = {}
        known_tile_keys = {f.name for f in fields(TileEntry)}

        for tile_id, tile_raw in raw.items():
            if tile_raw is None:
                entries[tile_id] = TileEntry()
                continue

            unknown = set(tile_raw.keys()) - known_tile_keys
            if unknown:
                raise ValueError(
                    f"Unknown key(s) in tiles.{tile_id}: {sorted(unknown)}. "
                    f"Valid keys: {sorted(known_tile_keys)}"
                )

            entry = TileEntry(
                polygon=tile_raw.get("polygon"),
                limit=tile_raw.get("limit"),
            )
            entry.validate(tile_id=tile_id)
            entries[tile_id] = entry

        return cls(entries=entries)


# ---------------------------------------------------------------------------
# Master config
# ---------------------------------------------------------------------------


@dataclass
class PipelineConfig:
    """Master pipeline configuration for aquamatch."""

    campaign_name: str = "rio_negro_2025"
    description: str = "Sentinel-2 / in situ water quality matchup"

    insitu: InsituSection = field(default_factory=InsituSection)
    sentinel: SentinelSection = field(default_factory=SentinelSection)
    download: DownloadSection = field(default_factory=DownloadSection)
    acolite: AcoliteSection = field(default_factory=AcoliteSection)
    tiles: TilesSection = field(default_factory=TilesSection)

    # ---------------------------------------------------------------------------
    # Template generation
    # ---------------------------------------------------------------------------

    @classmethod
    def generate(cls, output_path: Path | str) -> Path:
        """Write a fully-commented YAML template to *output_path*."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        template = """\
# =============================================================================
# aquamatch — Pipeline Configuration
# =============================================================================
# One YAML file drives the entire pipeline: in situ data preparation,
# satellite catalog building, product download, and atmospheric correction.
#
# Usage:
#   Generate this template:
#     python -m aquamatch.pipeline_config --generate campaign_2025.yaml
#   Run the full pipeline:
#     python -m aquamatch.pipeline_config --run campaign_2025.yaml
# =============================================================================

campaign_name: rio_negro_2025       # Identifier for this campaign run
description: "Sentinel-2 / in situ water quality matchup"

# =============================================================================
# Step 1 — In situ data preparation
# =============================================================================
insitu:
  enabled: true

  stations_path: data/original_data/estaciones-seleccionadas.xlsx
  campaigns_path: data/original_data/campaigns_sample.xlsx

  output_campaigns_csv: data/monitoring_data/campaigns_organized.csv
  output_unique_csv: data/monitoring_data/campaigns_unique_data.csv

  skip_clean: false

# =============================================================================
# Step 2 — Sentinel-2 catalog search
# =============================================================================
sentinel:
  enabled: true

  catalog_json: data/sentinel_downloads/sentinel_catalog.json
  csv: data/monitoring_data/campaigns_unique_data.csv

  time_delta: 1
  cloud_cover: 10

# =============================================================================
# Step 3 — Sentinel-2 image download
# =============================================================================
download:
  enabled: true

  output_dir: data/sentinel_downloads
  catalog_json: data/sentinel_downloads/sentinel_catalog.json

  # best | same_day | previous | posterior | all
  strategy: best
  max_per_date: 1
  max_cloud_cover: null
  download_scl: true

# =============================================================================
# Step 4 — Atmospheric correction (ACOLITE)
# =============================================================================
acolite:
  enabled: true

  acolite_executable: /path/to/acolite/acolite.py

  low_memory: false
  continue_on_error: true
  skip_existing: true

  # --- Input / Output ---
  io:
    output: data/acolite_output
    safe_dir: data/sentinel_downloads
    scl_dir: data/sentinel_downloads/scl

  # --- Radiometric correction ---
  radcor:
    aerosol_correction: dsf         # dsf | exp | tact
    dsf_path_reflectance: tiled     # tiled | fixed | percentile
    dsf_tile_dimensions: [120, 120]
    dsf_minimum_tile_cover: 0.10
    ancillary_data: true
    uoz: 0.3
    uwv: 1.5
    pressure: 1013.25

  # --- Sunglint correction ---
  glint:
    glint_correction: true
    glint_method: vanhellemont2019  # none | hedley | vanhellemont2019
    glint_threshold: 0.01
    glint_mask_rhos: true
    glint_mask_rhos_threshold: 0.15

  # --- L2W water quality products ---
  l2w:
    l2w_parameters:
      - t_nechad
      - spm_nechad
      - chl_oc3
      - chl_re
      - aphy_443
      - fai
      - ndwi
      - ndvi

    l2w_mask: true
    l2w_mask_negative_rhos: true
    l2w_mask_cirrus: true
    l2w_mask_high_toa: true
    l2w_mask_high_toa_threshold: 0.3
    l2w_mask_water_expr: "rhos_1600 < 0.0215"
    output_rhorc: false
    output_rhos: true

    # Extended masking — adjust to tune mask sensitivity
    l2w_mask_wave: 1600              # Band (nm) used as the water mask reference
    l2w_mask_threshold: 0.0215       # Reflectance threshold for the water mask
    l2w_mask_cirrus_threshold: 0.005 # Cirrus detection threshold
    l2w_mask_smooth: true            # Smooth mask to reduce speckle
    l2w_mask_smooth_sigma: 3         # Gaussian sigma for mask smoothing (pixels)

  # --- Output formats ---
  output_format:
    export_geotiff: true
    export_geotiff_coordinates: true
    export_cloud_optimized_geotiff: false
    netcdf_compression: true
    netcdf_compression_level: 4     # 1 (fast) – 9 (small)
    map_rgb: false
    map_rgb_maxrange: 0.15

    # Extended output content
    output_xy: false                 # Include x/y pixel coordinate arrays
    output_geometry: true            # Include sun/view geometry bands
    l2w_export_geotiff: false        # Also export L2W products as GeoTIFF
    copy_datasets: "lon,lat,rhot_*"  # Datasets copied from L1R to L2R

  # --- SCL-based water polygon clipping ---
  scl:
    use_scl: true
    min_area_m2: 5000.0
    simplify_tolerance: 20.0
    buffer_m: 0.0

    # --- Water polygon datacube ---
    # When build_polygon_datacube is true, all SCL files in scl_dir are
    # processed after ACOLITE runs and aggregated into a single GeoPackage.
    # The same min_area_m2, simplify_tolerance, and buffer_m values above
    # are reused — no duplication needed.
    build_polygon_datacube: false
    polygon_datacube_path: data/water_polygons.gpkg
    polygon_datacube_overwrite: false   # true = rebuild from scratch each run

  # ---------------------------------------------------------------------------
  # Sentinel-2 specific options
  # ---------------------------------------------------------------------------
  s2:
    # Target output resolution in metres. One of: 10, 20, 60.
    # Use 20 on low-memory machines to halve data volume.
    s2_target_res: 10

    # Merge adjacent SAFE tiles before processing.
    # Useful for water bodies that span a tile boundary.
    merge_tiles: false
    merge_full_tiles: false

    # Extend the ROI to cover the full tile (not just the cropped limit).
    extend_region: false

    # Per-pixel geometry grids
    geometry_type: grids_footprint
    geometry_res: 60                 # Resolution (m) for geometry computation

    # Blackfill handling — skip scenes that are mostly empty (no data).
    blackfill_skip: true
    blackfill_max: 1.0               # Max blackfill fraction (0–1); 1.0 = never skip
    blackfill_wave: 1600             # Band (nm) used to detect blackfill pixels

  # ---------------------------------------------------------------------------
  # DSF fine-tuning (advanced)
  # ---------------------------------------------------------------------------
  # Most users should leave these at their defaults.
  # Adjust for very turbid water, small lakes, or scenes with bright
  # non-water targets that confuse the standard dark-pixel retrieval.
  dsf:
    dsf_aot_estimate: tiled          # tiled | fixed | fixed_band
    dsf_spectrum_option: intercept   # intercept | darkest | percentile
    dsf_nbands: 2                    # Number of bands for AOT retrieval
    dsf_nbands_fit: 2                # Number of bands for spectral fit
    dsf_filter_rhot: false           # Apply percentile filter to TOA before DSF
    dsf_filter_percentile: 50        # Percentile used when dsf_filter_rhot=true
    dsf_smooth_aot: false            # Spatially smooth the retrieved AOT field
    dsf_fixed_aot: null              # Set a float (e.g. 0.1) to bypass AOT retrieval
    dsf_aot_most_common_model: true
    dsf_allow_lut_boundaries: false
    dsf_min_tile_aot: 0.01
    dsf_max_tile_aot: 1.20

  # ---------------------------------------------------------------------------
  # Output reprojection (optional)
  # ---------------------------------------------------------------------------
  # Disabled by default. Enable to reproject all outputs (L1R, L2R, L2W)
  # to a common projected CRS, useful for direct comparison with in situ
  # station coordinates and for consistent downstream spatial analysis.
  reproject:
    reproject_outputs: false
    output_projection_epsg: null     # e.g. 32721 for UTM zone 21S (Uruguay/NE Argentina)
    output_projection_resolution: null  # Target pixel size in metres (null = native)
    output_projection_resampling_method: bilinear  # nearest | bilinear | cubic

  # ---------------------------------------------------------------------------
  # L2W product datacube (Step 5)
  # ---------------------------------------------------------------------------
  # Aggregates all *_L2W.nc files produced by ACOLITE into a single Zarr
  # datacube using append_l2w_to_datacube().
  #
  # Disabled by default (enabled: false) because it requires xarray,
  # rioxarray, and zarr.  Enable once ACOLITE processing is complete.
  #
  # variables: which L2W parameters to include.  null = all variables
  # present in each file.  List specific names to restrict the datacube,
  # e.g. [t_nechad, spm_nechad, ndwi].  A warning is emitted for any
  # listed variable absent from a particular file; processing continues
  # for variables that are present.
  datacube:
    enabled: false
    output_path: data/l2w_datacube.zarr
    variables: null                  # null = all | e.g. [t_nechad, spm_nechad, ndwi]
    target_crs: "EPSG:4326"
    target_resolution: 0.0001        # degrees (~10 m at equator)
    overwrite_date: false            # true = replace existing dates in datacube
    zarr_chunks:
      time: 1
      y: 512
      x: 512

# =============================================================================
# Tile spatial restrictions
# =============================================================================
# Per-tile bounding boxes or polygon paths, keyed by 5-character MGRS tile
# code.  For each tile, specify either 'polygon' OR 'limit' (not both), or
# omit the tile entirely to process the full scene.
#
# polygon: path to a GeoJSON or WKT file defining the region of interest.
#          Takes precedence over 'limit' at runtime.
# limit:   bounding box as [south, west, north, east] in decimal degrees.
#
# Example:
#   21HUD:
#     polygon: data/polygons/21HUD.geojson
#   21HVD:
#     limit: [-34.2, -56.8, -33.0, -55.1]
#   21HWD:
#     # no entry — full scene processed
# =============================================================================
tiles: {}
"""
        output_path.write_text(template)
        logger.info(f"Pipeline template written to {output_path}")
        return output_path

    # ---------------------------------------------------------------------------
    # Parsing
    # ---------------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: Path | str) -> "PipelineConfig":
        """
        Load, parse, and validate a pipeline YAML file.

        Unknown top-level or section keys raise ``ValueError``.
        Missing keys fall back to dataclass defaults.
        """
        try:
            import yaml
        except ImportError as exc:
            raise ImportError(
                "PipelineConfig.from_yaml requires PyYAML. "
                "Install it with: pip install pyyaml"
            ) from exc

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Pipeline config not found: {path}")

        with path.open() as f:
            raw: dict = yaml.safe_load(f) or {}

        return cls._from_dict(raw)

    @classmethod
    def _from_dict(cls, raw: dict) -> "PipelineConfig":
        """Parse a raw dict into a PipelineConfig, raising on unknown keys."""
        _check_keys(raw, cls, context="pipeline root")

        insitu_raw = raw.get("insitu", {})
        sentinel_raw = raw.get("sentinel", {})
        download_raw = raw.get("download", {})
        acolite_raw = raw.get("acolite", {})
        tiles_raw = raw.get("tiles", {})

        _check_keys(insitu_raw, InsituSection, context="insitu")
        _check_keys(sentinel_raw, SentinelSection, context="sentinel")
        _check_keys(download_raw, DownloadSection, context="download")

        # Separate top-level acolite keys from sub-section keys
        _ACOLITE_SUBSECTIONS = {
            "io",
            "radcor",
            "glint",
            "l2w",
            "output_format",
            "scl",
            "s2",
            "dsf",
            "reproject",
            "datacube",
        }
        acolite_sub = {
            k: v for k, v in acolite_raw.items() if k not in _ACOLITE_SUBSECTIONS
        }
        _check_keys(acolite_sub, AcoliteSection, context="acolite")

        io_raw = acolite_raw.get("io", {})
        radcor_raw = acolite_raw.get("radcor", {})
        glint_raw = acolite_raw.get("glint", {})
        l2w_raw = acolite_raw.get("l2w", {})
        output_raw = acolite_raw.get("output_format", {})
        scl_raw = acolite_raw.get("scl", {})
        s2_raw = acolite_raw.get("s2", {})
        dsf_raw = acolite_raw.get("dsf", {})
        reproject_raw = acolite_raw.get("reproject", {})
        datacube_raw = acolite_raw.get("datacube", {})

        _check_keys(io_raw, AcoliteIOSection, context="acolite.io")
        _check_keys(radcor_raw, AcoliteRadCorSection, context="acolite.radcor")
        _check_keys(glint_raw, AcoliteGlintSection, context="acolite.glint")
        _check_keys(l2w_raw, AcoliteL2WSection, context="acolite.l2w")
        _check_keys(output_raw, AcoliteOutputSection, context="acolite.output_format")
        _check_keys(scl_raw, SclSection, context="acolite.scl")
        _check_keys(s2_raw, AcoliteS2Section, context="acolite.s2")
        _check_keys(dsf_raw, AcoliteDsfSection, context="acolite.dsf")
        _check_keys(reproject_raw, AcoliteReprojectSection, context="acolite.reproject")
        _check_keys(datacube_raw, DatacubeSection, context="acolite.datacube")

        # --- Validate download strategy ---
        if "strategy" in download_raw:
            strategy = download_raw["strategy"]
            if strategy not in VALID_DOWNLOAD_STRATEGIES:
                raise ValueError(
                    f"Unknown download strategy '{strategy}' in [download]. "
                    f"Valid values: {sorted(VALID_DOWNLOAD_STRATEGIES)}"
                )

        # Handle dsf_tile_dimensions: YAML list → list
        if "dsf_tile_dimensions" in radcor_raw:
            val = radcor_raw["dsf_tile_dimensions"]
            radcor_raw = {**radcor_raw, "dsf_tile_dimensions": list(val)}

        acolite = AcoliteSection(
            **{k: v for k, v in acolite_sub.items()},
            io=AcoliteIOSection(**io_raw),
            radcor=AcoliteRadCorSection(**radcor_raw),
            glint=AcoliteGlintSection(**glint_raw),
            l2w=AcoliteL2WSection(**l2w_raw),
            output_format=AcoliteOutputSection(**output_raw),
            scl=SclSection(**scl_raw),
            s2=AcoliteS2Section(**s2_raw),
            dsf=AcoliteDsfSection(**dsf_raw),
            reproject=AcoliteReprojectSection(**reproject_raw),
            datacube=DatacubeSection(**datacube_raw),
        )

        tiles = TilesSection.from_dict(tiles_raw or {})

        return cls(
            campaign_name=raw.get("campaign_name", "rio_negro_2025"),
            description=raw.get("description", ""),
            insitu=InsituSection(**insitu_raw),
            sentinel=SentinelSection(**sentinel_raw),
            download=DownloadSection(**download_raw),
            acolite=acolite,
            tiles=tiles,
        )

    # ---------------------------------------------------------------------------
    # Converters
    # ---------------------------------------------------------------------------

    def to_acolite_config(self):
        """Convert the acolite section to an ``AcoliteConfig`` instance."""
        from aquamatch.acolite_spec import (
            AcoliteConfig,
            IOConfig,
            RadCorConfig,
            GlintConfig,
            L2WConfig,
            OutputConfig,
            S2Config,
            DsfConfig,
            ReprojectConfig,
            AcoliteAtmosphericProcessor,
            AcoliteGlintCorrection,
        )

        a = self.acolite

        limit = tuple(a.io.limit) if a.io.limit is not None else None

        io = IOConfig(
            inputfile="",
            output=a.io.output,
            limit=limit,
        )

        radcor = RadCorConfig(
            aerosol_correction=AcoliteAtmosphericProcessor(a.radcor.aerosol_correction),
            dsf_path_reflectance=a.radcor.dsf_path_reflectance,
            dsf_tile_dimensions=tuple(a.radcor.dsf_tile_dimensions),
            dsf_minimum_tile_cover=a.radcor.dsf_minimum_tile_cover,
            ancillary_data=a.radcor.ancillary_data,
            uoz=a.radcor.uoz,
            uwv=a.radcor.uwv,
            pressure=a.radcor.pressure,
        )

        glint = GlintConfig(
            glint_correction=a.glint.glint_correction,
            glint_method=AcoliteGlintCorrection(a.glint.glint_method),
            glint_threshold=a.glint.glint_threshold,
            glint_mask_rhos=a.glint.glint_mask_rhos,
            glint_mask_rhos_threshold=a.glint.glint_mask_rhos_threshold,
        )

        l2w = L2WConfig(
            l2w_parameters=list(a.l2w.l2w_parameters),
            l2w_mask=a.l2w.l2w_mask,
            l2w_mask_negative_rhos=a.l2w.l2w_mask_negative_rhos,
            l2w_mask_cirrus=a.l2w.l2w_mask_cirrus,
            l2w_mask_high_toa=a.l2w.l2w_mask_high_toa,
            l2w_mask_high_toa_threshold=a.l2w.l2w_mask_high_toa_threshold,
            l2w_mask_water_expr=a.l2w.l2w_mask_water_expr,
            output_rhorc=a.l2w.output_rhorc,
            output_rhos=a.l2w.output_rhos,
            l2w_mask_wave=a.l2w.l2w_mask_wave,
            l2w_mask_threshold=a.l2w.l2w_mask_threshold,
            l2w_mask_cirrus_threshold=a.l2w.l2w_mask_cirrus_threshold,
            l2w_mask_smooth=a.l2w.l2w_mask_smooth,
            l2w_mask_smooth_sigma=a.l2w.l2w_mask_smooth_sigma,
        )

        output_format = OutputConfig(
            export_geotiff=a.output_format.export_geotiff,
            export_geotiff_coordinates=a.output_format.export_geotiff_coordinates,
            export_cloud_optimized_geotiff=a.output_format.export_cloud_optimized_geotiff,
            netcdf_compression=a.output_format.netcdf_compression,
            netcdf_compression_level=a.output_format.netcdf_compression_level,
            map_rgb=a.output_format.map_rgb,
            map_rgb_maxrange=a.output_format.map_rgb_maxrange,
            output_xy=a.output_format.output_xy,
            output_geometry=a.output_format.output_geometry,
            l2w_export_geotiff=a.output_format.l2w_export_geotiff,
            copy_datasets=a.output_format.copy_datasets,
        )

        s2 = S2Config(
            s2_target_res=a.s2.s2_target_res,
            merge_tiles=a.s2.merge_tiles,
            merge_full_tiles=a.s2.merge_full_tiles,
            extend_region=a.s2.extend_region,
            geometry_type=a.s2.geometry_type,
            geometry_res=a.s2.geometry_res,
            blackfill_skip=a.s2.blackfill_skip,
            blackfill_max=a.s2.blackfill_max,
            blackfill_wave=a.s2.blackfill_wave,
        )

        dsf = DsfConfig(
            dsf_aot_estimate=a.dsf.dsf_aot_estimate,
            dsf_spectrum_option=a.dsf.dsf_spectrum_option,
            dsf_nbands=a.dsf.dsf_nbands,
            dsf_nbands_fit=a.dsf.dsf_nbands_fit,
            dsf_filter_rhot=a.dsf.dsf_filter_rhot,
            dsf_filter_percentile=a.dsf.dsf_filter_percentile,
            dsf_smooth_aot=a.dsf.dsf_smooth_aot,
            dsf_fixed_aot=a.dsf.dsf_fixed_aot,
            dsf_aot_most_common_model=a.dsf.dsf_aot_most_common_model,
            dsf_allow_lut_boundaries=a.dsf.dsf_allow_lut_boundaries,
            dsf_min_tile_aot=a.dsf.dsf_min_tile_aot,
            dsf_max_tile_aot=a.dsf.dsf_max_tile_aot,
        )

        reproject = ReprojectConfig(
            reproject_outputs=a.reproject.reproject_outputs,
            output_projection_epsg=a.reproject.output_projection_epsg,
            output_projection_resolution=a.reproject.output_projection_resolution,
            output_projection_resampling_method=(
                a.reproject.output_projection_resampling_method
            ),
        )

        cfg = AcoliteConfig(
            acolite_executable=a.acolite_executable,
            io=io,
            radcor=radcor,
            glint=glint,
            l2w=l2w,
            output_format=output_format,
            s2=s2,
            dsf=dsf,
            reproject=reproject,
        )
        return cfg

    def to_tile_config(self) -> TilesSection:
        """Return the ``TilesSection`` for use in ``run_batch``."""
        return self.tiles

    def to_scl_kwargs(self) -> dict:
        """Return keyword arguments for SCL water extraction functions."""
        s = self.acolite.scl
        return {
            "min_area_m2": s.min_area_m2,
            "simplify_tolerance": s.simplify_tolerance,
            "buffer_m": s.buffer_m,
        }

    def to_insitu_args(self) -> dict:
        """Return arguments for the in situ data pipeline step."""
        s = self.insitu
        return {
            "stations_path": Path(s.stations_path),
            "campaigns_path": Path(s.campaigns_path),
            "output_campaigns_csv": Path(s.output_campaigns_csv),
            "output_unique_csv": Path(s.output_unique_csv),
            "skip_clean": s.skip_clean,
        }

    def to_sentinel_args(self) -> dict:
        """Return arguments for the Sentinel-2 catalog and download steps."""
        return {
            "csv": Path(self.sentinel.csv),
            "catalog_json": Path(self.sentinel.catalog_json),
            "time_delta": self.sentinel.time_delta,
            "cloud_cover": self.sentinel.cloud_cover,
            "output_dir": Path(self.download.output_dir),
            "strategy": self.download.strategy,
            "max_per_date": self.download.max_per_date,
            "max_cloud_cover": self.download.max_cloud_cover,
            "download_scl": self.download.download_scl,
        }

    # ---------------------------------------------------------------------------
    # Orchestration
    # ---------------------------------------------------------------------------

    def run(self, dry_run: bool = False) -> dict:
        """Execute the full pipeline in order, respecting ``enabled`` flags."""
        summary: dict[str, Any] = {}
        logger.info(
            f"=== Pipeline: {self.campaign_name} ==="
            + (" [DRY RUN]" if dry_run else "")
        )

        if not self.insitu.enabled:
            logger.info("[Step 1/5] in situ — SKIPPED (enabled: false)")
            summary["insitu"] = {"status": "skipped"}
        else:
            logger.info("[Step 1/5] In situ data preparation")
            if dry_run:
                summary["insitu"] = {"status": "dry_run"}
            else:
                try:
                    summary["insitu"] = self._run_insitu()
                except Exception as exc:
                    logger.error(f"  In situ step failed: {exc}")
                    summary["insitu"] = {"status": "error", "error": str(exc)}

        if not self.sentinel.enabled:
            logger.info("[Step 2/5] Sentinel catalog — SKIPPED (enabled: false)")
            summary["sentinel"] = {"status": "skipped"}
        else:
            logger.info("[Step 2/5] Sentinel-2 catalog search")
            if dry_run:
                summary["sentinel"] = {"status": "dry_run"}
            else:
                try:
                    summary["sentinel"] = self._run_sentinel_catalog()
                except Exception as exc:
                    logger.error(f"  Sentinel catalog step failed: {exc}")
                    summary["sentinel"] = {"status": "error", "error": str(exc)}

        if not self.download.enabled:
            logger.info("[Step 3/5] Download — SKIPPED (enabled: false)")
            summary["download"] = {"status": "skipped"}
        else:
            logger.info("[Step 3/5] Sentinel-2 image download")
            if dry_run:
                summary["download"] = {"status": "dry_run"}
            else:
                try:
                    summary["download"] = self._run_download()
                except Exception as exc:
                    logger.error(f"  Download step failed: {exc}")
                    summary["download"] = {"status": "error", "error": str(exc)}

        if not self.acolite.enabled:
            logger.info("[Step 4/5] ACOLITE — SKIPPED (enabled: false)")
            summary["acolite"] = {"status": "skipped"}
        else:
            logger.info("[Step 4/5] ACOLITE atmospheric correction")
            if dry_run:
                summary["acolite"] = {"status": "dry_run"}
            else:
                try:
                    summary["acolite"] = self._run_acolite()
                except Exception as exc:
                    logger.error(f"  ACOLITE step failed: {exc}")
                    summary["acolite"] = {"status": "error", "error": str(exc)}

        if not self.acolite.datacube.enabled:
            logger.info("[Step 5/5] L2W datacube — SKIPPED (enabled: false)")
            summary["datacube"] = {"status": "skipped"}
        else:
            logger.info("[Step 5/5] L2W product datacube")
            if dry_run:
                summary["datacube"] = {"status": "dry_run"}
            else:
                try:
                    summary["datacube"] = self._run_l2w_datacube()
                except Exception as exc:
                    logger.error(f"  L2W datacube step failed: {exc}")
                    summary["datacube"] = {"status": "error", "error": str(exc)}

        logger.info(f"=== Pipeline complete: {self.campaign_name} ===")
        return summary

    # ---------------------------------------------------------------------------
    # Internal step runners
    # ---------------------------------------------------------------------------

    def _run_insitu(self) -> dict:
        from aquamatch.insitu_data import (
            read_stations,
            read_campaigns,
            clean_campaigns,
            merge_stations_campaigns,
            remove_duplicate_records,
        )
        import pandas as pd

        args = self.to_insitu_args()
        stations_df = read_stations(args["stations_path"])
        campaigns_df = read_campaigns(args["campaigns_path"])

        if stations_df.empty or campaigns_df.empty:
            raise RuntimeError("Empty stations or campaigns DataFrame — check inputs.")

        if not args["skip_clean"]:
            campaigns_df = clean_campaigns(campaigns_df)
        else:
            if (
                "fecha_muestra" in campaigns_df.columns
                and "date" not in campaigns_df.columns
            ):
                campaigns_df["fecha_muestra"] = pd.to_datetime(
                    campaigns_df["fecha_muestra"], errors="coerce"
                )
                campaigns_df = campaigns_df.rename(columns={"fecha_muestra": "date"})

        merged_df = merge_stations_campaigns(stations_df, campaigns_df)

        out_campaigns = args["output_campaigns_csv"]
        out_campaigns.parent.mkdir(parents=True, exist_ok=True)
        merged_df.drop(columns="observaciones", errors="ignore").to_csv(
            out_campaigns, index=False
        )

        df_clean = remove_duplicate_records(merged_df)
        df_clean = pd.DataFrame(
            df_clean, columns=["date", "latitud", "longitud", "s2_tile"]
        )
        out_unique = args["output_unique_csv"]
        out_unique.parent.mkdir(parents=True, exist_ok=True)
        df_clean.to_csv(out_unique, index=False)

        logger.info(f"  Campaigns saved: {out_campaigns}")
        logger.info(f"  Unique records saved: {out_unique}")
        return {"status": "ok", "n_unique": len(df_clean)}

    def _run_sentinel_catalog(self) -> dict:
        from aquamatch.sentinel_data import build_catalog

        args = self.to_sentinel_args()
        build_catalog(
            csv_file=args["csv"],
            output_json=args["catalog_json"],
            time_delta=args["time_delta"],
            cloud_cover=args["cloud_cover"],
        )
        return {"status": "ok", "catalog_json": str(args["catalog_json"])}

    def _run_download(self) -> dict:
        from aquamatch.sentinel_data import run_download

        args = self.to_sentinel_args()
        run_download(
            catalog_json=args["catalog_json"],
            output_dir=args["output_dir"],
            strategy=args["strategy"],
            max_per_date=args["max_per_date"],
            max_cloud_cover=args["max_cloud_cover"],
            download_scl=args["download_scl"],
        )
        return {"status": "ok"}

    def _run_acolite(self) -> dict:
        safe_dir = Path(self.acolite.io.safe_dir)
        scl_dir = Path(self.acolite.io.scl_dir)

        safe_list = sorted(safe_dir.rglob("*.SAFE"))
        if not safe_list:
            logger.warning(f"  No .SAFE folders found in {safe_dir}")
            acolite_result = {"status": "ok", "n_scenes": 0, "scenes": []}
        else:
            cfg = self.to_acolite_config()
            results = cfg.run_batch(
                safe_list=safe_list,
                base_output=self.acolite.io.output,
                use_scl=self.acolite.scl.use_scl,
                scl_dir=scl_dir if self.acolite.scl.use_scl else None,
                scl_kwargs=self.to_scl_kwargs(),
                continue_on_error=self.acolite.continue_on_error,
                tile_config=self.to_tile_config(),
                skip_existing=self.acolite.skip_existing,
            )

            n_ok = sum(1 for r in results if r.get("returncode") == 0)
            n_err = sum(1 for r in results if r.get("returncode") not in (0, None))
            n_skip = sum(
                1 for r in results if r.get("skipped") or r.get("skipped_existing")
            )

            acolite_result = {
                "status": "ok",
                "n_scenes": len(safe_list),
                "n_success": n_ok,
                "n_error": n_err,
                "n_skipped": n_skip,
                "scenes": results,
            }

        # --- Sub-step 4b: water polygon datacube ---
        # Runs regardless of whether SAFE files were found — the datacube
        # may already contain scenes from previous runs.
        if self.acolite.scl.build_polygon_datacube:
            logger.info("  [Sub-step 4b] Building water polygon datacube...")
            polygon_result = self._run_polygon_datacube()
            acolite_result["polygon_datacube"] = polygon_result
        else:
            logger.info(
                "  [Sub-step 4b] Water polygon datacube — SKIPPED "
                "(build_polygon_datacube: false)"
            )
            acolite_result["polygon_datacube"] = {"status": "skipped"}

        return acolite_result

    def _run_polygon_datacube(self) -> dict:
        """
        Build (or update) the water polygon GeoPackage datacube from all
        SCL GeoTIFF files found in ``scl_dir``.

        Records are built automatically by globbing ``{scl_dir}/*_SCL.tif``.
        The same ``min_area_m2``, ``simplify_tolerance``, and ``buffer_m``
        values from ``acolite.scl`` are reused — no duplication in YAML.

        Returns a status dict with keys ``status``, ``output_path``,
        ``n_records``.
        """
        scl_dir = Path(self.acolite.io.scl_dir)
        scl_files = sorted(scl_dir.glob("*_SCL.tif"))

        if not scl_files:
            logger.warning(
                f"  No SCL files found in {scl_dir} — "
                "water polygon datacube not built."
            )
            return {"status": "skipped", "reason": f"No SCL files in {scl_dir}"}

        records = [{"scl_path": str(f)} for f in scl_files]
        output_path = Path(self.acolite.scl.polygon_datacube_path)

        logger.info(
            f"  Building water polygon datacube from {len(records)} SCL files "
            f"→ {output_path}"
        )

        gpkg_path = build_water_polygon_datacube(
            records=records,
            output_path=output_path,
            overwrite=self.acolite.scl.polygon_datacube_overwrite,
            min_area_m2=self.acolite.scl.min_area_m2,
            simplify_tolerance=self.acolite.scl.simplify_tolerance,
            buffer_m=self.acolite.scl.buffer_m,
        )

        logger.info(f"  Water polygon datacube written: {gpkg_path}")
        return {
            "status": "ok",
            "output_path": str(gpkg_path),
            "n_records": len(records),
        }

    def _run_l2w_datacube(self) -> dict:
        """
        Aggregate all ``*_L2W.nc`` files found under ``acolite.io.output``
        into a single Zarr datacube using
        :func:`~aquamatch.acolite_spec.append_l2w_to_datacube`.

        Each L2W file is appended in chronological order (by acquisition date
        parsed from the filename).  If a date already exists in the datacube
        and ``overwrite_date=False``, that file is skipped with a warning.

        ``variables`` controls which L2W parameters are written.  ``None``
        means all variables present in each file.  A warning is emitted for
        any listed variable absent from a particular file; processing continues
        for the variables that are present.

        Returns a status dict with keys ``status``, ``output_path``,
        ``n_processed``, ``n_skipped``, ``n_error``.
        """
        output_dir = Path(self.acolite.io.output)
        l2w_files = sorted(output_dir.rglob("*_L2W.nc"))

        if not l2w_files:
            logger.warning(
                f"  No L2W NetCDF files found under {output_dir} — "
                "datacube not built."
            )
            return {
                "status": "skipped",
                "reason": f"No *_L2W.nc files under {output_dir}",
            }

        dc = self.acolite.datacube
        output_path = Path(dc.output_path)
        variables = dc.variables  # None or list[str]

        # Warn about any user-requested variables not seen across all files
        # (we do this lazily per-file inside the loop below)
        logger.info(
            f"  Building L2W datacube from {len(l2w_files)} files → {output_path}"
        )
        if variables:
            logger.info(f"  Requested variables: {variables}")

        stats = {"n_processed": 0, "n_skipped": 0, "n_error": 0}

        for l2w_nc in l2w_files:
            try:
                # Warn if any requested variable is missing from this file
                if variables:
                    import xarray as xr

                    with xr.open_dataset(l2w_nc) as ds:
                        missing = [v for v in variables if v not in ds.data_vars]
                    if missing:
                        logger.warning(
                            f"  {l2w_nc.name}: requested variable(s) not found "
                            f"and will be skipped: {missing}"
                        )

                append_l2w_to_datacube(
                    l2w_nc=l2w_nc,
                    datacube_path=output_path,
                    target_crs=dc.target_crs,
                    target_resolution=dc.target_resolution,
                    variables=variables,
                    zarr_chunks=dc.zarr_chunks,
                    overwrite_date=dc.overwrite_date,
                )
                stats["n_processed"] += 1
                logger.info(f"  Appended: {l2w_nc.name}")

            except Exception as exc:
                logger.error(f"  Error appending {l2w_nc.name}: {exc}")
                stats["n_error"] += 1

        logger.info(
            f"  L2W datacube complete — "
            f"processed: {stats['n_processed']}, "
            f"errors: {stats['n_error']}"
        )

        return {
            "status": "ok",
            "output_path": str(output_path),
            **stats,
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_KNOWN_TOP_LEVEL = {
    "campaign_name",
    "description",
    "insitu",
    "sentinel",
    "download",
    "acolite",
    "tiles",
}


def _check_keys(raw: dict, dataclass_type, context: str) -> None:
    """Raise ValueError if *raw* contains keys not present in *dataclass_type*."""
    if not raw:
        return

    if dataclass_type is PipelineConfig:
        known = _KNOWN_TOP_LEVEL
    else:
        known = {f.name for f in fields(dataclass_type)}

    unknown = set(raw.keys()) - known
    if unknown:
        raise ValueError(
            f"Unknown configuration key(s) in [{context}]: {sorted(unknown)}. "
            f"Valid keys: {sorted(known)}"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m aquamatch.pipeline_config",
        description="aquamatch — YAML-driven pipeline runner",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--generate",
        metavar="OUTPUT_YAML",
        help="Write a fully-commented YAML template to OUTPUT_YAML and exit.",
    )
    group.add_argument(
        "--run",
        metavar="CONFIG_YAML",
        help="Load CONFIG_YAML and run the full pipeline.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Log pipeline steps without executing them (only valid with --run).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help=(
            "Reprocess all scenes, even those whose output files already exist. "
            "Overrides skip_existing in the YAML config (only valid with --run)."
        ),
    )
    return parser


def main(argv=None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.generate:
        out = PipelineConfig.generate(args.generate)
        print(f"Template written to: {out}")

    elif args.run:
        cfg = PipelineConfig.from_yaml(args.run)
        if args.force:
            cfg.acolite.skip_existing = False
        cfg.run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
