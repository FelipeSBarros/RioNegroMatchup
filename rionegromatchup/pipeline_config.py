"""
pipeline_config.py
==================
YAML-driven pipeline configuration for the Río Negro Matchup workflow.

A single YAML file drives the entire pipeline — one file per campaign,
version-controlled, self-documenting.

Usage
-----
Generate a template::

    python -m rionegromatchup.pipeline_config --generate campaign_2025.yaml

Run the full pipeline::

    python -m rionegromatchup.pipeline_config --run campaign_2025.yaml

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

logger = logging.getLogger(__name__)

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
    unique_csv: str = "data/monitoring_data/campaigns_unique_data.csv"
    time_delta_days: int = 1
    cloud_cover_max: int = 10


@dataclass
class DownloadSection:
    enabled: bool = True
    output_dir: str = "data/sentinel_downloads"
    catalog_json: str = "data/sentinel_downloads/sentinel_catalog.json"
    only_first: bool = True
    download_scl: bool = True


@dataclass
class SclSection:
    use_scl: bool = True
    min_area_m2: float = 5_000.0
    simplify_tolerance: float = 20.0
    buffer_m: float = 0.0


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


@dataclass
class AcoliteOutputSection:
    export_geotiff: bool = True
    export_geotiff_coordinates: bool = True
    export_cloud_optimized_geotiff: bool = False
    netcdf_compression: bool = True
    netcdf_compression_level: int = 4
    map_rgb: bool = False
    map_rgb_maxrange: float = 0.15


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


# ---------------------------------------------------------------------------
# Tile configuration
# ---------------------------------------------------------------------------


@dataclass
class TileEntry:
    """
    Spatial restriction for a single Sentinel-2 MGRS tile.

    Exactly one of ``polygon`` or ``limit`` may be set.  If neither is
    set the tile is processed without any spatial restriction (full scene).
    Setting both is a configuration error and raises ``ValueError`` on
    ``validate()``.

    Attributes
    ----------
    polygon:
        Path to a GeoJSON or WKT file defining the tile's region of
        interest.  Takes precedence over ``limit`` at runtime.
    limit:
        Bounding box as ``[south, west, north, east]`` in decimal
        degrees.  Used when ``polygon`` is not set.
    """

    polygon: Optional[str] = None
    limit: Optional[list[float]] = None

    def validate(self, tile_id: str = "") -> None:
        """
        Validate the tile entry.

        Raises
        ------
        ValueError
            If both ``polygon`` and ``limit`` are set, or if ``limit``
            does not contain exactly four values.
        """
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
    """
    Per-tile spatial restrictions, keyed by 5-character MGRS tile code.

    Each entry is a ``TileEntry`` that optionally defines a ``polygon``
    or ``limit`` for that tile.  Tiles not listed here are processed
    without any spatial restriction.

    Example YAML::

        tiles:
          21HUD:
            polygon: data/polygons/21HUD.geojson
          21HVD:
            limit: [-34.2, -56.8, -33.0, -55.1]
          21HWD:
            # no restriction — full scene
    """

    entries: dict[str, TileEntry] = field(default_factory=dict)

    def get(self, tile_id: str) -> Optional[TileEntry]:
        """
        Return the ``TileEntry`` for *tile_id*, or ``None`` if not configured.

        Parameters
        ----------
        tile_id:
            5-character MGRS tile code (e.g. ``'21HUD'``).

        Returns
        -------
        TileEntry or None
        """
        return self.entries.get(tile_id)

    def validate(self) -> None:
        """
        Validate all tile entries.

        Raises
        ------
        ValueError
            If any ``TileEntry`` fails its own validation.
        """
        for tile_id, entry in self.entries.items():
            entry.validate(tile_id=tile_id)

    @classmethod
    def from_dict(cls, raw: dict) -> "TilesSection":
        """
        Parse a raw dict (from YAML) into a ``TilesSection``.

        Each key is a tile ID string; each value is a dict with optional
        ``polygon`` and/or ``limit`` keys.  Unknown keys within a tile
        entry raise ``ValueError``.

        Parameters
        ----------
        raw:
            Dict mapping tile ID strings to tile entry dicts.

        Returns
        -------
        TilesSection

        Raises
        ------
        ValueError
            If any tile entry contains unknown keys or fails validation.
        """
        entries: dict[str, TileEntry] = {}
        known_tile_keys = {f.name for f in fields(TileEntry)}

        for tile_id, tile_raw in raw.items():
            # Allow empty / null tile entries (no restriction)
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
    """Master pipeline configuration for Río Negro Matchup."""

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
        """
        Write a fully-commented YAML template to *output_path*.

        The template includes all parameters at their defaults, with inline
        comments documenting units, valid values, and descriptions.  This is
        the primary entry point for new users.

        Parameters
        ----------
        output_path:
            Destination YAML file.  The parent directory is created if it
            does not exist.

        Returns
        -------
        Path
            The path to the written YAML file.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        template = """\
# =============================================================================
# Río Negro Matchup — Pipeline Configuration
# =============================================================================
# One YAML file drives the entire pipeline: in situ data preparation,
# satellite catalog building, product download, and atmospheric correction.
#
# Usage:
#   Generate this template:
#     python -m rionegromatchup.pipeline_config --generate campaign_2025.yaml
#   Run the full pipeline:
#     python -m rionegromatchup.pipeline_config --run campaign_2025.yaml
# =============================================================================

campaign_name: rio_negro_2025       # Identifier for this campaign run
description: "Sentinel-2 / in situ water quality matchup"

# =============================================================================
# Step 1 — In situ data preparation
# =============================================================================
insitu:
  enabled: true                     # Set false to skip this step

  # Input files
  stations_path: data/original_data/estaciones-seleccionadas.xlsx
  campaigns_path: data/original_data/campaigns_sample.xlsx

  # Output files
  output_campaigns_csv: data/monitoring_data/campaigns_organized.csv
  output_unique_csv: data/monitoring_data/campaigns_unique_data.csv

  # If true, skip clean_campaigns() — use when OAN already cleaned the export
  skip_clean: false

# =============================================================================
# Step 2 — Sentinel-2 catalog search
# =============================================================================
sentinel:
  enabled: true

  catalog_json: data/sentinel_downloads/sentinel_catalog.json
  unique_csv: data/monitoring_data/campaigns_unique_data.csv

  time_delta_days: 1                # ±N days around each field date (int ≥ 0)
  cloud_cover_max: 10               # Maximum cloud cover percentage (0–100)

# =============================================================================
# Step 3 — Sentinel-2 image download
# =============================================================================
download:
  enabled: true

  output_dir: data/sentinel_downloads
  catalog_json: data/sentinel_downloads/sentinel_catalog.json

  only_first: true                  # Download only the first scene per date
  download_scl: true                # Also download SCL asset (recommended)

# =============================================================================
# Step 4 — Atmospheric correction (ACOLITE)
# =============================================================================
acolite:
  enabled: true

  # Path to the ACOLITE executable (acolite.py or the compiled binary)
  acolite_executable: /path/to/acolite/acolite.py

  # low_memory: true reduces RAM usage at the cost of processing speed.
  # Recommended when processing large tiles on machines with < 16 GB RAM.
  low_memory: false

  continue_on_error: true           # Keep processing remaining scenes on failure
  skip_existing: true               # Skip scenes whose output files already exist;

  # --- Input / Output ---
  io:
    output: data/acolite_output     # Root output directory for ACOLITE products
    safe_dir: data/sentinel_downloads   # Directory containing .SAFE folders
    scl_dir: data/sentinel_downloads/scl  # Directory containing SCL GeoTIFFs

  # --- Radiometric correction ---
  radcor:
    # Atmospheric correction processor: dsf | exp | tact
    aerosol_correction: dsf

    # DSF path reflectance method: tiled | fixed | percentile
    dsf_path_reflectance: tiled

    # DSF tile dimensions [rows, cols] in pixels
    dsf_tile_dimensions: [120, 120]

    # Minimum fraction of valid pixels required per DSF tile (0.0–1.0)
    dsf_minimum_tile_cover: 0.10

    # Use ECMWF ancillary data for atmospheric parameters (recommended)
    ancillary_data: true

    # Fallback values used when ancillary_data is false
    uoz: 0.3                        # Ozone column (cm-atm)
    uwv: 1.5                        # Water vapour column (g/cm²)
    pressure: 1013.25               # Sea-level pressure (hPa)

  # --- Sunglint correction ---
  glint:
    glint_correction: true

    # Glint correction method: none | hedley | vanhellemont2019
    glint_method: vanhellemont2019

    glint_threshold: 0.01           # Minimum glint reflectance to correct
    glint_mask_rhos: true           # Mask pixels above threshold
    glint_mask_rhos_threshold: 0.15 # Masking threshold (dimensionless)

  # --- L2W water quality products ---
  l2w:
    # Parameters to compute. Available: t_nechad, spm_nechad, chl_oc3,
    # chl_re, aphy_443, fai, ndwi, ndvi, and others supported by ACOLITE.
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
    l2w_mask_high_toa_threshold: 0.3  # TOA reflectance threshold

    # Water expression for masking land. Set null to disable.
    l2w_mask_water_expr: "rhos_1600 < 0.0215"

    output_rhorc: false             # Output Rayleigh-corrected reflectance
    output_rhos: true               # Output surface reflectance

  # --- Output formats ---
  output_format:
    export_geotiff: true
    export_geotiff_coordinates: true
    export_cloud_optimized_geotiff: false
    netcdf_compression: true
    netcdf_compression_level: 4    # 1 (fast) – 9 (small); recommended: 4–6
    map_rgb: false
    map_rgb_maxrange: 0.15         # RGB stretch maximum (dimensionless)

  # --- SCL-based water polygon clipping ---
  scl:
    use_scl: true                   # Extract water mask from SCL and clip
    min_area_m2: 5000.0             # Minimum water polygon area (m²)
    simplify_tolerance: 20.0        # Douglas-Peucker tolerance (m, ~1 pixel)
    buffer_m: 0.0                   # Outward buffer around water mask (m)

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

        Parameters
        ----------
        path:
            Path to the YAML configuration file.

        Returns
        -------
        PipelineConfig

        Raises
        ------
        FileNotFoundError
            If the YAML file does not exist.
        ValueError
            If the YAML contains unknown keys at any level.
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

        acolite_sub = {
            k: v
            for k, v in acolite_raw.items()
            if k not in ("io", "radcor", "glint", "l2w", "output_format", "scl")
        }
        _check_keys(acolite_sub, AcoliteSection, context="acolite")

        io_raw = acolite_raw.get("io", {})
        radcor_raw = acolite_raw.get("radcor", {})
        glint_raw = acolite_raw.get("glint", {})
        l2w_raw = acolite_raw.get("l2w", {})
        output_raw = acolite_raw.get("output_format", {})
        scl_raw = acolite_raw.get("scl", {})

        _check_keys(io_raw, AcoliteIOSection, context="acolite.io")
        _check_keys(radcor_raw, AcoliteRadCorSection, context="acolite.radcor")
        _check_keys(glint_raw, AcoliteGlintSection, context="acolite.glint")
        _check_keys(l2w_raw, AcoliteL2WSection, context="acolite.l2w")
        _check_keys(output_raw, AcoliteOutputSection, context="acolite.output_format")
        _check_keys(scl_raw, SclSection, context="acolite.scl")

        # Handle dsf_tile_dimensions: YAML list → tuple
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
        )

        # tiles_raw may be None (YAML `tiles:` with no value) or a dict
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
        """
        Convert the acolite section to an ``AcoliteConfig`` instance.

        When ``acolite.low_memory`` is true the config is created via a
        hypothetical ``AcoliteConfig.low_memory()`` classmethod; otherwise
        the standard constructor is used.  Currently both code paths produce
        the same object — the hook is here so callers can customise it later.

        Returns
        -------
        AcoliteConfig
        """
        from rionegromatchup.acolite_spec import (
            AcoliteConfig,
            IOConfig,
            RadCorConfig,
            GlintConfig,
            L2WConfig,
            OutputConfig,
            AcoliteAtmosphericProcessor,
            AcoliteGlintCorrection,
        )

        a = self.acolite

        limit = tuple(a.io.limit) if a.io.limit is not None else None

        io = IOConfig(
            inputfile="",  # populated per-scene by run_batch
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
        )

        output_format = OutputConfig(
            export_geotiff=a.output_format.export_geotiff,
            export_geotiff_coordinates=a.output_format.export_geotiff_coordinates,
            export_cloud_optimized_geotiff=a.output_format.export_cloud_optimized_geotiff,
            netcdf_compression=a.output_format.netcdf_compression,
            netcdf_compression_level=a.output_format.netcdf_compression_level,
            map_rgb=a.output_format.map_rgb,
            map_rgb_maxrange=a.output_format.map_rgb_maxrange,
        )

        cfg = AcoliteConfig(
            acolite_executable=a.acolite_executable,
            io=io,
            radcor=radcor,
            glint=glint,
            l2w=l2w,
            output_format=output_format,
        )
        return cfg

    def to_tile_config(self) -> TilesSection:
        """
        Return the ``TilesSection`` for use in ``run_batch``.

        This is a thin accessor kept for symmetry with the other
        ``to_*`` converters (``to_acolite_config``, ``to_scl_kwargs``,
        ``to_insitu_args``, ``to_sentinel_args``).  Callers that need
        to pass tile spatial restrictions to ``run_batch`` or
        ``from_campaigns_row`` should use this rather than accessing
        ``self.tiles`` directly.

        Returns
        -------
        TilesSection
            The tile spatial restrictions defined in this config.
            Returns an empty ``TilesSection`` when no tiles are configured.
        """
        return self.tiles

    def to_scl_kwargs(self) -> dict:
        """
        Return keyword arguments for SCL water extraction functions.

        Returns
        -------
        dict
            Keys: ``min_area_m2``, ``simplify_tolerance``, ``buffer_m``.
        """
        s = self.acolite.scl
        return {
            "min_area_m2": s.min_area_m2,
            "simplify_tolerance": s.simplify_tolerance,
            "buffer_m": s.buffer_m,
        }

    def to_insitu_args(self) -> dict:
        """
        Return arguments for the in situ data pipeline step.

        Returns
        -------
        dict
            Keys mirror the CLI / function arguments of ``insitu_data``.
        """
        s = self.insitu
        return {
            "stations_path": Path(s.stations_path),
            "campaigns_path": Path(s.campaigns_path),
            "output_campaigns_csv": Path(s.output_campaigns_csv),
            "output_unique_csv": Path(s.output_unique_csv),
            "skip_clean": s.skip_clean,
        }

    def to_sentinel_args(self) -> dict:
        """
        Return arguments for the Sentinel-2 catalog and download steps.

        Returns
        -------
        dict
            Keys for catalog building (``unique_csv``, ``catalog_json``,
            ``time_delta_days``, ``cloud_cover_max``) and download
            (``output_dir``, ``only_first``, ``download_scl``).
        """
        return {
            "unique_csv": Path(self.sentinel.unique_csv),
            "catalog_json": Path(self.sentinel.catalog_json),
            "time_delta_days": self.sentinel.time_delta_days,
            "cloud_cover_max": self.sentinel.cloud_cover_max,
            "output_dir": Path(self.download.output_dir),
            "only_first": self.download.only_first,
            "download_scl": self.download.download_scl,
        }

    # ---------------------------------------------------------------------------
    # Orchestration
    # ---------------------------------------------------------------------------

    def run(self, dry_run: bool = False) -> dict:
        """
        Execute the full pipeline in order, respecting ``enabled`` flags.

        Steps
        -----
        1. In situ data preparation (``insitu.enabled``)
        2. Sentinel-2 catalog building (``sentinel.enabled``)
        3. Image download (``download.enabled``)
        4. ACOLITE atmospheric correction (``acolite.enabled``)

        Parameters
        ----------
        dry_run:
            If True, log what would be executed without calling any external
            tools.  Useful for testing the configuration.

        Returns
        -------
        dict
            Summary with keys ``insitu``, ``sentinel``, ``download``,
            ``acolite``, each containing a status string and any results.
        """
        summary: dict[str, Any] = {}
        logger.info(
            f"=== Pipeline: {self.campaign_name} ==="
            + (" [DRY RUN]" if dry_run else "")
        )

        # --- Step 1: In situ ---
        if not self.insitu.enabled:
            logger.info("[Step 1/4] in situ — SKIPPED (enabled: false)")
            summary["insitu"] = {"status": "skipped"}
        else:
            logger.info("[Step 1/4] In situ data preparation")
            if dry_run:
                summary["insitu"] = {"status": "dry_run"}
            else:
                try:
                    summary["insitu"] = self._run_insitu()
                except Exception as exc:
                    logger.error(f"  In situ step failed: {exc}")
                    summary["insitu"] = {"status": "error", "error": str(exc)}

        # --- Step 2: Sentinel catalog ---
        if not self.sentinel.enabled:
            logger.info("[Step 2/4] Sentinel catalog — SKIPPED (enabled: false)")
            summary["sentinel"] = {"status": "skipped"}
        else:
            logger.info("[Step 2/4] Sentinel-2 catalog search")
            if dry_run:
                summary["sentinel"] = {"status": "dry_run"}
            else:
                try:
                    summary["sentinel"] = self._run_sentinel_catalog()
                except Exception as exc:
                    logger.error(f"  Sentinel catalog step failed: {exc}")
                    summary["sentinel"] = {"status": "error", "error": str(exc)}

        # --- Step 3: Download ---
        if not self.download.enabled:
            logger.info("[Step 3/4] Download — SKIPPED (enabled: false)")
            summary["download"] = {"status": "skipped"}
        else:
            logger.info("[Step 3/4] Sentinel-2 image download")
            if dry_run:
                summary["download"] = {"status": "dry_run"}
            else:
                try:
                    summary["download"] = self._run_download()
                except Exception as exc:
                    logger.error(f"  Download step failed: {exc}")
                    summary["download"] = {"status": "error", "error": str(exc)}

        # --- Step 4: ACOLITE ---
        if not self.acolite.enabled:
            logger.info("[Step 4/4] ACOLITE — SKIPPED (enabled: false)")
            summary["acolite"] = {"status": "skipped"}
        else:
            logger.info("[Step 4/4] ACOLITE atmospheric correction")
            if dry_run:
                summary["acolite"] = {"status": "dry_run"}
            else:
                try:
                    summary["acolite"] = self._run_acolite()
                except Exception as exc:
                    logger.error(f"  ACOLITE step failed: {exc}")
                    summary["acolite"] = {"status": "error", "error": str(exc)}

        logger.info(f"=== Pipeline complete: {self.campaign_name} ===")
        return summary

    # ---------------------------------------------------------------------------
    # Internal step runners
    # ---------------------------------------------------------------------------

    def _run_insitu(self) -> dict:
        from rionegromatchup.insitu_data import (
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
        from rionegromatchup.sentinel_data import build_catalog

        args = self.to_sentinel_args()
        build_catalog(
            csv_file=args["unique_csv"],
            output_json=args["catalog_json"],
            time_delta=args["time_delta_days"],
            cloud_cover=args["cloud_cover_max"],
        )
        return {"status": "ok", "catalog_json": str(args["catalog_json"])}

    def _run_download(self) -> dict:
        from rionegromatchup.sentinel_data import run_download

        args = self.to_sentinel_args()
        run_download(
            catalog_json=args["catalog_json"],
            output_dir=args["output_dir"],
            only_first=args["only_first"],
            download_scl=args["download_scl"],
        )
        return {"status": "ok"}

    def _run_acolite(self) -> list[dict]:
        safe_dir = Path(self.acolite.io.safe_dir)
        scl_dir = Path(self.acolite.io.scl_dir)

        safe_list = sorted(safe_dir.rglob("*.SAFE"))
        if not safe_list:
            logger.warning(f"  No .SAFE folders found in {safe_dir}")
            return []

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
        return results


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

    # For top-level PipelineConfig we use the hard-coded set
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
        prog="python -m rionegromatchup.pipeline_config",
        description="Río Negro Matchup — YAML-driven pipeline runner",
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
        cfg.run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
