"""
acolite_spec.py
===============
Spec-driven configuration for ACOLITE atmospheric correction
and water quality (L2W) product generation.

Each dataclass section maps to a logical group of ACOLITE settings.
All fields carry a docstring, type annotation, and sensible default
so that a minimal run requires only `inputfile`, `output`, and a
geographic limit/polygon.

Usage (single image)
---------------------
    from acolite_spec import AcoliteConfig, IOConfig

    cfg = AcoliteConfig(
        acolite_executable="/path/to/acolite",
        io=IOConfig(
            inputfile="data/sentinel_downloads/S2A_MSIL1C_20250801.SAFE",
            output="data/acolite_output",
            limit=(-33.0, -57.0, -32.5, -56.0),   # S, W, N, E
        ),
    )
    cfg.validate()
    result = cfg.run()

Usage (batch — list of SAFE folders)
--------------------------------------
    safe_list = [
        "data/sentinel_downloads/S2A_MSIL1C_20250801.SAFE",
        "data/sentinel_downloads/S2B_MSIL1C_20250815.SAFE",
    ]
    results = cfg.run_batch(safe_list, base_output="data/acolite_output")

Usage (build a spatio-temporal datacube from batch results)
-----------------------------------------------------------
    from acolite_spec import append_l2w_to_datacube

    # Call once per scene — each call appends a new time slice.
    # All scenes are reprojected to the same grid automatically.
    for result in results:
        if result["l2w_file"] is not None:
            append_l2w_to_datacube(
                l2w_nc=result["l2w_file"],
                datacube_path="data/acolite_output/datacube.zarr",
                target_crs="EPSG:4326",
                target_resolution=0.0001,   # degrees (~10 m at these latitudes)
            )

    # Open the finished datacube
    import xarray as xr
    dc = xr.open_zarr("data/acolite_output/datacube.zarr")
    print(dc)   # Dimensions: (time: N, y: ..., x: ...)

Usage (convert L2W NetCDF to Zarr + COG after a run)
------------------------------------------------------
    from acolite_spec import convert_l2w_to_zarr_cog

    zarr_path, cog_paths = convert_l2w_to_zarr_cog(
        l2w_nc=result["l2w_file"],
        output_dir="data/acolite_output/cloud",
    )

Usage (from campaigns CSV)
---------------------------
    cfg = AcoliteConfig.from_campaigns_row(row, base_output="data/acolite_output")

"""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class AcoliteAtmosphericProcessor(str, Enum):
    """ACOLITE atmospheric correction processor."""
    DSF = "dsf"           # Dark Spectrum Fitting (default for most sensors)
    EXP = "exp"           # Exponential extrapolation
    TACT = "tact"         # Thermal AtmoCorrection (thermal band correction)


class AcoliteGlintCorrection(str, Enum):
    """Sun-glint correction method."""
    NONE = "none"
    HEDLEY = "hedley"
    VANHELLEMONT = "vanhellemont2019"


class AcoliteSurfaceReflectance(str, Enum):
    """Target surface reflectance type for L2R output."""
    RHO_S = "rhos"        # Surface reflectance (default)
    RHO_RC = "rhorc"      # Rayleigh-corrected reflectance


# ---------------------------------------------------------------------------
# Section dataclasses
# ---------------------------------------------------------------------------

@dataclass
class IOConfig:
    """
    Input / Output and Region of Interest (ROI) parameters.

    These define *what* data is processed and *where* outputs go.
    """

    inputfile: str
    """
    Full path to the input scene directory or SAFE file.
    For batch runs via run_batch(), this field is overridden per image
    and does not need to be set manually.
    Examples:
        "data/sentinel_downloads/S2A_MSIL1C_20250801.SAFE"
        "data/s2a.SAFE,data/s2b.SAFE"
    """

    output: str
    """
    Directory where all generated products (L1R, L2R, L2W) will be saved.
    Will be created if it does not exist.
    """

    limit: Optional[tuple[float, float, float, float]] = None
    """
    Geographic bounding box as (south, west, north, east) in decimal degrees.
    Example: (-33.0, -57.0, -32.5, -56.0)
    Mutually exclusive with `polygon`.
    """

    polygon: Optional[str] = None
    """
    Path to a GeoJSON or WKT file defining a non-rectangular ROI.
    Mutually exclusive with `limit`.
    """

    def validate(self) -> None:
        if self.limit is not None and self.polygon is not None:
            raise ValueError(
                "Specify either `limit` or `polygon`, not both."
            )
        if self.limit is not None:
            s, w, n, e = self.limit
            if s >= n:
                raise ValueError(f"limit: south ({s}) must be < north ({n}).")
            if w >= e:
                raise ValueError(f"limit: west ({w}) must be < east ({e}).")
            if not (-90 <= s <= 90 and -90 <= n <= 90):
                raise ValueError("limit: latitude values must be in [-90, 90].")
            if not (-180 <= w <= 180 and -180 <= e <= 180):
                raise ValueError("limit: longitude values must be in [-180, 180].")
        if self.inputfile and not Path(self.inputfile.split(",")[0].strip()).exists():
            raise FileNotFoundError(
                f"inputfile not found: {self.inputfile.split(',')[0].strip()}"
            )


@dataclass
class TACTConfig:
    """
    TACT — Thermal AtmoCorrection settings.

    Only relevant when processing thermal imagery. For Sentinel-2,
    this is typically left disabled.
    """

    tact_run: bool = False
    """Enable TACT thermal atmospheric correction."""

    tact_emissivity: float = 0.985
    """
    Surface emissivity assumed for TACT correction.
    Typical open-water value: 0.985–0.990.
    """

    tact_reanalysis: str = "era5"
    """
    Meteorological reanalysis product used for TACT.
    Options: 'era5', 'ncep'.
    """


@dataclass
class RadCorConfig:
    """
    Radiometric Correction (RAdCor) settings.

    Controls ACOLITE's atmospheric correction processor and its
    auxiliary parameters.
    """

    aerosol_correction: AcoliteAtmosphericProcessor = AcoliteAtmosphericProcessor.DSF
    """
    Atmospheric correction method.
    - 'dsf'  : Dark Spectrum Fitting (recommended for coastal/inland waters).
    - 'exp'  : Exponential extrapolation (alternative for turbid waters).
    - 'tact' : Thermal correction (not applicable to Sentinel-2 VNIR).
    """

    dsf_path_reflectance: str = "tiled"
    """
    DSF path reflectance retrieval strategy.
    Options: 'tiled', 'scene', 'fixed'.
    - 'tiled'  : spatially varying correction (default, recommended).
    - 'scene'  : single value per scene.
    - 'fixed'  : user-supplied fixed value.
    """

    dsf_tile_dimensions: tuple[int, int] = (120, 120)
    """
    Tile size (rows, columns) used when dsf_path_reflectance='tiled'.
    Smaller tiles increase spatial detail but raise computation time.
    """

    dsf_minimum_tile_cover: float = 0.10
    """
    Minimum fraction of valid (non-masked) pixels required per tile
    to compute aerosol retrieval. Tiles below this threshold are skipped.
    """

    ancillary_data: bool = True
    """
    Use ancillary meteorological data (ozone, water vapour, pressure)
    from NASA EARTHDATA to improve atmospheric correction.
    Requires internet access or pre-downloaded ancillary files.
    """

    uoz: float = 0.3
    """
    Total column ozone [cm-atm] used when ancillary_data=False.
    Typical value: 0.3.
    """

    uwv: float = 1.5
    """
    Total column water vapour [g cm⁻²] used when ancillary_data=False.
    Typical value: 1.5.
    """

    pressure: float = 1013.25
    """
    Sea-level atmospheric pressure [hPa] used when ancillary_data=False.
    Standard atmosphere: 1013.25 hPa.
    """


@dataclass
class GlintConfig:
    """
    Sun-glint correction settings.

    Glint contamination is common in near-nadir water imagery.
    These settings control if and how it is corrected.
    """

    glint_correction: bool = True
    """Enable sun-glint correction."""

    glint_method: AcoliteGlintCorrection = AcoliteGlintCorrection.VANHELLEMONT
    """
    Glint correction algorithm.
    - 'none'              : no correction.
    - 'hedley'            : Hedley et al. (2005) NIR-regression method.
    - 'vanhellemont2019'  : Vanhellemont (2019) SWIR-based method (default).
    """

    glint_threshold: float = 0.01
    """
    Minimum NIR/SWIR reflectance threshold [dimensionless] above which
    glint correction is applied.
    """

    glint_mask_rhos: bool = True
    """
    Mask pixels where glint-corrected surface reflectance remains
    above a defined threshold (i.e., residual glint contamination).
    """

    glint_mask_rhos_threshold: float = 0.15
    """
    Surface reflectance threshold [dimensionless] used when
    glint_mask_rhos=True. Pixels above this value are flagged.
    """


@dataclass
class L2WConfig:
    """
    L2W — Water Quality (Level-2 Water) product settings.

    Controls which bio-optical algorithms are applied to the
    atmospherically corrected reflectances to derive water quality
    parameters such as chlorophyll-a, turbidity, and CDOM.
    """

    l2w_parameters: list[str] = field(default_factory=lambda: [
        "t_nechad",        # Turbidity (Nechad et al. 2010)
        "spm_nechad",      # Suspended Particulate Matter
        "chl_oc3",         # Chlorophyll-a (OC3 algorithm)
        "chl_re",          # Chlorophyll-a (Red-Edge algorithm, Sentinel-2 only)
        "aphy_443",        # Phytoplankton absorption at 443 nm
        "fai",             # Floating Algae Index
        "ndwi",            # Normalized Difference Water Index
        "ndvi",            # Normalized Difference Vegetation Index
    ])
    """
    List of L2W bio-optical parameters to compute.
    Only parameters relevant to the sensor and available bands
    will be produced; unavailable ones are silently skipped by ACOLITE.
    """

    l2w_mask: bool = True
    """Apply pixel quality masking to L2W products."""

    l2w_mask_negative_rhos: bool = True
    """
    Mask pixels with negative surface reflectance after atmospheric
    correction (indicates cloud shadow or processing artefact).
    """

    l2w_mask_cirrus: bool = True
    """Mask thin cirrus cloud pixels using the 1375 nm cirrus band."""

    l2w_mask_high_toa: bool = True
    """
    Mask pixels with top-of-atmosphere reflectance above a threshold
    (likely clouds or snow/ice).
    """

    l2w_mask_high_toa_threshold: float = 0.3
    """
    TOA reflectance threshold used when l2w_mask_high_toa=True.
    Pixels with rho_toa > threshold are masked.
    """

    l2w_mask_water_expr: Optional[str] = "rhos_1600 < 0.0215"
    """
    Boolean expression (evaluated against surface reflectances) to
    restrict L2W processing to water pixels only.
    Default expression uses the SWIR band at 1600 nm following
    Vanhellemont & Ruddick (2014).
    Set to None to disable water masking (process all pixels).
    """

    output_rhorc: bool = False
    """Output Rayleigh-corrected reflectances (L2R rhorc) in addition to rhos."""

    output_rhos: bool = True
    """Output surface reflectances (L2R rhos)."""


@dataclass
class OutputConfig:
    """
    Output format and ancillary export settings.
    """

    export_geotiff: bool = True
    """Export output products as GeoTIFF files."""

    export_geotiff_coordinates: bool = True
    """Embed geographic coordinate information in output GeoTIFFs."""

    export_cloud_optimized_geotiff: bool = False
    """
    Write output GeoTIFFs as Cloud Optimized GeoTIFFs (COGs) directly
    from ACOLITE. Note: ACOLITE's native COG support is limited.
    For full COG and Zarr conversion of L2W products, use
    convert_l2w_to_zarr_cog() as a post-processing step instead.
    """

    netcdf_compression: bool = True
    """Apply lossless compression to NetCDF outputs."""

    netcdf_compression_level: int = 4
    """
    NetCDF compression level [1–9].
    Higher values yield smaller files but slower writes.
    """

    map_rgb: bool = False
    """Generate a quick-look RGB composite image."""

    map_rgb_maxrange: float = 0.15
    """
    Maximum reflectance value for RGB composite colour scaling.
    Adjust upward for turbid / highly reflective scenes.
    """


# ---------------------------------------------------------------------------
# Post-processing: NetCDF → spatio-temporal datacube (Zarr)
# ---------------------------------------------------------------------------

def _parse_date_from_l2w(l2w_nc: Path) -> "pd.Timestamp":
    """
    Extract the acquisition date from an ACOLITE L2W filename.

    ACOLITE names its outputs in two observed formats:
        S2A_MSI_20250801_...._L2W.nc              (compact: YYYYMMDD)
        S2A_MSI_2017_07_13_14_01_45_..._L2W.nc   (separated: YYYY_MM_DD_HH_MM_SS)

    Both are handled. Raises ValueError if no date pattern is found.
    """
    import re
    import pandas as pd

    # Pattern 1: compact YYYYMMDD between underscores
    match = re.search(r"_(\d{8})_", l2w_nc.name)
    if match:
        return pd.Timestamp(match.group(1))

    # Pattern 2: YYYY_MM_DD_HH_MM_SS (separated components)
    match = re.search(r"_(\d{4})_(\d{2})_(\d{2})_\d{2}_\d{2}_\d{2}_", l2w_nc.name)
    if match:
        year, month, day = match.group(1), match.group(2), match.group(3)
        return pd.Timestamp(f"{year}-{month}-{day}")

    raise ValueError(
        f"Could not parse acquisition date from filename '{l2w_nc.name}'. "
        "Expected format: S2A_MSI_YYYYMMDD_..._L2W.nc "
        "or S2A_MSI_YYYY_MM_DD_HH_MM_SS_..._L2W.nc"
    )


def append_l2w_to_datacube(
    l2w_nc: str | Path,
    datacube_path: str | Path,
    target_crs: str = "EPSG:4326",
    target_resolution: float = 0.0001,
    variables: Optional[list[str]] = None,
    zarr_chunks: dict = None,
    overwrite_date: bool = False,
) -> Path:
    """
    Reproject an ACOLITE L2W NetCDF to a common grid and append it as
    a new time slice to a shared spatio-temporal Zarr datacube.

    Each call adds one scene (one timestamp) to the datacube regardless
    of its original UTM zone or pixel grid. The first call creates the
    datacube and defines the target grid; subsequent calls append to it.

    The datacube has dimensions (time, y, x) where:
        - time : acquisition date parsed from the ACOLITE filename
        - y    : latitude in target_crs (or northing if projected)
        - x    : longitude in target_crs (or easting if projected)

    Parameters
    ----------
    l2w_nc:
        Path to the ACOLITE L2W NetCDF file (*_L2W.nc) to append.
    datacube_path:
        Path to the Zarr store to create or append to.
        Created on first call; appended on subsequent calls.
    target_crs:
        CRS for the common grid, as an EPSG string.
        Default is EPSG:4326 (WGS84 geographic) which avoids UTM zone
        boundary issues across scenes.
        Use a projected CRS (e.g. "EPSG:32721") if you need metre units.
    target_resolution:
        Pixel size in the units of target_crs.
        For EPSG:4326: degrees (0.0001° ≈ 10 m at mid-latitudes).
        For UTM: metres (e.g. 10.0 for 10 m resolution).
    variables:
        Variables to include in the datacube. If None, all L2W data
        variables are included. The same set must be consistent across
        all appended scenes.
    zarr_chunks:
        Chunk sizes as a dict. Defaults to
        {'time': 1, 'y': 512, 'x': 512}.
        A chunk size of 1 in time means each scene is its own chunk,
        which optimises reads of single time steps (e.g. for spatial
        queries at one date). Increase for time-series queries.
    overwrite_date:
        If True and a matching timestamp already exists in the datacube,
        overwrite it. If False (default), skip with a warning.

    Returns
    -------
    Path
        Path to the Zarr datacube store.

    Raises
    ------
    ImportError
        If xarray, rioxarray, or zarr are not installed.
    FileNotFoundError
        If l2w_nc does not exist.

    Example
    -------
        results = cfg.run_batch(safe_list, base_output="data/acolite_output")

        for result in results:
            if result["l2w_file"]:
                append_l2w_to_datacube(
                    l2w_nc=result["l2w_file"],
                    datacube_path="data/acolite_output/datacube.zarr",
                )

        import xarray as xr
        dc = xr.open_zarr("data/acolite_output/datacube.zarr")
        print(dc)
        # Dimensions: (time: N, y: ..., x: ...)
        # Select a single date
        dc.sel(time="2025-08-01")
        # Time series at a station point
        dc["chl_oc3"].sel(x=-56.5, y=-32.85, method="nearest")
    """
    try:
        import xarray as xr
        import rioxarray  # noqa: F401
        import zarr       # noqa: F401
        import numpy as np
        import pandas as pd
    except ImportError as e:
        raise ImportError(
            "append_l2w_to_datacube requires xarray, rioxarray, and zarr.\n"
            "Install with: pip install xarray rioxarray zarr\n"
            f"Original error: {e}"
        ) from e

    l2w_nc = Path(l2w_nc)
    datacube_path = Path(datacube_path)

    if not l2w_nc.exists():
        raise FileNotFoundError(f"L2W NetCDF not found: {l2w_nc}")

    GRID_MAPPING_NAMES = {
        "transverse_mercator", "polar_stereographic",
        "lambert_conformal_conic", "spatial_ref", "crs", "grid_mapping",
    }

    # ------------------------------------------------------------------
    # 1. Open and prepare the scene
    # ------------------------------------------------------------------
    date = _parse_date_from_l2w(l2w_nc)
    logger.info(f"Appending scene dated {date.date()} from {l2w_nc.name}")

    ds = xr.open_dataset(l2w_nc, decode_coords="all")

    data_vars = [
        v for v in ds.data_vars
        if v not in GRID_MAPPING_NAMES and ds[v].ndim >= 2
    ]

    if variables is not None:
        data_vars = [v for v in variables if v in data_vars]

    if not data_vars:
        raise ValueError(
            f"No exportable variables found in {l2w_nc.name}."
        )

    ds = ds[data_vars]

    # ------------------------------------------------------------------
    # 2. Reproject to the common target grid
    # ------------------------------------------------------------------
    logger.info(f"Reprojecting to {target_crs} at resolution {target_resolution}")

    reprojected = {}
    for var in data_vars:
        da = ds[var]

        x_dim = next((d for d in da.dims if d in ("x", "lon", "longitude")), None)
        y_dim = next((d for d in da.dims if d in ("y", "lat", "latitude")), None)

        if x_dim is None or y_dim is None:
            logger.warning(f"Skipping '{var}': spatial dims not found ({list(da.dims)})")
            continue

        da = da.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim)

        if da.rio.crs is None:
            logger.warning(f"Skipping '{var}': no CRS found.")
            continue

        da_reproj = da.rio.reproject(
            target_crs,
            resolution=target_resolution,
        )
        reprojected[var] = da_reproj

    if not reprojected:
        raise ValueError(
            f"No variables could be reprojected from {l2w_nc.name}. "
            "Check that the L2W file has valid CRS information."
        )

    # ------------------------------------------------------------------
    # 3. Assemble into a single-timestep Dataset with a time dimension
    # ------------------------------------------------------------------
    scene_ds = xr.Dataset(reprojected)

    # Standardise dimension names after reprojection
    rename_map = {}
    for dim in scene_ds.dims:
        if dim in ("lon", "longitude"):
            rename_map[dim] = "x"
        elif dim in ("lat", "latitude"):
            rename_map[dim] = "y"
    if rename_map:
        scene_ds = scene_ds.rename(rename_map)

    # Add the time dimension as a length-1 coordinate
    scene_ds = scene_ds.expand_dims(
        dim={"time": [date.to_datetime64()]}
    )

    # Cast all variables to float32 to keep the store compact
    scene_ds = scene_ds.astype(np.float32)

    chunks = zarr_chunks or {"time": 1, "y": 512, "x": 512}
    scene_ds = scene_ds.chunk(chunks)

    # ------------------------------------------------------------------
    # 4. Create or append to the Zarr datacube
    # ------------------------------------------------------------------
    if not datacube_path.exists():
        logger.info(f"Creating new datacube: {datacube_path}")
        scene_ds.to_zarr(datacube_path, mode="w")
        logger.info(
            f"Datacube created — variables: {list(scene_ds.data_vars)} | "
            f"shape: time=1, y={scene_ds.sizes['y']}, x={scene_ds.sizes['x']}"
        )
    else:
        # Check whether this date is already in the datacube
        existing = xr.open_zarr(datacube_path)
        existing_times = pd.DatetimeIndex(existing.time.values)
        existing.close()

        if date.normalize() in existing_times.normalize():
            if not overwrite_date:
                logger.warning(
                    f"Date {date.date()} already in datacube — skipping. "
                    "Set overwrite_date=True to replace it."
                )
                ds.close()
                return datacube_path
            else:
                logger.warning(
                    f"Date {date.date()} already in datacube — overwriting "
                    "is not yet supported for individual time slices. "
                    "Appending duplicate; deduplicate with "
                    "xr.open_zarr(...).drop_duplicates('time') if needed."
                )

        logger.info(f"Appending {date.date()} to existing datacube: {datacube_path}")
        scene_ds.to_zarr(datacube_path, append_dim="time")
        logger.info(f"Appended — datacube now has {len(existing_times) + 1} time steps")

    ds.close()
    return datacube_path


# ---------------------------------------------------------------------------
# Post-processing: NetCDF → Zarr + COG
# ---------------------------------------------------------------------------

def convert_l2w_to_zarr_cog(
    l2w_nc: str | Path,
    output_dir: str | Path,
    variables: Optional[list[str]] = None,
    zarr_chunks: dict = None,
    cog_overview_levels: list[int] = None,
    overwrite: bool = False,
) -> tuple[Path, list[Path]]:
    """
    Convert an ACOLITE L2W NetCDF file to Zarr (cloud-native) and
    per-variable Cloud Optimized GeoTIFFs (COGs).

    ACOLITE outputs NetCDF by default. Zarr and COG are better suited
    for cloud storage and tile-server access. This function performs the
    conversion as a post-processing step after a successful ACOLITE run.

    Requires: xarray, rioxarray, zarr, rasterio

    Parameters
    ----------
    l2w_nc:
        Path to the ACOLITE L2W NetCDF file (*_L2W.nc).
    output_dir:
        Directory where the Zarr store and COG files will be written.
    variables:
        List of variable names to export (e.g. ['chl_oc3', 't_nechad']).
        If None, all data variables present in the NetCDF are exported.
    zarr_chunks:
        Chunk sizes for Zarr storage as a dict, e.g. {'x': 512, 'y': 512}.
        Defaults to {'x': 512, 'y': 512} if not specified.
        Larger chunks improve compression; smaller chunks improve
        random-access performance for spatial queries.
    cog_overview_levels:
        Pyramid overview levels for COG files, e.g. [2, 4, 8, 16].
        Defaults to [2, 4, 8, 16] if not specified.
    overwrite:
        If True, overwrite existing Zarr store and COG files.
        If False, skip files that already exist.

    Returns
    -------
    tuple[Path, list[Path]]
        - zarr_path  : Path to the written Zarr store directory.
        - cog_paths  : List of Paths to the written COG .tif files,
                       one per exported variable.

    Raises
    ------
    ImportError
        If xarray, rioxarray, zarr, or rasterio are not installed.
    FileNotFoundError
        If the input L2W NetCDF file does not exist.

    Example
    -------
        zarr_path, cog_paths = convert_l2w_to_zarr_cog(
            l2w_nc="data/acolite_output/S2A_MSI_20250801_L2W.nc",
            output_dir="data/acolite_output/cloud",
            variables=["chl_oc3", "t_nechad"],
        )
    """
    try:
        import xarray as xr
        import rioxarray  # noqa: F401 — registers .rio accessor on xr.Dataset
        import zarr       # noqa: F401 — required by xarray zarr backend
        import rasterio
        from rasterio.enums import Resampling
    except ImportError as e:
        raise ImportError(
            f"convert_l2w_to_zarr_cog requires xarray, rioxarray, zarr, and "
            f"rasterio. Install them with: "
            f"pip install xarray rioxarray zarr rasterio\n"
            f"Original error: {e}"
        ) from e

    l2w_nc = Path(l2w_nc)
    output_dir = Path(output_dir)

    if not l2w_nc.exists():
        raise FileNotFoundError(f"L2W NetCDF not found: {l2w_nc}")

    output_dir.mkdir(parents=True, exist_ok=True)

    zarr_chunks = zarr_chunks or {"x": 512, "y": 512}
    cog_overview_levels = cog_overview_levels or [2, 4, 8, 16]

    # Grid-mapping scalar variables that must never be treated as raster layers
    GRID_MAPPING_NAMES = {
        "transverse_mercator", "polar_stereographic",
        "lambert_conformal_conic", "spatial_ref", "crs", "grid_mapping",
    }

    # ------------------------------------------------------------------
    # 1. Open with decode_coords="all" so the transverse_mercator /
    #    polar_stereographic variable is promoted to a coordinate and
    #    rioxarray can detect the CRS and spatial dims automatically.
    # ------------------------------------------------------------------
    logger.info(f"Opening L2W NetCDF: {l2w_nc}")
    ds = xr.open_dataset(l2w_nc, decode_coords="all")

    # Filter to genuine 2-D raster variables only
    available = [
        v for v in ds.data_vars
        if v not in GRID_MAPPING_NAMES and ds[v].ndim >= 2
    ]
    logger.info(f"Raster variables found: {available}")

    if variables is not None:
        missing = [v for v in variables if v not in available]
        if missing:
            logger.warning(f"Requested variables not found, skipping: {missing}")
        export_vars = [v for v in variables if v in available]
    else:
        export_vars = available

    if not export_vars:
        raise ValueError(
            "No exportable variables found. Check that the L2W file "
            "contains the requested parameters and is not empty."
        )

    # ------------------------------------------------------------------
    # 2. Write Zarr store (subset to export_vars)
    # ------------------------------------------------------------------
    zarr_path = output_dir / (l2w_nc.stem + ".zarr")

    if zarr_path.exists() and not overwrite:
        logger.info(f"Zarr store already exists, skipping: {zarr_path}")
    else:
        if zarr_path.exists() and overwrite:
            import shutil
            shutil.rmtree(zarr_path)
        logger.info(f"Writing Zarr store ({len(export_vars)} variables): {zarr_path}")
        ds[export_vars].chunk(zarr_chunks).to_zarr(zarr_path, mode="w")
        logger.info(f"Zarr store written: {zarr_path}")

    # ------------------------------------------------------------------
    # 3. Write one COG per variable
    # ------------------------------------------------------------------
    cog_paths: list[Path] = []

    for var in export_vars:
        cog_path = output_dir / f"{l2w_nc.stem}_{var}.tif"

        if cog_path.exists() and not overwrite:
            logger.info(f"COG already exists, skipping: {cog_path}")
            cog_paths.append(cog_path)
            continue

        da = ds[var]

        # Detect spatial dimension names from the DataArray dims
        x_dim = next((d for d in da.dims if d in ("x", "lon", "longitude")), None)
        y_dim = next((d for d in da.dims if d in ("y", "lat", "latitude")), None)

        if x_dim is None or y_dim is None:
            logger.warning(
                f"Cannot identify spatial dims for '{var}' "
                f"(dims={list(da.dims)}), skipping."
            )
            continue

        # Set spatial dims explicitly to avoid any rioxarray ambiguity
        da = da.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim)

        if da.rio.crs is None:
            logger.warning(f"No CRS found for '{var}', skipping COG export.")
            continue

        logger.info(
            f"Writing COG for '{var}' | CRS: {da.rio.crs} | shape: {da.shape}"
        )

        tmp_path = output_dir / f"_tmp_{var}.tif"
        try:
            da.rio.to_raster(str(tmp_path), driver="GTiff")

            with rasterio.open(tmp_path) as src:
                profile = src.profile.copy()
                profile.update(
                    driver="GTiff",
                    tiled=True,
                    blockxsize=512,
                    blockysize=512,
                    compress="deflate",
                    predictor=2,
                    interleave="band",
                )
                data = src.read()

            with rasterio.open(cog_path, "w", **profile) as dst:
                dst.write(data)
                dst.build_overviews(cog_overview_levels, Resampling.average)
                dst.update_tags(ns="rio_overview", resampling="average")

        finally:
            tmp_path.unlink(missing_ok=True)

        logger.info(f"COG written: {cog_path}")
        cog_paths.append(cog_path)

    ds.close()
    logger.info(
        f"Conversion complete — Zarr: {zarr_path} | COGs: {len(cog_paths)} file(s)"
    )
    return zarr_path, cog_paths


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------

@dataclass
class AcoliteConfig:
    """
    Master ACOLITE configuration.

    Aggregates all section configs and exposes helpers for validation,
    serialisation, single-image execution, and batch execution.

    Minimal example
    ---------------
        cfg = AcoliteConfig(
            acolite_executable="/path/to/acolite",
            io=IOConfig(
                inputfile="data/S2A_MSIL1C.SAFE",
                output="data/acolite_out",
                limit=(-33.0, -57.0, -32.5, -56.0),
            ),
        )
        cfg.validate()
        result = cfg.run()
    """

    acolite_executable: str
    """
    Full path to the ACOLITE binary (compiled executable from the
    REMSEM binary release).
    Example: "/opt/acolite/acolite"
    """

    io: IOConfig = field(default_factory=lambda: IOConfig(inputfile="", output=""))
    """Input / output and ROI parameters."""

    radcor: RadCorConfig = field(default_factory=RadCorConfig)
    """Radiometric / atmospheric correction parameters."""

    tact: TACTConfig = field(default_factory=TACTConfig)
    """TACT thermal correction parameters (Sentinel-2: usually disabled)."""

    glint: GlintConfig = field(default_factory=GlintConfig)
    """Sun-glint correction parameters."""

    l2w: L2WConfig = field(default_factory=L2WConfig)
    """Water quality (L2W) product parameters."""

    output_format: OutputConfig = field(default_factory=OutputConfig)
    """Output format and export settings."""

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """
        Validate the full configuration.

        Raises
        ------
        FileNotFoundError
            If the ACOLITE executable or inputfile cannot be found.
        ValueError
            If any parameter combination is logically inconsistent.
        """
        if not Path(self.acolite_executable).expanduser().exists():
            raise FileNotFoundError(
                f"ACOLITE executable not found: {self.acolite_executable}"
            )
        self.io.validate()

        if self.tact.tact_run and self.radcor.aerosol_correction != AcoliteAtmosphericProcessor.TACT:
            import warnings
            warnings.warn(
                "tact_run=True but aerosol_correction is not 'tact'. "
                "TACT will run in addition to the selected aerosol correction.",
                stacklevel=2,
            )

        if self.output_format.netcdf_compression_level not in range(1, 10):
            raise ValueError(
                "netcdf_compression_level must be between 1 and 9, "
                f"got {self.output_format.netcdf_compression_level}."
            )

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_settings_dict(self) -> dict[str, str]:
        """
        Return a flat dict of ACOLITE key=value settings.

        All values are stringified following ACOLITE's expected format.
        """
        d: dict[str, str] = {}

        # --- IO ---
        d["inputfile"] = self.io.inputfile
        d["output"] = self.io.output
        if self.io.limit is not None:
            s, w, n, e = self.io.limit
            d["limit"] = f"{s},{w},{n},{e}"
        if self.io.polygon is not None:
            d["polygon"] = self.io.polygon

        # --- RadCor ---
        d["aerosol_correction"] = self.radcor.aerosol_correction.value
        d["dsf_path_reflectance"] = self.radcor.dsf_path_reflectance
        rows, cols = self.radcor.dsf_tile_dimensions
        d["dsf_tile_dimensions"] = f"{rows},{cols}"
        d["dsf_minimum_tile_cover"] = str(self.radcor.dsf_minimum_tile_cover)
        d["ancillary_data"] = str(self.radcor.ancillary_data).lower()
        if not self.radcor.ancillary_data:
            d["uoz"] = str(self.radcor.uoz)
            d["uwv"] = str(self.radcor.uwv)
            d["pressure"] = str(self.radcor.pressure)

        # --- TACT ---
        d["tact_run"] = str(self.tact.tact_run).lower()
        if self.tact.tact_run:
            d["tact_emissivity"] = str(self.tact.tact_emissivity)
            d["tact_reanalysis"] = self.tact.tact_reanalysis

        # --- Glint ---
        d["glint_correction"] = str(self.glint.glint_correction).lower()
        if self.glint.glint_correction:
            d["glint_method"] = self.glint.glint_method.value
            d["glint_threshold"] = str(self.glint.glint_threshold)
            d["glint_mask_rhos"] = str(self.glint.glint_mask_rhos).lower()
            if self.glint.glint_mask_rhos:
                d["glint_mask_rhos_threshold"] = str(
                    self.glint.glint_mask_rhos_threshold
                )

        # --- L2W ---
        d["l2w_parameters"] = ",".join(self.l2w.l2w_parameters)
        d["l2w_mask"] = str(self.l2w.l2w_mask).lower()
        d["l2w_mask_negative_rhos"] = str(self.l2w.l2w_mask_negative_rhos).lower()
        d["l2w_mask_cirrus"] = str(self.l2w.l2w_mask_cirrus).lower()
        d["l2w_mask_high_toa"] = str(self.l2w.l2w_mask_high_toa).lower()
        d["l2w_mask_high_toa_threshold"] = str(self.l2w.l2w_mask_high_toa_threshold)
        if self.l2w.l2w_mask_water_expr is not None:
            d["l2w_mask_water_expr"] = self.l2w.l2w_mask_water_expr
        d["output_rhorc"] = str(self.l2w.output_rhorc).lower()
        d["output_rhos"] = str(self.l2w.output_rhos).lower()

        # --- Output format ---
        d["export_geotiff"] = str(self.output_format.export_geotiff).lower()
        d["export_geotiff_coordinates"] = str(
            self.output_format.export_geotiff_coordinates
        ).lower()
        d["export_cloud_optimized_geotiff"] = str(
            self.output_format.export_cloud_optimized_geotiff
        ).lower()
        d["netcdf_compression"] = str(self.output_format.netcdf_compression).lower()
        d["netcdf_compression_level"] = str(
            self.output_format.netcdf_compression_level
        )
        d["map_rgb"] = str(self.output_format.map_rgb).lower()
        if self.output_format.map_rgb:
            d["map_rgb_maxrange"] = str(self.output_format.map_rgb_maxrange)

        return d

    def to_settings_file(self, path: str | Path) -> Path:
        """
        Serialise configuration to an ACOLITE-compatible key=value settings file.

        Parameters
        ----------
        path:
            Destination file path. The parent directory will be created
            if it does not exist.

        Returns
        -------
        Path
            Resolved path to the written settings file.
        """
        out = Path(path).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        settings = self.to_settings_dict()
        lines = [f"{k}={v}\n" for k, v in settings.items()]
        out.write_text("".join(lines))
        return out

    # ------------------------------------------------------------------
    # Execution — single image
    # ------------------------------------------------------------------

    def _execute(self, settings_path: Path) -> dict:
        """
        Internal: call the ACOLITE binary with a settings file and
        collect outputs. Used by both run() and run_batch().
        """
        output_dir = Path(self.io.output)
        cmd = [
            str(Path(self.acolite_executable).expanduser().resolve()),
            "--cli",
            f"--settings={settings_path}",
        ]

        logger.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        logger.info(result.stdout)
        if result.returncode != 0:
            logger.error(f"ACOLITE exited with code {result.returncode}")
            logger.error(result.stderr)

        log_files = sorted(output_dir.glob("acolite_run_*.log"))
        l2w_files = sorted(output_dir.glob("*L2W.nc"))

        return {
            "returncode": result.returncode,
            "log_file":   log_files[-1] if log_files else None,
            "l2w_file":   l2w_files[-1] if l2w_files else None,
            "stdout":     result.stdout,
            "stderr":     result.stderr,
            "inputfile":  self.io.inputfile,
            "output_dir": output_dir,
        }

    def run(self, dry_run: bool = False) -> dict:
        """
        Execute ACOLITE for the single image defined in self.io.inputfile.

        Parameters
        ----------
        dry_run:
            If True, prints the command and settings without executing.

        Returns
        -------
        dict with keys:
            'returncode'  : int   — 0 means success
            'log_file'    : Path  — ACOLITE log file (if found)
            'l2w_file'    : Path  — L2W NetCDF output (if found)
            'stdout'      : str
            'stderr'      : str
            'inputfile'   : str   — image that was processed
            'output_dir'  : Path  — directory where outputs were written
        """
        self.validate()

        output_dir = Path(self.io.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        settings_path = self.to_settings_file(output_dir / "acolite_settings.txt")
        logger.info(f"Settings written to {settings_path}")

        if dry_run:
            cmd = [
                str(Path(self.acolite_executable).expanduser().resolve()),
                "--cli",
                f"--settings={settings_path}",
            ]
            logger.info(f"[dry_run] Command: {' '.join(cmd)}")
            logger.info(f"[dry_run] Settings:\n{settings_path.read_text()}")
            return {
                "returncode": None, "log_file": None, "l2w_file": None,
                "stdout": "", "stderr": "",
                "inputfile": self.io.inputfile, "output_dir": output_dir,
            }

        return self._execute(settings_path)

    # ------------------------------------------------------------------
    # Execution — batch (list of SAFE folders)
    # ------------------------------------------------------------------

    def run_batch(
        self,
        safe_list: list[str | Path],
        base_output: str | Path,
        dry_run: bool = False,
        continue_on_error: bool = True,
    ) -> list[dict]:
        """
        Run ACOLITE for each SAFE folder in safe_list, using the same
        correction settings defined in this config.

        Each image gets its own output sub-directory named after the
        SAFE folder stem (e.g. S2A_MSIL1C_20250801T...) so outputs
        never overwrite each other.

        Parameters
        ----------
        safe_list:
            List of paths to Sentinel-2 SAFE folders or other ACOLITE-
            compatible input directories.
        base_output:
            Parent output directory. Per-image subdirectories are
            created automatically:
                <base_output>/<SAFE_stem>/
        dry_run:
            If True, log the command for each image without executing.
        continue_on_error:
            If True (default), log errors and continue processing the
            remaining images when one fails.
            If False, raise immediately on the first non-zero return code.

        Returns
        -------
        list[dict]
            One result dict per image (same structure as run()).
            Includes a 'skipped' key (bool) for images that were not
            processed due to missing input paths.

        Example
        -------
            safe_folders = sorted(
                Path("data/sentinel_downloads").glob("*.SAFE")
            )
            results = cfg.run_batch(
                safe_list=safe_folders,
                base_output="data/acolite_output",
            )
            # Summarise
            ok  = [r for r in results if r["returncode"] == 0]
            err = [r for r in results if r["returncode"] not in (0, None)]
            print(f"{len(ok)} succeeded, {len(err)} failed")
        """
        if not Path(self.acolite_executable).expanduser().exists():
            raise FileNotFoundError(
                f"ACOLITE executable not found: {self.acolite_executable}"
            )

        base_output = Path(base_output)
        results = []
        total = len(safe_list)

        for idx, safe_path in enumerate(safe_list, start=1):
            safe_path = Path(safe_path)
            stem = safe_path.stem
            logger.info(f"[{idx}/{total}] Processing: {stem}")

            if not safe_path.exists():
                logger.warning(f"  SAFE folder not found, skipping: {safe_path}")
                results.append({
                    "returncode": None,
                    "log_file":   None,
                    "l2w_file":   None,
                    "stdout":     "",
                    "stderr":     f"Input not found: {safe_path}",
                    "inputfile":  str(safe_path),
                    "output_dir": None,
                    "skipped":    True,
                })
                continue

            # Each image gets its own output directory
            image_output = base_output / stem
            image_output.mkdir(parents=True, exist_ok=True)

            # Override IO for this image
            self.io.inputfile = str(safe_path)
            self.io.output = str(image_output)

            # Validate IO for this specific image
            try:
                self.io.validate()
            except (FileNotFoundError, ValueError) as e:
                logger.error(f"  Validation failed for {stem}: {e}")
                results.append({
                    "returncode": -1,
                    "log_file":   None,
                    "l2w_file":   None,
                    "stdout":     "",
                    "stderr":     str(e),
                    "inputfile":  str(safe_path),
                    "output_dir": image_output,
                    "skipped":    False,
                })
                if not continue_on_error:
                    raise
                continue

            # Write per-image settings file
            settings_path = self.to_settings_file(
                image_output / "acolite_settings.txt"
            )

            if dry_run:
                cmd = [
                    str(Path(self.acolite_executable).expanduser().resolve()),
                    "--cli",
                    f"--settings={settings_path}",
                ]
                logger.info(f"  [dry_run] Command: {' '.join(cmd)}")
                results.append({
                    "returncode": None,
                    "log_file":   None,
                    "l2w_file":   None,
                    "stdout":     "",
                    "stderr":     "",
                    "inputfile":  str(safe_path),
                    "output_dir": image_output,
                    "skipped":    False,
                })
                continue

            result = self._execute(settings_path)
            result["skipped"] = False
            results.append(result)

            if result["returncode"] != 0 and not continue_on_error:
                raise RuntimeError(
                    f"ACOLITE failed for {stem} "
                    f"(returncode={result['returncode']}). "
                    f"stderr: {result['stderr']}"
                )

        # Summary log
        processed = [r for r in results if not r.get("skipped")]
        ok  = [r for r in processed if r["returncode"] == 0]
        err = [r for r in processed if r["returncode"] not in (0, None)]
        skipped = [r for r in results if r.get("skipped")]

        logger.info(
            f"Batch complete — {len(ok)}/{total} succeeded, "
            f"{len(err)} failed, {len(skipped)} skipped."
        )
        return results

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_campaigns_row(
        cls,
        row: dict,
        acolite_executable: str,
        base_output: str,
        inputfile: str,
        time_delta_days: int = 1,
        cloud_cover_max: int = 10,
        **kwargs,
    ) -> "AcoliteConfig":
        """
        Build an AcoliteConfig from a campaigns CSV row.

        Derives the geographic limit from the row's latitud/longitud
        columns with a default 0.1° buffer around the measurement point.

        Parameters
        ----------
        row:
            Dict-like row from the campaigns DataFrame.
            Must contain 'latitud' and 'longitud' keys.
        acolite_executable:
            Path to ACOLITE executable.
        base_output:
            Parent output directory; a sub-directory named after the
            measurement date is created automatically.
        inputfile:
            Path to the matched Sentinel-2 SAFE product.
        time_delta_days:
            Unused here; kept for API consistency with sentinel_pipeline.
        cloud_cover_max:
            Unused here; kept for API consistency with sentinel_pipeline.
        **kwargs:
            Override defaults in the nested config sections. Keys must
            match section attribute names (e.g. glint=GlintConfig(...)).
        """
        lat = float(row["latitud"])
        lon = float(row["longitud"])
        date_str = str(row.get("date", "unknown"))[:10]

        buffer = 0.1
        limit = (lat - buffer, lon - buffer, lat + buffer, lon + buffer)
        output_dir = str(Path(base_output) / date_str)

        io = IOConfig(inputfile=inputfile, output=output_dir, limit=limit)
        return cls(
            acolite_executable=acolite_executable,
            io=io,
            **kwargs,
        )

    def __repr__(self) -> str:
        settings = self.to_settings_dict()
        lines = "\n".join(f"  {k} = {v}" for k, v in settings.items())
        return f"AcoliteConfig(\n{lines}\n)"