"""
acolite_spec.py
===============
Spec-driven configuration for ACOLITE atmospheric correction
and water quality (L2W) product generation.

Each dataclass section maps to a logical group of ACOLITE settings.
All fields carry a docstring, type annotation, and sensible default
so that a minimal run requires only `inputfile`, `output`, and a
geographic limit/polygon.

Usage (direct instantiation)
-----------------------------
    from acolite_spec import AcoliteConfig

    cfg = AcoliteConfig(
        inputfile="data/sentinel_downloads/S2A_MSIL1C_20250801T131031.SAFE",
        output="data/acolite_output",
        limit=(-33.0, -57.0, -32.5, -56.0),   # S, W, N, E
    )
    cfg.validate()
    cfg.to_settings_file("data/acolite_output/acolite_settings.txt")

Usage (from campaigns CSV)
---------------------------
    cfg = AcoliteConfig.from_campaigns_row(row, base_output="data/acolite_output")

"""

from __future__ import annotations

import subprocess
import textwrap
import logging
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class AcoliteAtmosphericProcessor(str, Enum):
    """ACOLITE atmospheric correction processor."""

    DSF = "dsf"  # Dark Spectrum Fitting (default for most sensors)
    EXP = "exp"  # Exponential extrapolation
    TACT = "tact"  # Thermal AtmoCorrection (thermal band correction)


class AcoliteGlintCorrection(str, Enum):
    """Sun-glint correction method."""

    NONE = "none"
    HEDLEY = "hedley"
    VANHELLEMONT = "vanhellemont2019"


class AcoliteSurfaceReflectance(str, Enum):
    """Target surface reflectance type for L2R output."""

    RHO_S = "rhos"  # Surface reflectance (default)
    RHO_RC = "rhorc"  # Rayleigh-corrected reflectance


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
    Accepts a comma-separated list to batch-process multiple images.
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
            raise ValueError("Specify either `limit` or `polygon`, not both.")
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
        if not Path(self.inputfile.split(",")[0].strip()).exists():
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

    l2w_parameters: list[str] = field(
        default_factory=lambda: [
            "t_nechad",  # Turbidity (Nechad et al. 2010)
            "spm_nechad",  # Suspended Particulate Matter
            "chl_oc3",  # Chlorophyll-a (OC3 algorithm)
            "chl_re",  # Chlorophyll-a (Red-Edge algorithm, Sentinel-2 only)
            "aphy_443",  # Phytoplankton absorption at 443 nm
            "fai",  # Floating Algae Index
            "ndwi",  # Normalized Difference Water Index
            "ndvi",  # Normalized Difference Vegetation Index
        ]
    )
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
    Write output GeoTIFFs as Cloud Optimized GeoTIFFs (COGs).
    Recommended when outputs will be served via a tile server.
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
# Top-level config
# ---------------------------------------------------------------------------


@dataclass
class AcoliteConfig:
    """
    Master ACOLITE configuration.

    Aggregates all section configs and exposes helpers for validation
    and serialisation to ACOLITE's key=value settings file format.

    Minimal example
    ---------------
        cfg = AcoliteConfig(
            acolite_executable="~/acolite/acolite.py",
            io=IOConfig(
                inputfile="data/S2A_MSIL1C.SAFE",
                output="data/acolite_out",
                limit=(-33.0, -57.0, -32.5, -56.0),
            ),
        )
        cfg.validate()
        cfg.to_settings_file("data/acolite_out/settings.txt")
    """

    acolite_executable: str
    """
    Full path to the ACOLITE Python entry point (acolite.py) or
    the compiled ACOLITE executable.
    Example: "/opt/acolite/acolite.py"
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

        # TACT + DSF are independent; warn if tact_run=True but aerosol != tact
        if (
            self.tact.tact_run
            and self.radcor.aerosol_correction != AcoliteAtmosphericProcessor.TACT
        ):
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
        d["netcdf_compression_level"] = str(self.output_format.netcdf_compression_level)
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
            Any additional keyword arguments are passed through to
            override defaults in the nested config sections. Keys must
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

    import subprocess
    import re
    from pathlib import Path

    def run(self, dry_run: bool = False) -> dict:
        """
        Execute ACOLITE using the binary and the current configuration.

        1. Validates the config.
        2. Writes a settings file to the output directory.
        3. Calls the binary as a subprocess.
        4. Checks for expected output files.

        Parameters
        ----------
        dry_run:
            If True, prints the command and settings but does not execute.

        Returns
        -------
        dict with keys:
            'returncode'  : int   — 0 means success
            'log_file'    : Path  — path to the ACOLITE log file (if found)
            'l2w_file'    : Path  — path to the L2W NetCDF (if found)
            'stdout'      : str
            'stderr'      : str
        """
        self.validate()

        output_dir = Path(self.io.output)
        output_dir.mkdir(parents=True, exist_ok=True)

        settings_path = self.to_settings_file(output_dir / "acolite_settings.txt")
        logger.info(f"Settings written to {settings_path}")

        cmd = [
            str(Path(self.acolite_executable).expanduser().resolve()),
            "--cli",
            f"--settings={settings_path}",
        ]

        if dry_run:
            logger.info(f"[dry_run] Would execute: {' '.join(cmd)}")
            logger.info(f"[dry_run] Settings:\n{settings_path.read_text()}")
            return {
                "returncode": None,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
            }

        logger.info(f"Running ACOLITE: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        logger.info(result.stdout)
        if result.returncode != 0:
            logger.error(f"ACOLITE exited with code {result.returncode}")
            logger.error(result.stderr)

        # Find outputs
        log_files = sorted(output_dir.glob("acolite_run_*.log"))
        l2w_files = sorted(output_dir.glob("*L2W.nc"))

        return {
            "returncode": result.returncode,
            "log_file": log_files[-1] if log_files else None,
            "l2w_file": l2w_files[-1] if l2w_files else None,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
