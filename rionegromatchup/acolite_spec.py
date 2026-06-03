"""
acolite_spec.py
===============
Spec-driven configuration for ACOLITE atmospheric correction
and water quality (L2W) product generation.
"""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass, field
from dataclasses import replace
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class AcoliteAtmosphericProcessor(str, Enum):
    DSF = "dsf"
    EXP = "exp"
    TACT = "tact"


class AcoliteGlintCorrection(str, Enum):
    NONE = "none"
    HEDLEY = "hedley"
    VANHELLEMONT = "vanhellemont2019"


class AcoliteSurfaceReflectance(str, Enum):
    RHO_S = "rhos"
    RHO_RC = "rhorc"


@dataclass
class IOConfig:
    """Input / Output and Region of Interest (ROI) parameters."""

    inputfile: str
    """Full path to the input scene directory or SAFE file."""

    output: str
    """Directory where all generated products will be saved."""

    limit: Optional[tuple[float, float, float, float]] = None
    """
    Geographic bounding box as (south, west, north, east) in decimal degrees.
    Mutually exclusive with `polygon`.
    """

    polygon: Optional[str] = None
    """
    Path to a GeoJSON or WKT file defining a non-rectangular ROI.
    Mutually exclusive with `limit`.
    """

    polygon_clip: bool = False
    """
    If True, ACOLITE restricts processing to pixels inside the polygon
    boundary (sets polygon_clip=true in the settings file).
    Requires a valid ``polygon`` path — validated at run time.
    """

    def validate(self) -> None:
        if self.limit is not None and self.polygon is not None:
            raise ValueError("Specify either `limit` or `polygon`, not both.")
        if self.polygon_clip and not self.polygon:
            raise ValueError(
                "polygon_clip=True requires a valid polygon path. "
                "Set IOConfig.polygon to a GeoJSON or WKT file path."
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
    tact_run: bool = False
    tact_emissivity: float = 0.985
    tact_reanalysis: str = "era5"


@dataclass
class RadCorConfig:
    aerosol_correction: AcoliteAtmosphericProcessor = AcoliteAtmosphericProcessor.DSF
    dsf_path_reflectance: str = "tiled"
    dsf_tile_dimensions: tuple[int, int] = (120, 120)
    dsf_minimum_tile_cover: float = 0.10
    ancillary_data: bool = True
    uoz: float = 0.3
    uwv: float = 1.5
    pressure: float = 1013.25


@dataclass
class GlintConfig:
    glint_correction: bool = True
    glint_method: AcoliteGlintCorrection = AcoliteGlintCorrection.VANHELLEMONT
    glint_threshold: float = 0.01
    glint_mask_rhos: bool = True
    glint_mask_rhos_threshold: float = 0.15


@dataclass
class L2WConfig:
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
class OutputConfig:
    export_geotiff: bool = True
    export_geotiff_coordinates: bool = True
    export_cloud_optimized_geotiff: bool = False
    netcdf_compression: bool = True
    netcdf_compression_level: int = 4
    map_rgb: bool = False
    map_rgb_maxrange: float = 0.15


# ---------------------------------------------------------------------------
# Post-processing helpers (unchanged from original)
# ---------------------------------------------------------------------------


def _parse_date_from_l2w(l2w_nc: Path) -> "pd.Timestamp":
    import re
    import pandas as pd

    match = re.search(r"_(\d{8})_", l2w_nc.name)
    if match:
        return pd.Timestamp(match.group(1))

    match = re.search(r"_(\d{4})_(\d{2})_(\d{2})_\d{2}_\d{2}_\d{2}_", l2w_nc.name)
    if match:
        year, month, day = match.group(1), match.group(2), match.group(3)
        return pd.Timestamp(f"{year}-{month}-{day}")

    raise ValueError(f"Could not parse acquisition date from filename '{l2w_nc.name}'.")


def append_l2w_to_datacube(
    l2w_nc,
    datacube_path,
    target_crs="EPSG:4326",
    target_resolution=0.0001,
    variables=None,
    zarr_chunks=None,
    overwrite_date=False,
):
    try:
        import xarray as xr
        import rioxarray
        import zarr
        import numpy as np
        import pandas as pd
    except ImportError as e:
        raise ImportError(f"Requires xarray, rioxarray, and zarr.\n{e}") from e

    l2w_nc = Path(l2w_nc)
    datacube_path = Path(datacube_path)

    if not l2w_nc.exists():
        raise FileNotFoundError(f"L2W NetCDF not found: {l2w_nc}")

    GRID_MAPPING_NAMES = {
        "transverse_mercator",
        "polar_stereographic",
        "lambert_conformal_conic",
        "spatial_ref",
        "crs",
        "grid_mapping",
    }

    date = _parse_date_from_l2w(l2w_nc)
    logger.info(f"Appending scene dated {date.date()} from {l2w_nc.name}")

    ds = xr.open_dataset(l2w_nc, decode_coords="all")
    data_vars = [
        v for v in ds.data_vars if v not in GRID_MAPPING_NAMES and ds[v].ndim >= 2
    ]
    if variables is not None:
        data_vars = [v for v in variables if v in data_vars]
    if not data_vars:
        raise ValueError(f"No exportable variables found in {l2w_nc.name}.")

    ds = ds[data_vars]
    reprojected = {}
    for var in data_vars:
        da = ds[var]
        x_dim = next((d for d in da.dims if d in ("x", "lon", "longitude")), None)
        y_dim = next((d for d in da.dims if d in ("y", "lat", "latitude")), None)
        if x_dim is None or y_dim is None:
            continue
        da = da.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim)
        if da.rio.crs is None:
            continue
        reprojected[var] = da.rio.reproject(target_crs, resolution=target_resolution)

    if not reprojected:
        raise ValueError(f"No variables could be reprojected from {l2w_nc.name}.")

    scene_ds = xr.Dataset(reprojected)
    rename_map = {}
    for dim in scene_ds.dims:
        if dim in ("lon", "longitude"):
            rename_map[dim] = "x"
        elif dim in ("lat", "latitude"):
            rename_map[dim] = "y"
    if rename_map:
        scene_ds = scene_ds.rename(rename_map)

    scene_ds = scene_ds.expand_dims(dim={"time": [date.to_datetime64()]})
    scene_ds = scene_ds.astype(np.float32)
    chunks = zarr_chunks or {"time": 1, "y": 512, "x": 512}
    scene_ds = scene_ds.chunk(chunks)

    if not datacube_path.exists():
        scene_ds.to_zarr(datacube_path, mode="w")
    else:
        existing = xr.open_zarr(datacube_path)
        existing_times = pd.DatetimeIndex(existing.time.values)
        existing.close()
        if date.normalize() in existing_times.normalize():
            if not overwrite_date:
                logger.warning(f"Date {date.date()} already in datacube — skipping.")
                ds.close()
                return datacube_path
        scene_ds.to_zarr(datacube_path, append_dim="time")

    ds.close()
    return datacube_path


def convert_l2w_to_zarr_cog(
    l2w_nc,
    output_dir,
    variables=None,
    zarr_chunks=None,
    cog_overview_levels=None,
    overwrite=False,
):
    try:
        import xarray as xr
        import rioxarray
        import zarr
        import rasterio
        from rasterio.enums import Resampling
    except ImportError as e:
        raise ImportError(f"Requires xarray, rioxarray, zarr, rasterio.\n{e}") from e

    l2w_nc = Path(l2w_nc)
    output_dir = Path(output_dir)
    if not l2w_nc.exists():
        raise FileNotFoundError(f"L2W NetCDF not found: {l2w_nc}")

    output_dir.mkdir(parents=True, exist_ok=True)
    zarr_chunks = zarr_chunks or {"x": 512, "y": 512}
    cog_overview_levels = cog_overview_levels or [2, 4, 8, 16]

    GRID_MAPPING_NAMES = {
        "transverse_mercator",
        "polar_stereographic",
        "lambert_conformal_conic",
        "spatial_ref",
        "crs",
        "grid_mapping",
    }

    ds = xr.open_dataset(l2w_nc, decode_coords="all")
    available = [
        v for v in ds.data_vars if v not in GRID_MAPPING_NAMES and ds[v].ndim >= 2
    ]
    export_vars = [v for v in variables if v in available] if variables else available

    if not export_vars:
        raise ValueError("No exportable variables found.")

    zarr_path = output_dir / (l2w_nc.stem + ".zarr")
    if not (zarr_path.exists() and not overwrite):
        if zarr_path.exists():
            import shutil

            shutil.rmtree(zarr_path)
        ds[export_vars].chunk(zarr_chunks).to_zarr(zarr_path, mode="w")

    cog_paths = []
    for var in export_vars:
        cog_path = output_dir / f"{l2w_nc.stem}_{var}.tif"
        if cog_path.exists() and not overwrite:
            cog_paths.append(cog_path)
            continue

        da = ds[var]
        x_dim = next((d for d in da.dims if d in ("x", "lon", "longitude")), None)
        y_dim = next((d for d in da.dims if d in ("y", "lat", "latitude")), None)
        if x_dim is None or y_dim is None:
            continue

        da = da.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim)
        if da.rio.crs is None:
            continue

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

        cog_paths.append(cog_path)

    ds.close()
    return zarr_path, cog_paths


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------


@dataclass
class AcoliteConfig:
    """Master ACOLITE configuration."""

    acolite_executable: str

    io: IOConfig = field(default_factory=lambda: IOConfig(inputfile="", output=""))
    radcor: RadCorConfig = field(default_factory=RadCorConfig)
    tact: TACTConfig = field(default_factory=TACTConfig)
    glint: GlintConfig = field(default_factory=GlintConfig)
    l2w: L2WConfig = field(default_factory=L2WConfig)
    output_format: OutputConfig = field(default_factory=OutputConfig)

    def validate(self) -> None:
        if not Path(self.acolite_executable).expanduser().exists():
            raise FileNotFoundError(
                f"ACOLITE executable not found: {self.acolite_executable}"
            )
        self.io.validate()

        if (
            self.tact.tact_run
            and self.radcor.aerosol_correction != AcoliteAtmosphericProcessor.TACT
        ):
            import warnings

            warnings.warn(
                "tact_run=True but aerosol_correction is not 'tact'.",
                stacklevel=2,
            )

        if self.output_format.netcdf_compression_level not in range(1, 10):
            raise ValueError(
                "netcdf_compression_level must be between 1 and 9, "
                f"got {self.output_format.netcdf_compression_level}."
            )

    def to_settings_dict(self) -> dict[str, str]:
        d: dict[str, str] = {}

        d["inputfile"] = self.io.inputfile
        d["output"] = self.io.output
        if self.io.limit is not None:
            s, w, n, e = self.io.limit
            d["limit"] = f"{s},{w},{n},{e}"
        if self.io.polygon is not None:
            d["polygon"] = self.io.polygon
        if self.io.polygon_clip:
            d["polygon_clip"] = "true"

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

        d["tact_run"] = str(self.tact.tact_run).lower()
        if self.tact.tact_run:
            d["tact_emissivity"] = str(self.tact.tact_emissivity)
            d["tact_reanalysis"] = self.tact.tact_reanalysis

        d["glint_correction"] = str(self.glint.glint_correction).lower()
        if self.glint.glint_correction:
            d["glint_method"] = self.glint.glint_method.value
            d["glint_threshold"] = str(self.glint.glint_threshold)
            d["glint_mask_rhos"] = str(self.glint.glint_mask_rhos).lower()
            if self.glint.glint_mask_rhos:
                d["glint_mask_rhos_threshold"] = str(
                    self.glint.glint_mask_rhos_threshold
                )

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

    def to_settings_file(self, path) -> Path:
        out = Path(path).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        settings = self.to_settings_dict()
        lines = [f"{k}={v}\n" for k, v in settings.items()]
        out.write_text("".join(lines))
        return out

    def _execute(self, settings_path: Path) -> dict:
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
            "log_file": log_files[-1] if log_files else None,
            "l2w_file": l2w_files[-1] if l2w_files else None,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "inputfile": self.io.inputfile,
            "output_dir": output_dir,
        }

    def run(self, dry_run: bool = False) -> dict:
        self.validate()
        output_dir = Path(self.io.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        settings_path = self.to_settings_file(output_dir / "acolite_settings.txt")

        if dry_run:
            cmd = [
                str(Path(self.acolite_executable).expanduser().resolve()),
                "--cli",
                f"--settings={settings_path}",
            ]
            logger.info(f"[dry_run] Command: {' '.join(cmd)}")
            return {
                "returncode": None,
                "log_file": None,
                "l2w_file": None,
                "stdout": "",
                "stderr": "",
                "inputfile": self.io.inputfile,
                "output_dir": output_dir,
            }

        return self._execute(settings_path)

    def run_batch(
        self,
        safe_list,
        base_output,
        dry_run=False,
        continue_on_error=True,
        use_scl=False,
        scl_dir=None,
        scl_kwargs=None,
    ) -> list[dict]:
        """
        Run ACOLITE for each SAFE folder in safe_list.

        Parameters
        ----------
        safe_list:
            List of paths to Sentinel-2 SAFE folders.
        base_output:
            Parent output directory; per-image subdirectories are created
            automatically: ``<base_output>/<SAFE_stem>/``.
        dry_run:
            If True, log commands without executing.
        continue_on_error:
            If True (default), log errors and continue on failure.
        use_scl:
            If True, automatically resolve the SCL file for each scene,
            extract water polygons, and apply polygon clipping.
            Requires ``scl_dir`` to be set — raises ``ValueError`` immediately
            if ``scl_dir`` is None.
        scl_dir:
            Directory containing SCL GeoTIFF files (typically
            ``{output_dir}/scl/``).  Required when ``use_scl=True``.
        scl_kwargs:
            Optional dict of keyword arguments forwarded to
            ``with_scl_polygon()`` / ``scl_water_to_geojson()``
            (e.g. ``{"min_area_m2": 10000, "buffer_m": 30}``).

        Returns
        -------
        list[dict]
            One result dict per image.  Each dict includes a ``scl_used``
            key (bool) indicating whether polygon clipping was applied.
        """
        from rionegromatchup.scl_water import resolve_scl_path

        if not Path(self.acolite_executable).expanduser().exists():
            raise FileNotFoundError(
                f"ACOLITE executable not found: {self.acolite_executable}"
            )

        # Raise early — don't let every scene fail silently
        if use_scl and scl_dir is None:
            raise ValueError(
                "use_scl=True requires scl_dir to be set. "
                "Pass the directory containing SCL GeoTIFF files "
                "(typically {output_dir}/scl/)."
            )

        scl_dir = Path(scl_dir) if scl_dir is not None else None
        scl_kwargs = scl_kwargs or {}
        base_output = Path(base_output)
        results = []
        total = len(safe_list)

        for idx, safe_path in enumerate(safe_list, start=1):
            safe_path = Path(safe_path)
            stem = safe_path.stem
            logger.info(f"[{idx}/{total}] Processing: {stem}")

            if not safe_path.exists():
                logger.warning(f"  SAFE folder not found, skipping: {safe_path}")
                results.append(
                    {
                        "returncode": None,
                        "log_file": None,
                        "l2w_file": None,
                        "stdout": "",
                        "stderr": f"Input not found: {safe_path}",
                        "inputfile": str(safe_path),
                        "output_dir": None,
                        "skipped": True,
                        "scl_used": False,
                    }
                )
                continue

            image_output = base_output / stem
            image_output.mkdir(parents=True, exist_ok=True)

            # Reset polygon state at the start of each scene to prevent
            # state bleed from the previous iteration
            original_io = self.io
            self.io = replace(
                self.io,
                inputfile=str(safe_path),
                output=str(image_output),
                polygon=None,
                polygon_clip=False,
            )

            scl_used = False
            try:
                # --- Optional SCL polygon clipping ---
                if use_scl:
                    scl_path = resolve_scl_path(safe_path, scl_dir)
                    if scl_path is None:
                        logger.warning(
                            f"  SCL not found for {stem} — "
                            "processing without polygon clipping."
                        )
                    else:
                        try:
                            patched = self.with_scl_polygon(
                                scl_path,
                                geojson_output_dir=image_output / "geojson",
                                **scl_kwargs,
                            )
                            self.io = patched.io
                            scl_used = True
                            logger.info(
                                f"  SCL polygon clipping enabled: {scl_path.name}"
                            )
                        except Exception as e:
                            logger.warning(
                                f"  SCL extraction failed for {stem}: {e} — "
                                "processing without polygon clipping."
                            )

                try:
                    self.io.validate()
                except (FileNotFoundError, ValueError) as e:
                    logger.error(f"  Validation failed for {stem}: {e}")
                    results.append(
                        {
                            "returncode": -1,
                            "log_file": None,
                            "l2w_file": None,
                            "stdout": "",
                            "stderr": str(e),
                            "inputfile": str(safe_path),
                            "output_dir": image_output,
                            "skipped": False,
                            "scl_used": scl_used,
                        }
                    )
                    if not continue_on_error:
                        raise
                    continue

                settings_path = self.to_settings_file(
                    image_output / "acolite_settings.txt"
                )

                if dry_run:
                    results.append(
                        {
                            "returncode": None,
                            "log_file": None,
                            "l2w_file": None,
                            "stdout": "",
                            "stderr": "",
                            "inputfile": str(safe_path),
                            "output_dir": image_output,
                            "skipped": False,
                            "scl_used": scl_used,
                        }
                    )
                    continue

                result = self._execute(settings_path)
                result["skipped"] = False
                result["scl_used"] = scl_used
                results.append(result)

                if result["returncode"] != 0 and not continue_on_error:
                    raise RuntimeError(
                        f"ACOLITE failed for {stem} "
                        f"(returncode={result['returncode']})"
                    )
            finally:
                self.io = original_io

        processed = [r for r in results if not r.get("skipped")]
        ok = [r for r in processed if r["returncode"] == 0]
        err = [r for r in processed if r["returncode"] not in (0, None)]
        skipped = [r for r in results if r.get("skipped")]
        logger.info(
            f"Batch complete — {len(ok)}/{total} succeeded, "
            f"{len(err)} failed, {len(skipped)} skipped."
        )
        return results

    @classmethod
    def from_campaigns_row(
        cls,
        row,
        acolite_executable,
        base_output,
        inputfile,
        time_delta_days=1,
        cloud_cover_max=10,
        **kwargs,
    ):
        lat = float(row["latitud"])
        lon = float(row["longitud"])
        date_str = str(row.get("date", "unknown"))[:10]
        buffer = 0.1
        limit = (lat - buffer, lon - buffer, lat + buffer, lon + buffer)
        output_dir = str(Path(base_output) / date_str)
        io = IOConfig(inputfile=inputfile, output=output_dir, limit=limit)
        return cls(acolite_executable=acolite_executable, io=io, **kwargs)

    def __repr__(self) -> str:
        settings = self.to_settings_dict()
        lines = "\n".join(f"  {k} = {v}" for k, v in settings.items())
        return f"AcoliteConfig(\n{lines}\n)"


# Monkey-patch with_scl_polygon onto AcoliteConfig
# (defined outside the class body to keep the heredoc clean,
#  then assigned as a method below)


def _with_scl_polygon(
    self,
    scl_path,
    geojson_output_dir=None,
    overwrite=False,
    **scl_kwargs,
):
    """
    Extract water polygons from an SCL file and return a new
    ``AcoliteConfig`` with ``polygon`` and ``polygon_clip`` wired up.

    Calls ``scl_water_to_geojson()`` internally to produce the GeoJSON,
    then returns a copy of this config (via ``dataclasses.replace``) with:

    - ``io.polygon``      → path to the generated GeoJSON
    - ``io.polygon_clip`` → ``True``
    - ``io.limit``        → ``None``  (mutually exclusive with polygon)

    The original config is never mutated — safe to call in a loop.

    Parameters
    ----------
    scl_path:
        Path to the SCL GeoTIFF asset for this scene.
    geojson_output_dir:
        Directory where the GeoJSON file will be written.
        Defaults to ``{io.output}/geojson/``.
    overwrite:
        Passed through to ``scl_water_to_geojson``.  If ``False``
        (default) and the GeoJSON already exists, it is reused.
    **scl_kwargs:
        Extra keyword arguments forwarded to ``scl_water_to_geojson``
        (e.g. ``min_area_m2``, ``buffer_m``, ``simplify_tolerance``).

    Returns
    -------
    AcoliteConfig
        New config instance with polygon clipping configured.

    Raises
    ------
    FileNotFoundError
        If ``scl_path`` does not exist.
    ValueError
        If no water pixels are found in the SCL raster.
    """
    from rionegromatchup.scl_water import scl_water_to_geojson, GEOJSON_SUBDIR

    scl_path = Path(scl_path)

    # Derive GeoJSON output directory
    if geojson_output_dir is None:
        geojson_output_dir = Path(self.io.output) / GEOJSON_SUBDIR
    else:
        geojson_output_dir = Path(geojson_output_dir)

    scene_id = scl_path.stem
    geojson_path = geojson_output_dir / f"{scene_id}_water.geojson"

    logger.info(f"with_scl_polygon: extracting water mask from {scl_path.name}")
    scl_water_to_geojson(
        scl_path=scl_path,
        output_path=geojson_path,
        overwrite=overwrite,
        **scl_kwargs,
    )
    logger.info(f"with_scl_polygon: polygon set to {geojson_path}")

    # Return a new config with polygon wired up; clear limit (mutually exclusive)
    new_io = replace(self.io, polygon=str(geojson_path), polygon_clip=True, limit=None)
    return replace(self, io=new_io)


AcoliteConfig.with_scl_polygon = _with_scl_polygon
