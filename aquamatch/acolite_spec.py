"""
acolite_spec.py
===============
Spec-driven configuration for ACOLITE atmospheric correction
and water quality (L2W) product generation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import logging
import subprocess
from dataclasses import dataclass, field
from dataclasses import replace
from enum import Enum
from pathlib import Path
from typing import Optional

from aquamatch.scl_water import build_water_polygon_datacube

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
    output: str
    limit: Optional[tuple[float, float, float, float]] = None
    polygon: Optional[str] = None
    polygon_clip: bool = False

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
    l2w_mask_wave: int = 1600
    l2w_mask_threshold: float = 0.0215
    l2w_mask_cirrus_threshold: float = 0.005
    l2w_mask_smooth: bool = True
    l2w_mask_smooth_sigma: int = 3


@dataclass
class OutputConfig:
    export_geotiff: bool = True
    export_geotiff_coordinates: bool = True
    export_cloud_optimized_geotiff: bool = False
    netcdf_compression: bool = True
    netcdf_compression_level: int = 4
    map_rgb: bool = False
    map_rgb_maxrange: float = 0.15
    output_xy: bool = False
    output_geometry: bool = True
    l2w_export_geotiff: bool = False
    copy_datasets: str = "lon,lat,rhot_*"


@dataclass
class S2Config:
    s2_target_res: int = 10
    merge_tiles: bool = False
    merge_full_tiles: bool = False
    extend_region: bool = False
    geometry_type: str = "grids_footprint"
    geometry_res: int = 60
    blackfill_skip: bool = True
    blackfill_max: float = 1.0
    blackfill_wave: int = 1600

    def validate(self) -> None:
        if self.s2_target_res not in (10, 20, 60):
            raise ValueError(
                f"s2_target_res must be 10, 20, or 60 m, got {self.s2_target_res}."
            )
        if not (0.0 <= self.blackfill_max <= 1.0):
            raise ValueError(
                f"blackfill_max must be in [0, 1], got {self.blackfill_max}."
            )


@dataclass
class DsfConfig:
    dsf_aot_estimate: str = "tiled"
    dsf_spectrum_option: str = "intercept"
    dsf_nbands: int = 2
    dsf_nbands_fit: int = 2
    dsf_filter_rhot: bool = False
    dsf_filter_percentile: int = 50
    dsf_smooth_aot: bool = False
    dsf_fixed_aot: Optional[float] = None
    dsf_aot_most_common_model: bool = True
    dsf_allow_lut_boundaries: bool = False
    dsf_min_tile_aot: float = 0.01
    dsf_max_tile_aot: float = 1.20

    def validate(self) -> None:
        valid_aot = {"tiled", "fixed", "fixed_band"}
        if self.dsf_aot_estimate not in valid_aot:
            raise ValueError(
                f"dsf_aot_estimate must be one of {sorted(valid_aot)}, "
                f"got '{self.dsf_aot_estimate}'."
            )
        valid_spectrum = {"intercept", "darkest", "percentile"}
        if self.dsf_spectrum_option not in valid_spectrum:
            raise ValueError(
                f"dsf_spectrum_option must be one of {sorted(valid_spectrum)}, "
                f"got '{self.dsf_spectrum_option}'."
            )
        if self.dsf_fixed_aot is not None and not (0.0 <= self.dsf_fixed_aot <= 5.0):
            raise ValueError(
                f"dsf_fixed_aot must be in [0, 5], got {self.dsf_fixed_aot}."
            )


@dataclass
class ReprojectConfig:
    reproject_outputs: bool = False
    output_projection_epsg: Optional[int] = None
    output_projection_resolution: Optional[float] = None
    output_projection_resampling_method: str = "bilinear"

    def validate(self) -> None:
        if self.reproject_outputs:
            if self.output_projection_epsg is None:
                raise ValueError(
                    "reproject_outputs=True requires output_projection_epsg to be set."
                )
            valid_methods = {"nearest", "bilinear", "cubic"}
            if self.output_projection_resampling_method not in valid_methods:
                raise ValueError(
                    f"output_projection_resampling_method must be one of "
                    f"{sorted(valid_methods)}, "
                    f"got '{self.output_projection_resampling_method}'."
                )
            if (
                self.output_projection_resolution is not None
                and self.output_projection_resolution <= 0
            ):
                raise ValueError(
                    "output_projection_resolution must be positive, "
                    f"got {self.output_projection_resolution}."
                )


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

    # rioxarray's reproject() can leave '_FillValue' in both .attrs (written
    # for GDAL/CF nodata compatibility) and .encoding (carried over from
    # decoding the source NetCDF). xarray's zarr writer refuses to silently
    # reconcile the two, so drop the stale encoding/attrs here.
    for var in scene_ds.data_vars:
        scene_ds[var].attrs.pop("_FillValue", None)
        scene_ds[var].encoding = {}

    if not datacube_path.exists():
        scene_ds.to_zarr(datacube_path, mode="w", consolidated=False)
    else:
        existing = xr.open_zarr(datacube_path, consolidated=False)
        existing_times = pd.DatetimeIndex(existing.time.values)
        existing.close()
        if date.normalize() in existing_times.normalize():
            if not overwrite_date:
                logger.warning(f"Date {date.date()} already in datacube — skipping.")
                ds.close()
                return datacube_path
        scene_ds.to_zarr(datacube_path, append_dim="time", consolidated=False)

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
        ds[export_vars].chunk(zarr_chunks).to_zarr(
            zarr_path, mode="w", consolidated=False
        )

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


if TYPE_CHECKING:
    from aquamatch.acolite_spec import AcoliteConfig


def expected_outputs(
    output_dir: Path | str,
    acolite_cfg: "AcoliteConfig",
) -> dict[str, Path | None]:
    output_dir = Path(output_dir)
    l2w_enabled = bool(acolite_cfg.l2w.l2w_parameters)

    def _first(pattern: str) -> Path | None:
        matches = sorted(output_dir.glob(pattern))
        return matches[0] if matches else None

    return {
        "l1r": _first("*_L1R.nc"),
        "l2r": _first("*_L2R.nc"),
        "l2w": _first("*_L2W.nc") if l2w_enabled else None,
    }


def is_scene_processed(
    output_dir: Path | str,
    acolite_cfg: "AcoliteConfig",
) -> bool:
    output_dir = Path(output_dir)
    if not output_dir.exists():
        return False

    l2w_enabled = bool(acolite_cfg.l2w.l2w_parameters)
    outputs = expected_outputs(output_dir, acolite_cfg)

    if outputs["l1r"] is None or outputs["l2r"] is None:
        return False

    if l2w_enabled and outputs["l2w"] is None:
        return False

    return True


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
    s2: S2Config = field(default_factory=S2Config)
    dsf: DsfConfig = field(default_factory=DsfConfig)
    reproject: ReprojectConfig = field(default_factory=ReprojectConfig)

    def validate(self) -> None:
        if not Path(self.acolite_executable).expanduser().exists():
            raise FileNotFoundError(
                f"ACOLITE executable not found: {self.acolite_executable}"
            )
        self.io.validate()
        self.s2.validate()
        self.dsf.validate()
        self.reproject.validate()

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
        d["l2w_mask_wave"] = str(self.l2w.l2w_mask_wave)
        d["l2w_mask_threshold"] = str(self.l2w.l2w_mask_threshold)
        d["l2w_mask_cirrus_threshold"] = str(self.l2w.l2w_mask_cirrus_threshold)
        d["l2w_mask_smooth"] = str(self.l2w.l2w_mask_smooth).lower()
        d["l2w_mask_smooth_sigma"] = str(self.l2w.l2w_mask_smooth_sigma)

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
        d["output_xy"] = str(self.output_format.output_xy).lower()
        d["output_geometry"] = str(self.output_format.output_geometry).lower()
        d["l2w_export_geotiff"] = str(self.output_format.l2w_export_geotiff).lower()
        d["copy_datasets"] = self.output_format.copy_datasets

        d["s2_target_res"] = str(self.s2.s2_target_res)
        d["merge_tiles"] = str(self.s2.merge_tiles).lower()
        d["merge_full_tiles"] = str(self.s2.merge_full_tiles).lower()
        d["extend_region"] = str(self.s2.extend_region).lower()
        d["geometry_type"] = self.s2.geometry_type
        d["geometry_res"] = str(self.s2.geometry_res)
        d["blackfill_skip"] = str(self.s2.blackfill_skip).lower()
        d["blackfill_max"] = str(self.s2.blackfill_max)
        d["blackfill_wave"] = str(self.s2.blackfill_wave)

        d["dsf_aot_estimate"] = self.dsf.dsf_aot_estimate
        d["dsf_spectrum_option"] = self.dsf.dsf_spectrum_option
        d["dsf_nbands"] = str(self.dsf.dsf_nbands)
        d["dsf_nbands_fit"] = str(self.dsf.dsf_nbands_fit)
        d["dsf_filter_rhot"] = str(self.dsf.dsf_filter_rhot).lower()
        if self.dsf.dsf_filter_rhot:
            d["dsf_filter_percentile"] = str(self.dsf.dsf_filter_percentile)
        d["dsf_smooth_aot"] = str(self.dsf.dsf_smooth_aot).lower()
        if self.dsf.dsf_fixed_aot is not None:
            d["dsf_fixed_aot"] = str(self.dsf.dsf_fixed_aot)
        d["dsf_aot_most_common_model"] = str(self.dsf.dsf_aot_most_common_model).lower()
        d["dsf_allow_lut_boundaries"] = str(self.dsf.dsf_allow_lut_boundaries).lower()
        d["dsf_min_tile_aot"] = str(self.dsf.dsf_min_tile_aot)
        d["dsf_max_tile_aot"] = str(self.dsf.dsf_max_tile_aot)

        if self.reproject.reproject_outputs:
            d["reproject_outputs"] = "L1R,L2R,L2W"
            if self.reproject.output_projection_epsg is not None:
                d["output_projection_epsg"] = str(self.reproject.output_projection_epsg)
            if self.reproject.output_projection_resolution is not None:
                d["output_projection_resolution"] = str(
                    self.reproject.output_projection_resolution
                )
            d["output_projection_resampling_method"] = (
                self.reproject.output_projection_resampling_method
            )

        return d

    def to_settings_file(self, path) -> Path:
        out = Path(path).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        settings = self.to_settings_dict()
        lines = [f"{k}={v}\n" for k, v in settings.items()]
        out.write_text("".join(lines))

        roi = settings.get("limit") or settings.get("polygon") or "full scene"
        polygon_clip = settings.get("polygon_clip", "false")
        logger.info(
            f"Settings written: {out.name} | ROI={roi} | polygon_clip={polygon_clip}"
        )
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
        tile_config=None,
        skip_existing: bool = True,
    ) -> list[dict]:
        from aquamatch.scl_water import resolve_scl_path
        from aquamatch.sentinel_data import _tile_from_scene_id

        if not Path(self.acolite_executable).expanduser().exists():
            raise FileNotFoundError(
                f"ACOLITE executable not found: {self.acolite_executable}"
            )

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
                        "skipped_existing": False,
                        "scl_used": False,
                        "tile_restriction": "none",
                    }
                )
                continue

            if skip_existing:
                image_output_check = base_output / stem
                if is_scene_processed(image_output_check, self):
                    logger.info(
                        f"  [{idx}/{total}] Already processed, skipping: {stem}"
                    )
                    results.append(
                        {
                            "returncode": None,
                            "log_file": None,
                            "l2w_file": None,
                            "stdout": "",
                            "stderr": "",
                            "inputfile": str(safe_path),
                            "output_dir": base_output / stem,
                            "skipped": False,
                            "skipped_existing": True,
                            "scl_used": False,
                            "tile_restriction": "none",
                        }
                    )
                    continue

            image_output = base_output / stem
            image_output.mkdir(parents=True, exist_ok=True)

            tile_polygon = None
            tile_limit = None
            tile_restriction = "none"

            if tile_config is not None:
                tile_id = _tile_from_scene_id(stem)
                if tile_id is not None:
                    entry = tile_config.get(tile_id)
                    if entry is not None:
                        if entry.polygon is not None:
                            tile_polygon = entry.polygon
                            tile_restriction = "polygon"
                            logger.info(
                                f"  Tile {tile_id}: static polygon → {entry.polygon}"
                            )
                        elif entry.limit is not None:
                            tile_limit = tuple(entry.limit)
                            tile_restriction = "limit"
                            logger.info(f"  Tile {tile_id}: limit → {entry.limit}")
                        else:
                            logger.info(
                                f"  Tile {tile_id}: no restriction configured "
                                "— full scene."
                            )
                    else:
                        logger.info(
                            f"  Tile {tile_id}: not listed in tile_config "
                            "— full scene."
                        )
                else:
                    logger.warning(
                        f"  Could not extract tile ID from {stem} — "
                        "processing full scene."
                    )

            original_io = self.io
            self.io = replace(
                self.io,
                inputfile=str(safe_path),
                output=str(image_output),
                limit=tile_limit if tile_limit is not None else original_io.limit,
                polygon=(
                    tile_polygon if tile_polygon is not None else original_io.polygon
                ),
                polygon_clip=(
                    tile_polygon is not None
                    or (
                        tile_limit is None
                        and original_io.polygon is not None
                        and original_io.polygon_clip
                    )
                ),
            )

            scl_used = False
            try:
                if use_scl and tile_polygon is None:
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
                            tile_restriction = "polygon"
                            logger.info(
                                f"  SCL polygon clipping enabled: {scl_path.name}"
                            )
                        except Exception as e:
                            logger.warning(
                                f"  SCL extraction failed for {stem}: {e} — "
                                "processing without polygon clipping."
                            )
                elif use_scl and tile_polygon is not None:
                    logger.info(
                        f"  SCL clipping suppressed for {stem} — "
                        "static tile polygon takes precedence."
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
                            "skipped_existing": False,
                            "scl_used": scl_used,
                            "tile_restriction": tile_restriction,
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
                            "skipped_existing": False,
                            "scl_used": scl_used,
                            "tile_restriction": tile_restriction,
                        }
                    )
                    continue

                result = self._execute(settings_path)
                result["skipped"] = False
                result["skipped_existing"] = False
                result["scl_used"] = scl_used
                result["tile_restriction"] = tile_restriction
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
        time_delta=1,
        cloud_cover=10,
        tile_config=None,
        **kwargs,
    ):
        lat = float(row["latitud"])
        lon = float(row["longitud"])
        date_str = str(row.get("date", "unknown"))[:10]
        output_dir = str(Path(base_output) / date_str)

        polygon = None
        polygon_clip = False
        limit = None

        if tile_config is not None:
            tile_id = row.get("s2_tile")
            if tile_id is not None:
                entry = tile_config.get(str(tile_id))
                if entry is not None:
                    if entry.polygon is not None:
                        polygon = entry.polygon
                        polygon_clip = True
                    elif entry.limit is not None:
                        limit = tuple(entry.limit)
            else:
                logger.warning(
                    "from_campaigns_row: 's2_tile' not found in row — "
                    "processing full scene (no spatial restriction)."
                )
        else:
            buffer = 0.1
            limit = (lat - buffer, lon - buffer, lat + buffer, lon + buffer)

        io = IOConfig(
            inputfile=inputfile,
            output=output_dir,
            limit=limit,
            polygon=polygon,
            polygon_clip=polygon_clip,
        )
        return cls(acolite_executable=acolite_executable, io=io, **kwargs)

    @classmethod
    def low_memory(cls, acolite_executable: str, **kwargs) -> "AcoliteConfig":
        return cls(
            acolite_executable=acolite_executable,
            radcor=RadCorConfig(
                dsf_tile_dimensions=(60, 60),
                dsf_path_reflectance="tiled",
            ),
            l2w=L2WConfig(
                output_rhorc=False,
                output_rhos=True,
                l2w_mask_water_expr="rhos_1600 < 0.0215",
            ),
            output_format=OutputConfig(
                export_cloud_optimized_geotiff=False,
                map_rgb=False,
                netcdf_compression=True,
                netcdf_compression_level=2,
            ),
            s2=S2Config(
                s2_target_res=20,
            ),
            **kwargs,
        )

    def __repr__(self) -> str:
        settings = self.to_settings_dict()
        lines = "\n".join(f"  {k} = {v}" for k, v in settings.items())
        return f"AcoliteConfig(\n{lines}\n)"


# ---------------------------------------------------------------------------
# Shared post-processing step: water polygon datacube (GeoPackage)
# ---------------------------------------------------------------------------


def _run_polygon_datacube_step(
    scl_dir: "Path | str",
    output_path: "Path | str",
    overwrite: bool,
    scl_kwargs: dict,
) -> dict:
    """
    Build (or update) the water polygon GeoPackage datacube from all SCL
    GeoTIFF files found in ``scl_dir``.

    This is the single shared implementation used by both
    ``PipelineConfig._run_polygon_datacube()`` (YAML-driven pipeline) and
    ``run_acolite_pipeline()`` (direct/programmatic usage), so the two
    entry points can never drift out of sync.

    Records are built automatically by globbing ``{scl_dir}/*_SCL.tif``.

    Parameters
    ----------
    scl_dir:
        Directory containing SCL GeoTIFF files (typically ``{output_dir}/scl/``).
    output_path:
        Destination GeoPackage file (``.gpkg``).
    overwrite:
        If ``True``, delete any existing GeoPackage and rebuild from scratch.
    scl_kwargs:
        Forwarded to ``build_water_polygon_datacube`` (``min_area_m2``,
        ``simplify_tolerance``, ``buffer_m``).

    Returns
    -------
    dict
        ``{"status": "skipped", "reason": ...}`` when no SCL files are found,
        otherwise ``{"status": "ok", "output_path": str, "n_records": int}``.
        Raises whatever ``build_water_polygon_datacube`` raises — callers are
        expected to handle/propagate failures themselves.
    """
    scl_dir = Path(scl_dir)
    output_path = Path(output_path)
    scl_files = sorted(scl_dir.glob("*_SCL.tif"))

    if not scl_files:
        logger.warning(
            f"  No SCL files found in {scl_dir} — " "water polygon datacube not built."
        )
        return {"status": "skipped", "reason": f"No SCL files in {scl_dir}"}

    records = [{"scl_path": str(f)} for f in scl_files]

    logger.info(
        f"  Building water polygon datacube from {len(records)} SCL files "
        f"→ {output_path}"
    )

    gpkg_path = build_water_polygon_datacube(
        records=records,
        output_path=output_path,
        overwrite=overwrite,
        **scl_kwargs,
    )

    logger.info(f"  Water polygon datacube written: {gpkg_path}")
    return {
        "status": "ok",
        "output_path": str(gpkg_path),
        "n_records": len(records),
    }


# ---------------------------------------------------------------------------
# Public pipeline wrapper
# ---------------------------------------------------------------------------


def run_acolite_pipeline(
    acolite_executable: "str | None" = None,
    safe_dir: "Path | str | None" = None,
    output: "Path | str | None" = None,
    scl_dir: "Path | str | None" = None,
    use_scl: "bool | None" = None,
    skip_existing: "bool | None" = None,
    continue_on_error: "bool | None" = None,
    tile_config: "Optional[object]" = None,
    scl_kwargs: "dict | None" = None,
    acolite_config: "Optional[AcoliteConfig]" = None,
    dry_run: bool = False,
    limit: "Optional[tuple[float, float, float, float]]" = None,
    polygon: "Optional[str]" = None,
    build_polygon_datacube: "bool | None" = None,
    polygon_datacube_path: "Path | str | None" = None,
    polygon_datacube_overwrite: "bool | None" = None,
) -> dict:
    import time
    from aquamatch.pipeline_config import (
        AcoliteSection,
        AcoliteIOSection,
        SclSection,
        TilesSection,
    )

    if limit is not None and polygon is not None:
        raise ValueError("Specify either 'limit' or 'polygon', not both.")
    if (limit is not None or polygon is not None) and tile_config is not None:
        raise ValueError(
            "Specify either 'limit'/'polygon' (global restriction) or "
            "'tile_config' (per-tile restriction), not both."
        )

    _a = AcoliteSection()
    _io = AcoliteIOSection()
    _scl = SclSection()

    safe_dir_path = Path(safe_dir) if safe_dir is not None else Path(_io.safe_dir)
    output_path = Path(output) if output is not None else Path(_io.output)
    scl_dir_path = Path(scl_dir) if scl_dir is not None else Path(_io.scl_dir)
    _use_scl = use_scl if use_scl is not None else _scl.use_scl
    _skip_existing = skip_existing if skip_existing is not None else _a.skip_existing
    _continue_on_error = (
        continue_on_error if continue_on_error is not None else _a.continue_on_error
    )
    _tile_config = tile_config if tile_config is not None else TilesSection()

    # --- Water polygon datacube defaults (Gap A / feature parity with
    # PipelineConfig._run_polygon_datacube via SclSection) ---
    _build_polygon_datacube = (
        build_polygon_datacube
        if build_polygon_datacube is not None
        else _scl.build_polygon_datacube
    )
    _polygon_datacube_path = (
        Path(polygon_datacube_path)
        if polygon_datacube_path is not None
        else Path(_scl.polygon_datacube_path)
    )
    _polygon_datacube_overwrite = (
        polygon_datacube_overwrite
        if polygon_datacube_overwrite is not None
        else _scl.polygon_datacube_overwrite
    )

    if limit is not None:
        _global_limit = tuple(limit)
        _global_polygon = None
    elif polygon is not None:
        _global_limit = None
        _global_polygon = str(polygon)
    else:
        _global_limit = None
        _global_polygon = None

    _scl_kwargs = (
        scl_kwargs
        if scl_kwargs is not None
        else {
            "min_area_m2": _scl.min_area_m2,
            "simplify_tolerance": _scl.simplify_tolerance,
            "buffer_m": _scl.buffer_m,
        }
    )

    t0 = time.monotonic()

    try:
        if acolite_config is not None:
            cfg = acolite_config
        else:
            _executable = (
                acolite_executable
                if acolite_executable is not None
                else _a.acolite_executable
            )
            cfg = AcoliteConfig(
                acolite_executable=_executable,
                io=IOConfig(inputfile="", output=str(output_path)),
            )

        if _global_limit is not None or _global_polygon is not None:
            cfg.io = replace(
                cfg.io,
                limit=_global_limit,
                polygon=_global_polygon,
                polygon_clip=_global_polygon is not None,
            )

        cfg.s2.validate()
        cfg.dsf.validate()
        cfg.reproject.validate()

        safe_list = sorted(safe_dir_path.rglob("*.SAFE"))
        if not safe_list:
            logger.warning(f"No .SAFE folders found in {safe_dir_path}")

        results = cfg.run_batch(
            safe_list=safe_list,
            base_output=output_path,
            dry_run=dry_run,
            use_scl=_use_scl,
            scl_dir=scl_dir_path if _use_scl else None,
            scl_kwargs=_scl_kwargs,
            continue_on_error=_continue_on_error,
            tile_config=_tile_config,
            skip_existing=_skip_existing,
        )

        n_success = sum(1 for r in results if r.get("returncode") == 0)
        n_skipped = sum(
            1 for r in results if r.get("skipped") or r.get("skipped_existing")
        )
        n_error = sum(1 for r in results if r.get("returncode") not in (0, None))

        # --- Water polygon datacube (optional) ---
        # Runs regardless of whether SAFE files were found — the datacube
        # may already contain scenes from previous runs. Mirrors
        # PipelineConfig._run_acolite()'s sub-step 4b via the shared
        # _run_polygon_datacube_step() helper (see Gap A / A1). Any
        # exception raised here is intentionally left uncaught so it
        # propagates to the except block below and the whole call reports
        # status="error" — the same severity as an ACOLITE sub-config
        # validation failure.
        if _build_polygon_datacube:
            logger.info("Building water polygon datacube...")
            polygon_datacube_result = _run_polygon_datacube_step(
                scl_dir=scl_dir_path,
                output_path=_polygon_datacube_path,
                overwrite=_polygon_datacube_overwrite,
                scl_kwargs=_scl_kwargs,
            )
        else:
            polygon_datacube_result = {
                "status": "skipped",
                "reason": "build_polygon_datacube=False",
            }

        return {
            "step": "acolite",
            "status": "success",
            "outputs": {
                "safe_dir": safe_dir_path,
                "output": output_path,
                "n_scenes": len(safe_list),
                "n_success": n_success,
                "n_skipped": n_skipped,
                "n_error": n_error,
                "scenes": results,
                "polygon_datacube": polygon_datacube_result,
            },
            "error": None,
            "elapsed_seconds": round(time.monotonic() - t0, 2),
        }

    except Exception as exc:
        logger.error(f"run_acolite_pipeline failed: {exc}")
        return {
            "step": "acolite",
            "status": "error",
            "outputs": {},
            "error": str(exc),
            "elapsed_seconds": round(time.monotonic() - t0, 2),
        }


def _with_scl_polygon(
    self,
    scl_path,
    geojson_output_dir=None,
    overwrite=False,
    **scl_kwargs,
):
    from aquamatch.scl_water import scl_water_to_geojson, GEOJSON_SUBDIR

    scl_path = Path(scl_path)

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
    logger.info(f"with_scl_polygon: polygon written → {geojson_path.name}")
    logger.info(
        f"with_scl_polygon: polygon_clip=True | limit cleared | "
        f"polygon={geojson_path}"
    )

    new_io = replace(self.io, polygon=str(geojson_path), polygon_clip=True, limit=None)
    return replace(self, io=new_io)


AcoliteConfig.with_scl_polygon = _with_scl_polygon


def _build_acolite_parser() -> "argparse.ArgumentParser":
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m aquamatch.acolite_spec",
        description=(
            "Run ACOLITE atmospheric correction.\n\n"
            "Two modes:\n"
            "  --config  Load a full pipeline YAML and run only the ACOLITE step.\n"
            "  (flags)   Build the configuration from explicit command-line arguments."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    mode_group = parser.add_mutually_exclusive_group(required=False)
    mode_group.add_argument(
        "--config",
        metavar="YAML",
        help="Path to a pipeline YAML config file.",
    )

    parser.add_argument(
        "--executable",
        metavar="PATH",
        default=None,
        help="Path to the ACOLITE executable.",
    )
    parser.add_argument(
        "--safe-dir",
        metavar="DIR",
        default=None,
        help=f"Directory to search for .SAFE folders. Default: {AcoliteIOSection_DEFAULT_SAFE_DIR}",
    )
    parser.add_argument(
        "--output",
        metavar="DIR",
        default=None,
        help=f"Root output directory. Default: {AcoliteIOSection_DEFAULT_OUTPUT}",
    )
    parser.add_argument(
        "--scl-dir",
        metavar="DIR",
        default=None,
        help=f"Directory containing SCL GeoTIFFs. Default: {AcoliteIOSection_DEFAULT_SCL_DIR}",
    )
    parser.add_argument(
        "--use-scl",
        action="store_true",
        default=False,
        help="Extract SCL water polygon per scene and apply polygon clipping.",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        default=False,
        help="Reprocess all scenes even if output files already exist.",
    )
    parser.add_argument(
        "--no-continue-on-error",
        action="store_true",
        default=False,
        help="Stop processing on first scene failure.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Log what would be executed without calling the ACOLITE binary.",
    )

    return parser


def _get_io_defaults():
    from aquamatch.pipeline_config import AcoliteIOSection

    _io = AcoliteIOSection()
    return _io.safe_dir, _io.output, _io.scl_dir


try:
    (
        AcoliteIOSection_DEFAULT_SAFE_DIR,
        AcoliteIOSection_DEFAULT_OUTPUT,
        AcoliteIOSection_DEFAULT_SCL_DIR,
    ) = _get_io_defaults()
except Exception:
    AcoliteIOSection_DEFAULT_SAFE_DIR = "data/sentinel_downloads"
    AcoliteIOSection_DEFAULT_OUTPUT = "data/acolite_output"
    AcoliteIOSection_DEFAULT_SCL_DIR = "data/sentinel_downloads/scl"


if __name__ == "__main__":
    import argparse
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    _parser = _build_acolite_parser()
    _args = _parser.parse_args()

    if _args.config:
        from aquamatch.pipeline_config import PipelineConfig

        _cfg = PipelineConfig.from_yaml(_args.config)

        if _args.dry_run:
            logger.info("[dry_run] YAML mode — logging mode without executing.")
        if _args.no_skip_existing:
            _cfg.acolite.skip_existing = False

        _results = _cfg._run_acolite()
        _n_ok = sum(1 for r in _results if r.get("returncode") == 0)
        _n_total = len(_results)
        logger.info(f"ACOLITE complete — {_n_ok}/{_n_total} scenes succeeded.")

    else:
        if _args.executable is None:
            _parser.error(
                "--executable is required when --config is not supplied.\n"
                "Provide the path to your ACOLITE executable, e.g.:\n"
                "  --executable /path/to/acolite/acolite.py"
            )

        _result = run_acolite_pipeline(
            acolite_executable=_args.executable,
            safe_dir=_args.safe_dir,
            output=_args.output,
            scl_dir=_args.scl_dir,
            use_scl=_args.use_scl,
            skip_existing=not _args.no_skip_existing,
            continue_on_error=not _args.no_continue_on_error,
            dry_run=_args.dry_run,
        )

        if _result["status"] != "success":
            logger.error(f"Pipeline failed: {_result['error']}")
        else:
            _out = _result["outputs"]
            logger.info(
                f"ACOLITE complete — "
                f"{_out['n_success']}/{_out['n_scenes']} scenes succeeded, "
                f"{_out['n_skipped']} skipped, "
                f"{_out['n_error']} errors. "
                f"({_result['elapsed_seconds']}s)"
            )
