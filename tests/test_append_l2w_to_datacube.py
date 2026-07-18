"""
Unit tests for aquamatch.acolite_spec.append_l2w_to_datacube.

Regression coverage for a bug where appending a second scene to an
existing L2W zarr datacube raised:

    ValueError: Key '_FillValue' already exists in attrs, and will not
    be overwritten.

rioxarray's reproject() can return a DataArray whose '_FillValue' ends
up set in both .attrs and .encoding at once. xarray's zarr writer
refuses to reconcile the two itself. append_l2w_to_datacube() must
strip the stale value before calling to_zarr() so this never surfaces
regardless of which scene triggers it.
"""

from pathlib import Path

import numpy as np
import pytest
import rioxarray  # noqa: F401 - registers the .rio accessor
import xarray as xr
from rasterio.crs import CRS

from aquamatch.acolite_spec import append_l2w_to_datacube

TEST_CRS = "EPSG:32721"


def _make_l2w_nc(path: Path, seed: int) -> Path:
    """Write a minimal single-variable NetCDF shaped like an ACOLITE L2W scene."""
    rng = np.random.default_rng(seed)
    ny, nx = 20, 20
    y = np.linspace(6_300_000, 6_300_000 - (ny - 1) * 10, ny)
    x = np.linspace(500_000, 500_000 + (nx - 1) * 10, nx)
    data = (rng.random((ny, nx)) * 10).astype(np.float32)

    da = xr.DataArray(
        data, dims=("y", "x"), coords={"y": y, "x": x}, name="chl_oc3"
    )
    da = da.rio.write_crs(CRS.from_epsg(32721))
    da.to_dataset().to_netcdf(path)
    return path


@pytest.fixture
def force_fillvalue_conflict(monkeypatch):
    """Make every rio.reproject() output carry '_FillValue' in both attrs
    and encoding — the exact state that previously crashed the zarr write."""
    real_reproject = rioxarray.raster_array.RasterArray.reproject

    def fake_reproject(self, *args, **kwargs):
        result = real_reproject(self, *args, **kwargs)
        result.attrs["_FillValue"] = np.float32(np.nan)
        result.encoding["_FillValue"] = np.float32(np.nan)
        return result

    monkeypatch.setattr(
        rioxarray.raster_array.RasterArray, "reproject", fake_reproject
    )


class TestAppendL2wToDatacubeFillValueConflict:
    def test_second_scene_append_does_not_raise(
        self, tmp_path, force_fillvalue_conflict
    ):
        nc1 = _make_l2w_nc(
            tmp_path / "S2A_MSI_2020_01_26_13_51_35_T21HWD_L2W.nc", seed=1
        )
        nc2 = _make_l2w_nc(
            tmp_path / "S2A_MSI_2022_01_15_13_51_43_T21HWD_L2W.nc", seed=2
        )
        datacube_path = tmp_path / "l2w_datacube.zarr"

        append_l2w_to_datacube(
            nc1,
            datacube_path,
            target_crs=TEST_CRS,
            target_resolution=10,
            variables=["chl_oc3"],
        )
        # Before the fix, this second append is where the ValueError fired.
        append_l2w_to_datacube(
            nc2,
            datacube_path,
            target_crs=TEST_CRS,
            target_resolution=10,
            variables=["chl_oc3"],
        )

        ds = xr.open_zarr(datacube_path, consolidated=False)
        assert ds.sizes["time"] == 2

    def test_first_scene_write_does_not_raise(
        self, tmp_path, force_fillvalue_conflict
    ):
        nc1 = _make_l2w_nc(
            tmp_path / "S2A_MSI_2020_01_26_13_51_35_T21HWD_L2W.nc", seed=1
        )
        datacube_path = tmp_path / "l2w_datacube.zarr"

        append_l2w_to_datacube(
            nc1,
            datacube_path,
            target_crs=TEST_CRS,
            target_resolution=10,
            variables=["chl_oc3"],
        )

        ds = xr.open_zarr(datacube_path, consolidated=False)
        assert ds.sizes["time"] == 1
        assert "chl_oc3" in ds.data_vars
