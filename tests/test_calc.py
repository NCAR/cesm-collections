import pytest
import os
import pathlib
import xarray as xr
import numpy as np
import pop_tools
from esm_collections import calc

sample_data_dir = pathlib.Path(os.path.dirname(__file__)).parent / 'data'

@pytest.mark.parametrize(
    'data',
    [sample_data_dir / 'cam' / 'b.e21.BW.f09_g17.SSP245-TSMLT-GAUSS-LOWER-0.5.001.cam.h0.2035-01.nc',
     sample_data_dir / 'pop' / 'pop_no_mcog.pop.h.0001-01.nc' 
    ],
)
def test_center_time(data):
    ds = xr.open_dataset(data)
    ds_center_time = calc.center_time(ds)
    assert isinstance(ds_center_time, xr.Dataset)
    assert ds.time.dt.month.values[0] == 2
    assert ds_center_time.time.dt.month.values == 1

@pytest.mark.parametrize(
    'data, horizontal_dims, area_field, land_sea_mask, normalize, time_dim',
    [(sample_data_dir / 'pop' / 'pop_no_mcog.pop.h.0001-01.nc',
     ('nlat', 'nlon'),
     'TAREA',
     'KMT',
     True,
     'time')],
)
def test_global_mean(data, horizontal_dims, area_field, land_sea_mask, normalize, time_dim):
    ds = xr.open_dataset(data)
    grid = pop_tools.get_grid('POP_gx1v7')
    ds = xr.merge([ds, grid[['TAREA', 'REGION_MASK', 'KMT']]])
    ds_global_mean= calc.global_mean(ds,
                                     horizontal_dims=horizontal_dims,
                                     area_field=area_field,
                                     land_sea_mask=land_sea_mask,
                                     normalize=normalize,
                                     region_mask=None,
                                     time_dim=time_dim)
    assert isinstance(ds_global_mean, xr.Dataset)

@pytest.mark.parametrize(
    'data, horizontal_dims, area_field, land_sea_mask, normalize, include_ms, time_dim',
    [(sample_data_dir / 'pop' / 'pop_no_mcog.pop.h.0001-01.nc',
     ('nlat', 'nlon'),
     'TAREA',
     'KMT',
     [True, False],
     [True, False],
     'time')],
)
def test_regional_mean(data, horizontal_dims, area_field, land_sea_mask, normalize, include_ms, time_dim):
    ds = xr.open_dataset(data)
    grid = pop_tools.get_grid('POP_gx1v7')
    region_mask = pop_tools.region_mask_3d('POP_gx1v7')
    ds = xr.merge([ds, grid[['TAREA', 'REGION_MASK', 'KMT']]])
    ds_global_mean= calc.global_mean(ds,
                                     horizontal_dims=horizontal_dims,
                                     area_field=area_field,
                                     land_sea_mask=land_sea_mask,
                                     normalize=normalize,
                                     include_ms=include_ms,
                                     region_mask=region_mask,
                                     time_dim=time_dim)
    assert isinstance(ds_global_mean, xr.Dataset)

@pytest.mark.parametrize(
    'data, lat_aux_grid_file',
    [(sample_data_dir / 'pop' / 'pop_no_mcog.pop.h.0001-01.nc',
      sample_data_dir / 'pop' / 'lat_aux_grid.nc')],
)
def test_zonal_mean(data, lat_aux_grid_file):
    ds = xr.open_dataset(data)
    grid = pop_tools.get_grid('POP_gx1v7')
    lat_aux_grid = xr.open_dataset(lat_aux_grid_file).lat_aux_grid
    ds_zonal_average= calc.zonal_mean(ds.TEMP,
                                      grid,
                                      lat_axis=lat_aux_grid,
                                      lat_field='TLAT',
                                      ydim='nlat',
                                      xdim='nlon',
                                      area_field='TAREA',
                                      region_mask=None)

    assert isinstance(ds_zonal_average, xr.DataArray)
