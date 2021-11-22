import os
from prefect import task
import intake
import xcollection

from . import calc


# Deal with Intake-ESM catalogs
@task
def read_catalog(path, csv_kwargs):
    return intake.open_esm_datastore(path, csv_kwargs=csv_kwargs)

@task
def subset_catalog(catalog, search_dict):
    return catalog.search(**search_dict)

# Calculations
@task
def center_time(ds, model):
    return calc.center_time(ds)

@task
def subset_dates(catalog, date_subset):
    return catalog.search(date=catalog.df.date[date_subset].values)

@task
def convert_to_collection(dsets):
    return xcollection.Collection(dsets)

@task
def load_data(catalog, cdf_kwargs):
    return catalog.to_dataset_dict(cdf_kwargs=cdf_kwargs)

@task
def long_term_mean(collection):
    return collection.map(calc.temporal_average)

@task
def annual_mean(collection):
    return collection.map(calc.yearly_average)

@task
def global_average(ds, horizontal_dims, area_field, land_sea_mask, time_dim, include_ms=False):
    return calc.global_mean(ds, horizontal_dims=horizontal_dims, area_field=area_field, land_sea_mask=land_sea_mask, time_dim=time_dim, normalize=True)

@task
def global_integral(ds, horizontal_dims, area_field, land_sea_mask, time_dim, include_ms=False):
    return calc.global_mean(ds, horizontal_dims=horizontal_dims, area_field=area_field, land_sea_mask=land_sea_mask, time_dim=time_dim, normalize=False)

@task
def zonal_average(da, grid, lat_field, ydim, xdim, lat_axis, region_mask=None):
    return calc.zonal_mean(da=da, grid=grid, lat_field=lat_field, ydim=ydim, xdim=xdim, lat_axis=lat_axis, region_mask=region_mask)

@task
def cache_result(collection, operation, cache_dir, cache_format='zarr'):
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
        
    for key in collection.keys():
        cache_path = f'{cache_dir}/{operation}/{key}.{operation}.{cache_format}'
        dso = collection[key]
        dso['member_id'] = key
        dso = dso.expand_dims('member_id')
        if cache_format == 'zarr':
            collection[key].to_zarr(cache_path, mode='w')
            
        elif cache_format == 'netcdf':
            collection[key].to_netcdf(cache_path, mode='w')
