from prefect import task
import intake
import calc


# Deal with Intake-ESM catalogs
@task
def read_catalog(path, csv_kwargs):
    return intake.open_esm_datastore(path, read_csv_kwargs=csv_kwargs)

@task
def subset_catalog(catalog, search_dict):
    return catalog.search(**search_dict)

# Calculations
@task
def center_time(ds, model):
    return calc.center_time(ds)

@task
def global_average(ds, horizontal_dims, area_field, land_sea_mask, time_dim, include_ms=False):
    return calc.global_mean(ds, horizontal_dims=horizontal_dims, area_field=area_field, land_sea_mask=land_sea_mask, time_dim=time_dim, normalize=True)

@task
def global_integral(ds, horizontal_dims, area_field, land_sea_mask, time_dim, include_ms=False):
    return calc.global_mean(ds, horizontal_dims=horizontal_dims, area_field=area_field, land_sea_mask=land_sea_mask, time_dim=time_dim, normalize=False)

@task
def zonal_average(da, grid, lat_field, ydim, xdim, lat_axis, region_mask=None):
    return calc.zonal_mean(da=da, grid=grid, lat_field=lat_field, ydim=ydim, xdim=xdim, lat_axis=lat_axis, region_mask=region_mask)
