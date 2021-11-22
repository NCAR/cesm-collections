import pop_tools
import xarray as xr
import numpy as np
from xhistogram.xarray import histogram
import geocat.comp


def _get_tb_name_and_tb_dim(ds):
    """return the name of the time 'bounds' variable and its second dimension"""
    assert "bounds" in ds.time.attrs, 'missing "bounds" attr on time'
    tb_name = ds.time.attrs["bounds"]
    assert tb_name in ds, f'missing "{tb_name}"'
    tb_dim = ds[tb_name].dims[-1]
    return tb_name, tb_dim


def center_time(ds):
    """make time the center of the time bounds"""
    ds = ds.copy()
    attrs = ds.time.attrs
    encoding = ds.time.encoding
    tb_name, tb_dim = _get_tb_name_and_tb_dim(ds)
    new_time = ds[tb_name].compute().mean(tb_dim).squeeze()
    if len(ds[tb_name]) == 1:
        new_time = [new_time.data]
        ds['time'] = new_time
        ds = ds.isel(time=0)
    else:
        ds['time'] = new_time
    
    attrs["note"] = f"time recomputed as {tb_name}.mean({tb_dim})"
    ds.time.attrs = attrs
    ds.time.encoding = encoding
    return ds

def global_mean(ds, horizontal_dims, area_field, land_sea_mask, normalize=True, include_ms=False, region_mask=None, time_dim="year"):
    """
    Compute the global mean on some dataset
    Return computed quantity in conventional units.
    """

    compute_vars = [
        v for v in ds if time_dim in ds[v].dims and horizontal_dims == ds[v].dims[-2:]
    ]

    other_vars = list(set(ds.variables) - set(compute_vars))

    if include_ms:
        surface_mask = ds[area_field].where(ds[land_sea_mask] > 0).fillna(0.0)
    else:
        surface_mask = ds[area_field].where(ds.REGION_MASK > 0).fillna(0.0)

    if region_mask is not None:
        surface_mask = surface_mask * region_mask

    masked_area = {
        v: surface_mask.where(ds[v].notnull()).fillna(0.0) for v in compute_vars
    }

    with xr.set_options(keep_attrs=True):

        dso = xr.Dataset(
            {v: (ds[v] * masked_area[v]).sum(horizontal_dims) for v in compute_vars}
        )
        if normalize:
            dso = xr.Dataset(
                {v: dso[v] / masked_area[v].sum(horizontal_dims) for v in compute_vars}
            )

        return xr.merge([dso, ds[other_vars]]).drop(
            [c for c in ds.coords if ds[c].dims == horizontal_dims]
        )

def zonal_mean(da_in, grid, lat_axis=None, lat_field='geolat', ydim='yh', xdim='xh', area_field='area_t', region_mask=None):
    """Calculate a zonal average from some model on xarray.DataArray
    
    Parameters
    ----------
    
    da_in : xarray.DataArray
       DataArray to calculate a zonal average from. This should be your data variable
       
    grid : xarray.Dataset
       Grid with the latitude, area field, and latitude axis (if needed), matching dims of da_in
       
    lat_axis : xarray.DataArray
       Latitude axis to use for latitude bins
    
    lat_field : string
       Name of the latitude field to use
    
    ydim : string
       Name of y-dimension
    
    xdim : string
       Name of x-dimension
       
    area_field : string
       Field to use for the area values, used for weighting
       
    Returns
    -------
    da_out : xarray.DataArray
       Resultant zonally averaged field, with the same input name and a new latitude bin axis
    """

    # If not provided a latitude axis, use the y-axis
    if lat_axis is None:
        lat_axis = grid[ydim]
    
    area = grid[area_field].broadcast_like(da_in).where(da_in > -9999)
    lat_2d = grid[lat_field]
    
    if region_mask is not None:
        da_in = da_in.where(region_mask>0)
        area = area * region_mask.where(region_mask>0)
        lat_2d = lat_2d.where(region_mask>0)
    
    # Create the latitude bins using the lat_axis data array
    bins =  lat_axis.values
    
    # Calculate the numerator
    histVolCoordDepth = histogram(lat_2d.broadcast_like(area).where(~np.isnan(area)), bins=[bins], weights=area, dim=[ydim, xdim])
    
    # Calculate the denominator
    histTVolCoordDepth = histogram(lat_2d.broadcast_like(area).where(~np.isnan(area)), bins=[bins], weights=(area*da_in).fillna(0), dim=[ydim, xdim])
    
    if region_mask is not None:
        histRegionVolCoordDepth = histogram(lat_2d.broadcast_like(area).where(~np.isnan(area)), bins=[bins], weights=(area*region_mask).fillna(0), dim=[ydim, xdim])
    
    da_out = (histTVolCoordDepth/histVolCoordDepth).rename(da_in.name)
    
    # Return the zonal average, renaming the variable to the variable in
    return da_out

def temporal_average(ds, time_dim='time'):
    time_variable_dims = []
    for var in ds:
        if time_dim in ds[var].dims:
            time_variable_dims.append(var)
    return ds[time_variable_dims].mean(dim='time')

def yearly_average(ds, time_dim='time'):
    time_variable_dims = []
    for var in ds:
        if time_dim in ds[var].dims:
            time_variable_dims.append(var)
    return geocat.comp.climatologies.climatology(ds[time_variable_dims], "year", time_dim)