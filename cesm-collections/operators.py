from geocat.comp.climatology import calendar_average
import numpy as np

def _get_tb_name_and_tb_dim(ds):
    """return the name of the time 'bounds' variable and its second dimension"""
    assert 'bounds' in ds.time.attrs, 'missing "bounds" attr on time'
    tb_name = ds.time.attrs['bounds']        
    assert tb_name in ds, f'missing "{tb_name}"'    
    tb_dim = ds[tb_name].dims[-1]
    return tb_name, tb_dim

def center_time(ds):
    """make time the center of the time bounds"""
    ds = ds.copy()
    attrs = ds.time.attrs
    encoding = ds.time.encoding
    tb_name, tb_dim = _get_tb_name_and_tb_dim(ds)
    
    ds['time'] = ds[tb_name].compute().mean(tb_dim).squeeze()
    attrs['note'] = f'time recomputed as {tb_name}.mean({tb_dim})'
    ds.time.attrs = attrs
    ds.time.encoding = encoding
    return ds

def seasonal_average(ds):
    """Calculate a seasonal average"""
    return calendar_average(ds, 'season', time_dim='time')

def annual_average(ds):
    """Caclulate an annual average"""
    return calendar_average(ds, 'year', time_dim='time')

def get_lat_name(ds):
    for lat_name in ['lat', 'latitude']:
        if lat_name in ds.coords:
            return lat_name
    raise RuntimeError("Couldn't find a latitude coordinate")

def global_mean(ds):
    """Calcualtes a global mean for the atmosphere"""
    lat = ds[get_lat_name(ds)]
    weight = np.cos(np.deg2rad(lat))
    weight /= weight.mean()
    other_dims = set(ds.dims) - {'time'}
    return (ds * weight).mean(other_dims)