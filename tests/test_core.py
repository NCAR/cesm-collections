import xarray as xr
import xcollection as xc
from esm_collections.core import convert_to_collection, workflow_baseclass

ds = xr.tutorial.open_dataset('air_temperature')
dsets = [ds.isel(time=0),
         ds.isel(time=1)]
keys = ['foo', 'bar']

def test_convert_to_collection(keys=keys, dsets=dsets):
    collection = convert_to_collection(keys=keys,
                                       dsets=dsets)

    assert isinstance(collection, xc.Collection)
    assert isinstance(collection['foo'], xr.Dataset)

def test_workflow_baseclass():
    workflow = workflow_baseclass()
    assert workflow.keys == None
    assert workflow.flow == None