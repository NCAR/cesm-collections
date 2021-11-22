import pytest
import os
import pathlib
from esm_collections import flows
from xcollection import Collection

sample_data_dir = pathlib.Path(os.path.dirname(__file__)).parent / 'data'

@pytest.mark.parametrize(
    'data_catalog, search_dict',
    [(sample_data_dir / 'catalogs' / 'cam_catalog.json',
     {'variables':['T']})],
)
def test_base_flow(data_catalog, search_dict):
    flow = flows.base_collection
    run_flow = flow.run(path='data/catalogs/cam_catalog.json',
             search_dict=search_dict)

    print(run_flow.result[flows.collection]._result.value)
    assert isinstance(run_flow.result[flows.collection]._result.value, Collection)
