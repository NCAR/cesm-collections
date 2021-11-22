from . import tasks
import ast
from prefect import Flow, Parameter

with Flow('base_collection') as base_collection:
    path = Parameter('path', )
    csv_kwargs = Parameter('csv_kwargs', default={"converters": {"variables": ast.literal_eval}})
    search_dict = Parameter('search_dict', default={})
    cdf_kwargs = Parameter('cdf_kwargs', default={'chunks':{}})

    # Read in the data catalog
    data_catalog = tasks.read_catalog(path, csv_kwargs)

    # Subset the catalog
    catalog_subset = tasks.subset_catalog(data_catalog, search_dict)

    # Load the data
    dsets = tasks.load_data(catalog_subset, cdf_kwargs=cdf_kwargs)

    # Convert to xcollection
    collection = tasks.convert_to_collection(dsets)