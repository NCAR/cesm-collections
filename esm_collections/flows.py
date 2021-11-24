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
    
with Flow('timeseries_collection') as timeseries_collection:
    path = Parameter('path', )
    csv_kwargs = Parameter('csv_kwargs', default={"converters": {"variables": ast.literal_eval}})
    search_dict = Parameter('search_dict', default={})
    cdf_kwargs = Parameter('cdf_kwargs', default={'chunks':{}})
    subset_times = Parameter('subset_times', default=slice(-48, -1))

    # Read in the data catalog
    data_catalog = tasks.read_catalog(path, csv_kwargs)

    # Subset the catalog
    catalog_subset = tasks.subset_catalog(data_catalog, search_dict)
    
    # Subset some number of dates
    date_subset = tasks.subset_dates(catalog_subset, search_dict, subset_times)

    # Load the data
    dsets_subset_dates = tasks.load_data(date_subset, cdf_kwargs=cdf_kwargs)
    dsets_timeseries = tasks.load_data(catalog_subset, cdf_kwargs=cdf_kwargs)

    # Convert to xcollection
    collection_subset_dates = tasks.convert_to_collection(dsets_subset_dates)
    collection_timeseries = tasks.convert_to_collection(dsets_timeseries)

    # Center time
    collection_subset_dates_center_time = tasks.center_time(collection_subset_dates)
    collection_timeseries_center_time = tasks.center_time(collection_timeseries)
    
    # Calculate a long-term mean
    long_term_average = tasks.long_term_mean(collection_subset_dates_center_time)
    
    # Calculate the annual average
    annual_average = tasks.annual_mean(collection_timeseries_center_time)