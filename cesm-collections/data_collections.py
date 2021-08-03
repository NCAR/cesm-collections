from funnel.collection.main import Collection
import operators

def seasonal_mean(esm_catalog_json, query, name='seasonal_mean', center_time=True, kwargs={}):
    """
    Instantiate a `funnel.Collection` object for computing 
    global mean, annual mean timeseries.
    """
    
    postproccess = [operators.seasonal_average] 
    postproccess_kwargs = [{}]    
    
    if center_time:
        postproccess = [operators.center_time] + postproccess
        postproccess_kwargs = [{}] + postproccess_kwargs

    return Collection(
        collection_name=name,
        esm_collection_json=catalog_json,
        esm_collection_query=query,
        operators=postproccess,
        operator_kwargs=postproccess_kwargs,
        kwargs=kwargs
    )