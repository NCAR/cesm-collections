import xcollection as xc

def convert_to_collection(keys, dsets):
    """Converts keys and datasets to an xcollection.Collection"""
    return xc.Collection({keys: dsets for keys, dsets in zip(keys, dsets)})

class workflow_baseclass(object):
    """base class for flow"""
    def __init__(self):
        self.keys = None
        self.key_final_result = None
        self.flow = None
        
    def run(self, catalog_path=None, multi_var_row=False, search_dict={}, cdf_kwargs=None, *args):
        if self.flow is None:
            raise AttributeError('Missing the Flow')
        
        
        run_flow = self.flow.run(catalog_path=catalog_path,
                                 multi_var_row=multi_var_row,
                                 search_dict=search_dict,
                                 cdf_kwargs=cdf_kwargs,
                                 *args)    
        
        return convert_to_collection(
            run_flow.result[self.keys].result,
            run_flow.result[self.key_final_result].result, 
        )