from typing import Dict, List
from prefect import flow
from prefect_dask import get_dask_client
from echodataflow.models.deployment.stage import Stage
from echodataflow.models.output_model import EchodataflowObject
from echodataflow.utils import log_util
from echodataflow.utils.function_utils import dynamic_function_call

@flow(name='Sv_flow')
def Sv_flow(stage: Stage):
    
    # call source extraction, which converts source into intermediate data representation
    # puts in dict of dict of list of Object
    def_metadata: Dict[str, Dict[str, List[EchodataflowObject]]] = stage.source.extract_source(stage.options)
    
    # Group data based on given file(s)
    
    # For each file, call tasks
    
    for file in def_metadata:
        # TODO: Logic to determine when and where to call the grouping and super grouping logic
        for task in stage.tasks:
            
            task_fn = dynamic_function_call(task.module, task.name)
            
            data = task_fn.submit(task, data, stage.source.storage_options._storage_options_dict)

    
    stage.destination.store_result(data=data, engine="zarr")
    
    return True