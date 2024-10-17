from typing import List, Dict, Any, Optional
from distributed import Client
from prefect import flow
from prefect_dask import get_dask_client

from echodataflow.aspects.singleton_echodataflow import Singleton_Echodataflow
from echodataflow.models.deployment.stage import Stage
from echodataflow.utils import log_util
from echodataflow.utils.config_utils import get_prefect_config_dict
from echodataflow.utils.function_utils import dynamic_function_call

from echodataflow.deployment.flow import edf_flow

@flow(name="Echodataflow-Service")
def edf_service(stages: List[Stage], edf_logger: Optional[Dict[str, Any]]) -> bool:
    """
    Service for EDF
    """
    
    # Add optional support to create a cluster which stays the same for all stages
    # For specific stages to be executed on dask cluster add the config to prefect config of the stage.
    client: Client = None
    
    #Initiate Logging
    Singleton_Echodataflow(log_file=edf_logger)
    gea = Singleton_Echodataflow().get_instance()
    
    for stage in stages:
        
        if stage.stage_params:
            stage.prefect_config = stage.stage_params.get("prefect_config", None)
            stage.options = stage.stage_params.get("options", None)
            
            if stage.module:
                master_flow = dynamic_function_call(stage.module, stage.name)
            else:
                master_flow = edf_flow

            prefect_config_dict = get_prefect_config_dict(stage)
            
            prefect_config_dict["name"] = stage.name
            prefect_config_dict["flow_run_name"] = stage.name
            
            if prefect_config_dict.get("task_runner", None):
                stage.options["use_dask"] = True
                if client is not None:
                    client.subscribe_topic("echodataflow", lambda event: log_util.log_event(event))
                else:     
                    with get_dask_client() as cl:
                        cl.subscribe_topic("echodataflow", lambda event: log_util.log_event(event))
                        
            # master_flow is expected to store the output to the destination
            # Source for the next flows to match with the destination of previous flows if two stages are connected
            master_flow.with_options(**prefect_config_dict)(stage)
    
    for stage in stages:
        if not stage.options.get("save_offline", True): 
            # cleanup
            stage.destination.cleanup()

    # close cluster if required
    if client:
        client.close()
    
    
    
    return True
            