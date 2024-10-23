from typing import List, Dict, Any, Optional
from distributed import Client, LocalCluster
from prefect import flow
from prefect_dask import get_dask_client
from prefect_dask.task_runners import DaskTaskRunner

from echodataflow.aspects.singleton_echodataflow import Singleton_Echodataflow
from echodataflow.models.deployment.deployment import Cluster
from echodataflow.models.deployment.stage import Stage
from echodataflow.utils import log_util
from echodataflow.utils.config_utils import get_prefect_config_dict
from echodataflow.utils.function_utils import dynamic_function_call

from echodataflow.deployment.flow import Sv_flow

@flow(name="Echodataflow-Service")
def edf_service(stages: List[Stage], edf_logger: Optional[Dict[str, Any]] = None, cluster: Optional[Cluster] = None) -> bool:
    """
    Service for EDF
    """
    
    # Add optional support to create a cluster which stays the same for all stages
    # For specific stages to be executed on dask cluster add the config to prefect config of the stage.
    client: Client = None
    
    #Initiate Logging
    Singleton_Echodataflow(log_file=edf_logger)
    gea = Singleton_Echodataflow().get_instance()
    
    global_dask = False
    close_cluster_allowed = True
    
    if cluster:
        global_dask = True
        if cluster.address is None:
            client = Client(LocalCluster(n_workers=cluster.workers, nanny=True).address)
        else:
            client = Client(cluster.address)
            close_cluster_allowed = False
    
    for stage in stages:
        
        if stage.module:
            master_flow = dynamic_function_call(stage.module, stage.name)
        else:
            master_flow = Sv_flow
        
        prefect_config_dict = get_prefect_config_dict(stage)
        
        prefect_config_dict["name"] = stage.name
        prefect_config_dict["flow_run_name"] = stage.name
        
        if global_dask:
            prefect_config_dict["task_runner"] = DaskTaskRunner(address=client.scheduler.address)
        
        if client is not None:
            client.subscribe_topic("echodataflow", lambda event: log_util.log_event(event))
            
        if prefect_config_dict.get("task_runner", None):
            stage.options["use_dask"] = True
            
        # master_flow is expected to store the output to the destination
        # Source for the next flows to match with the destination of previous flows if two stages are connected
        master_flow.with_options(**prefect_config_dict)(stage)
    
    for stage in stages:
        if not stage.options.get("save_offline", True): 
            # cleanup
            stage.destination.cleanup()

    # close cluster if required
    if client and close_cluster_allowed:
        client.close()
    
    return True