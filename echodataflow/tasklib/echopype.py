from typing import Any, Dict, Optional
from echopype import open_raw
from echopype.commongrid import compute_MVBS
from echopype.calibrate import compute_Sv
from prefect import task
from prefect_dask import get_dask_client

from echodataflow.models.deployment.stage import Task
from echodataflow.models.deployment.storage_options import StorageOptions


@task
def edf_open_raw(task: Task, data: Any, storage_options: Optional[Dict[str, Any]] = {}):
    
    with get_dask_client() as dclient:
        print(dclient.scheduler.address)
    
    if task.task_params is not None:
        # TODO: Validate task params if required
        ed = open_raw(
                raw_file=data.get('source_file_path'),
                sonar_model=task.task_params.get('sonar_model', None),
                storage_options=storage_options,                
            )
    else:
        raise ValueError("task_params are required for edf_open_raw")
    
    data['data'] = ed
    
    return data

@task
def edf_Sv(task: Task, data: Dict[str, Any], storage_options: Optional[Dict[str, Any]] = {}):
    
    if task.task_params is not None:
        
        # Validate task params if required

        Sv = compute_Sv(
                echodata=data.get('data', None)
            )
    else:
        raise ValueError("task_params are required for compute_Sv")
    
    data['data'] = Sv
    return data


