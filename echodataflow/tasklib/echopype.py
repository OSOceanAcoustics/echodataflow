from typing import Any, Dict, Optional
from echopype import open_raw
from echopype.commongrid import compute_MVBS
from echopype.calibrate import compute_Sv
from prefect import task

from echodataflow.models.deployment.stage import Task
from echodataflow.models.deployment.storage_options import StorageOptions


@task
def edf_open_raw(task: Task, data: Any, storage_options: Optional[Dict[str, Any]] = {}):
    
    if task.task_params is not None:
        
        # Validate task params if required

        ed = open_raw(
                raw_file=data,
                sonar_model=task.task_params.get('sonar_model', None),
                storage_options=storage_options,                
            )
    else:
        raise ValueError("task_params are required for edf_open_raw")
    
    return {'data': ed}

@task
def edf_sv(task: Task, data: Dict[str, Any], storage_options: Optional[Dict[str, Any]] = {}):
    
    if task.task_params is not None:
        
        # Validate task params if required

        ed = compute_Sv(
                Sv=data.get('data'),
                sonar_model=task.task_params.get('sonar_model', None),
                storage_options=storage_options,                
            )
    else:
        raise ValueError("task_params are required for edf_open_raw")
    
    return {'data': ed, 'output1': ed}


