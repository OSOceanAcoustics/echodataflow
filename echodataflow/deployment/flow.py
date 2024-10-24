import json
import os
from typing import Dict, List
from fastapi.encoders import jsonable_encoder
from prefect import Task, flow
from prefect_dask import get_dask_client
from echodataflow.models.deployment.stage import Stage
from echodataflow.models.output_model import EchodataflowObject
from echodataflow.utils import log_util
from echodataflow.utils.config_utils import club_raw_files, parse_raw_paths
from echodataflow.utils.function_utils import dynamic_function_call

@flow(name='Sv_flow')
def Sv_flow(stage: Stage):
    
    # call source extraction, which converts source into intermediate data representation
    # puts in dict of dict of list of Object
    raw_files: List[str] = stage.source.extract_source()

    # Group data based on given file(s)
    file_dicts = parse_raw_paths(all_raw_files=raw_files, config=stage.source, group=stage.group)

    log_util.log(
        msg={
            "msg": f"Files To Be Processed",
            "mod_name": __file__,
            "func_name": "Init Flow",
        }
    )
    log_util.log(
        msg={
            "msg": json.dumps(jsonable_encoder(file_dicts)),
            "mod_name": __file__,
            "func_name": "Init Flow",
        }
    )
    edf_metadata = club_raw_files(
        config=stage.group,
        raw_dicts=file_dicts,
        raw_url_file=None,
        json_storage_options=None,
    )
    
    # For each file, call tasks
    for group in edf_metadata:
        for fdict in group:
            
            file_info = os.path.basename(fdict.get("file_path")).split(".", maxsplit=1)
            
            data_dict = {"source_file_path": fdict.get("file_path"),
                    "filename": file_info[0],
                    "file_extension": file_info[-1]}
            
            for task in stage.tasks:

                task_fn = dynamic_function_call(task.module, task.name)
                if not isinstance(task_fn, Task):
                    raise ValueError(f"Task {task.name} is not a valid Task. Annotate tasks with @task decorator")
                
                data_dict = task_fn.submit(task, data_dict, stage.source.storage_options._storage_options_dict)

            data_dict = data_dict.result()
            stage.destination.store_result(data=data_dict, engine="zarr")
    
    return True