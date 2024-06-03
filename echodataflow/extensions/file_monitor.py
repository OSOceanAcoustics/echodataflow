from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union
from prefect import flow, task
from prefect.variables import Variable
from prefect.blocks.core import Block
from prefect.deployments import run_deployment

from datetime import datetime
from typing import List

from pydantic import BaseModel

class EDFRun(BaseModel):
    last_run_time: str = datetime.min.isoformat()
    processed_files: List[str] = []
    
@task
def execute_flow(dataset_config, pipeline_config, logging_config, storage_options, options, file_path, json_data_path):
    print("Processing : ", file_path)
    options["file_name"] = os.path.basename(file_path).split(".", maxsplit=1)[0]       
    run_deployment(name='echodataflow-start/echodataflow-worker', parameters={"dataset_config":dataset_config,
        "pipeline_config":pipeline_config,
        "logging_config":logging_config,
        "storage_options":storage_options,
        "options":options,
        "json_data_path":json_data_path}, as_subflow=False, timeout=0)

@flow
def file_monitor(
    dir_to_watch: str,
    dataset_config: Union[Dict[str, Any], str, Path],
    pipeline_config: Union[Dict[str, Any], str, Path],
    logging_config: Union[Dict[str, Any], str, Path] = None,
    storage_options: Union[Dict[str, Any], Block] = None,
    options: Optional[Dict[str, Any]] = {},
    json_data_path: Union[str, Path] = None,
):
    new_run = datetime.now().isoformat()
    var: Variable = Variable.get(name="last_run")
    print('Var is ',var)
    
    if var:
        edfrun = EDFRun(**json.loads(var.value))
    else:        
        edfrun = EDFRun()
    print('Sorting', edfrun)
    last_run = datetime.fromisoformat(edfrun.last_run_time)
    exceptionFlag = False
    print("dir",dir_to_watch)
    # List all files and their modification times
    all_files = []
    for root, _, files in os.walk(dir_to_watch):
        for file in files:
            
            file_path = os.path.join(root, file)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            print(file_path)
            if file_mtime > last_run and file_path not in edfrun.processed_files:                                
                all_files.append((file_path, file_mtime))
    # Sort files by modification time
    all_files.sort(key=lambda x: x[1])
    # Skip the most recently modified file
    if all_files:
        all_files = all_files[:-1]
    print('Sorting', all_files)
    for file_path, file_mtime in all_files:

        # Process the file
        edfrun.processed_files.append(file_path)
        try:
            execute_flow.with_options(tags=['edfFM']).submit(dataset_config=dataset_config, pipeline_config=pipeline_config, 
                                logging_config=logging_config, storage_options=storage_options, 
                                options=options, file_path=file_path, json_data_path=json_data_path)
        except Exception as e:
            exceptionFlag = True
            edfrun.processed_files.remove(file_path)          
            raise e            

    if not exceptionFlag:
        edfrun.last_run_time = new_run
    
    run_json = json.dumps(edfrun.__dict__)
    
    Variable.set(name="last_run", value=run_json, overwrite=True)
        
if __name__ == "__main__":
    file_monitor.serve()
