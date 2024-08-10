import asyncio
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Coroutine, Dict, List, Optional, Union

from prefect import flow, get_client, task
from prefect.blocks.core import Block
from prefect.client.schemas.filters import (DeploymentFilter,
                                            DeploymentFilterId, FlowRunFilter,
                                            FlowRunFilterState,
                                            FlowRunFilterStateType)
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.deployments import run_deployment
from prefect.runtime import deployment
from prefect.states import Cancelled
from prefect.variables import Variable

from echodataflow.models.datastore import StorageType
from echodataflow.models.run import EDFRun, FileDetails
from echodataflow.utils.config_utils import glob_url, load_block
from prefect.task_runners import SequentialTaskRunner

@task
def execute_flow(
    dataset_config,
    pipeline_config,
    logging_config,
    storage_options,
    options,
    file_path,
    json_data_path,
    deployment_name,
):
    """
    Executes a Prefect deployment for the given file.

    Args:
        dataset_config: Configuration for the dataset.
        pipeline_config: Configuration for the pipeline.
        logging_config: Configuration for logging.
        storage_options: Options for storage.
        options: Additional options.
        file_path: Path of the file to be processed.
        json_data_path: Path for the JSON data.
        deployment_name: Name of the Prefect deployment.

    Returns:
        Tuple containing the file path and a boolean indicating success.
    """
    print("Processing : ", file_path)
    if isinstance(file_path, str):
        options["file_name"] = os.path.basename(file_path).split(".", maxsplit=1)[0]
    elif isinstance(file_path, list):
        options["file_name"] = [os.path.basename(f).split(".", maxsplit=1)[0] for f in file_path]
    
    flow_run: FlowRun = run_deployment(
        name=deployment_name,
        parameters={
            "dataset_config": dataset_config,
            "pipeline_config": pipeline_config,
            "logging_config": logging_config,
            "storage_options": storage_options,
            "options": options,
            "json_data_path": json_data_path,
        },
    )
    if flow_run.state and flow_run.state.type == StateType.FAILED:
        return (os.path.basename(file_path), False)
    return (os.path.basename(file_path), True)


@flow
def file_monitor(
    dir_to_watch: str,
    dataset_config: Union[Dict[str, Any], str, Path],
    pipeline_config: Union[Dict[str, Any], str, Path],
    logging_config: Union[Dict[str, Any], str, Path] = None,
    storage_options: Union[Dict[str, Any], Block] = None,
    options: Optional[Dict[str, Any]] = {},
    json_data_path: Union[str, Path] = None,
    fail_safe: bool = True,
    deployment_name: str = "echodataflowv2/Echodataflowv2",
    hour_threshold: int = 2,
    minute_threshold: int = 0,
    retry_threshold: int = 3,
    extension: str = None,
    file_name: str = "Bell_M._Shimada-SH2407-EK80",
    min_time: str = "2024-07-05T15:45:00.000000",
    max_folder_depth: int = 1,
    block_name: str = "edf-fm-last-run",
    tags: List[str] = ["edfFM"],
    max_files: int = -1,
    processing_mode: str = "realtime",
):
    """
    Monitors a directory for file changes and processes new or modified files.

    Args:
        dir_to_watch: Directory to monitor for changes.
        dataset_config: Configuration for the dataset.
        pipeline_config: Configuration for the pipeline.
        logging_config: Configuration for logging.
        storage_options: Options for storage.
        options: Additional options.
        json_data_path: Path for the JSON data.
        fail_safe: Flag to enable fail-safe mode.
        deployment_name: Name of the Prefect deployment.

    Raises:
        ValueError: If an exception occurs in one or more files.
    """
    
    if deployment_already_running():
        return Cancelled()
    
    new_run = datetime.now().isoformat()
    edfrun: EDFRun = None
    try:
        edfrun = load_block(
                    name=block_name,
                    type=StorageType.EDFRUN,
                )
    except Exception as e:
        print(e)        
        edfrun = EDFRun()
    
    last_run = datetime.fromisoformat(edfrun.last_run_time)
    exceptionFlag = False
    min_time = datetime.fromisoformat(min_time)

    # List all files and their modification times
    all_files = []
    
    if "*" in dir_to_watch:
        files = glob_url(dir_to_watch, storage_options=storage_options if storage_options else {}, maxdepth=max_folder_depth)
        for file in files:
            try:
                fext = os.path.basename(file).split('.')[1]
            except Exception:
                fext = ""
                
            if not extension or (extension and extension == fext):
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file))
                file = os.path.basename(file)
                if file_mtime > last_run or not edfrun.processed_files.get(file) or not edfrun.processed_files[file].status:
                    if file_mtime > min_time:
                        if not edfrun.processed_files.get(file):
                            edfrun.processed_files[os.path.basename(file)] = FileDetails()
                        all_files.append((file, file_mtime, file))
    else:
        for root, _, files in os.walk(dir_to_watch):
            for file in files:
                
                file_path = os.path.join(root, file)
                file = os.path.basename(file)
                try:
                    fext = os.path.basename(file_path).split('.')[1]
                except Exception:
                    fext = ""
                    
                if not extension or (extension and extension == fext):
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                    if file_mtime > last_run or not edfrun.processed_files.get(file) or not edfrun.processed_files[file].status:
                        if not edfrun.processed_files.get(file):
                            edfrun.processed_files[file] = FileDetails()
                        all_files.append((file_path, file_mtime, file))
            break
                            
    if processing_mode == "batch":
        status = execute_flow.with_options(tags=tags, task_run_name=file_path)(
                    dataset_config=dataset_config,
                    pipeline_config=pipeline_config,
                    logging_config=logging_config,
                    storage_options=storage_options,
                    options=options,
                    file_path=all_files,
                    json_data_path=json_data_path,
                    deployment_name=deployment_name,
                )[1]
        for _, _, file in all_files:
            edfrun.processed_files[file].status = status
            edfrun.processed_files[file].last_run = datetime.now()
    else:
        
        # Sort files by modification time
        all_files.sort(key=lambda x: x[1])
        print("Files To be processed : ",len(all_files))
        
        last_file = None
        
        if len(all_files) > 0:
            last_file = all_files[-1]
        
        print(all_files)
        # Skip the most recently modified file
        if all_files and (datetime.now() - timedelta(hours=hour_threshold, minutes=minute_threshold)) < all_files[-1][1]:
            all_files = all_files[:-1]

        futures = []
        
        var: Variable = Variable.get(name="run_name", default=None)
        
        if not var:
            value = file_name + f"_{datetime.now().strftime('D%Y%m%d-T%H%M%S')}"
            Variable.set(name="run_name", value=value, overwrite=True)
        else:
            value = var.value
            
        if fail_safe:
            for file_path, file_mtime, file in all_files:
                
                if edfrun.processed_files[file].retry_count < retry_threshold:
                    edfrun.processed_files[file].retry_count += 1
                    try:
                        futures.append(
                            execute_flow.with_options(tags=tags, task_run_name=file_path).submit(
                                dataset_config=dataset_config,
                                pipeline_config=pipeline_config,
                                logging_config=logging_config,
                                storage_options=storage_options,
                                options=options,
                                file_path=file_path,
                                json_data_path=json_data_path,
                                deployment_name=deployment_name,
                            )
                        )
                    except Exception as e:
                        pass

            tuple_list = [f.result() for f in futures]

            exceptionFlag = True if any(not t for _, t in tuple_list) else False

            for file, status in tuple_list:
                edfrun.processed_files[file].status = status
                edfrun.processed_files[file].process_timestamp = datetime.now().isoformat()
        else:
            itr = 0
            for file_path, file_mtime, file in all_files:
                if itr == max_files:
                    break
                if edfrun.processed_files[file].retry_count < retry_threshold:
                    edfrun.processed_files[file].retry_count += 1
                    
                    if edfrun.processed_files[file].retry_count == retry_threshold:
                        value = f"{file_name}_{datetime.now().strftime('D%Y%m%d-T%H%M%S')}"
                        Variable.set(name="run_name", value=value, overwrite=True)
                        options["run_name"] = value
                    else:
                        options["run_name"] = value
                    
                    status = execute_flow.with_options(tags=tags, task_run_name=file_path)(
                        dataset_config=dataset_config,
                        pipeline_config=pipeline_config,
                        logging_config=logging_config,
                        storage_options=storage_options,
                        options=options,
                        file_path=file_path,
                        json_data_path=json_data_path,
                        deployment_name=deployment_name,
                    )[1]
                    # edfrun.processed_files[file].status = True # hardcoded to true to avoid backlog processing in different schedules
                    edfrun.processed_files[file].status = status
                    edfrun.processed_files[file].process_timestamp = datetime.now().isoformat()
                    if not status:                
                        exceptionFlag = True
                        value = f"{file_name}_{datetime.now().strftime('D%Y%m%d-T%H%M%S')}"
                        Variable.set(name="run_name", value=value, overwrite=True)
                itr += 1
                
        if last_file:
            _, _, file = last_file
            edfrun.processed_files[file].status = False
            edfrun.processed_files[file].retry_count -= 1
        
        edfrun.last_run_time = new_run

    block = edfrun.save(
            block_name, overwrite=True
    )
    if isinstance(block, Coroutine):
        block = asyncio.run(block)

    if exceptionFlag:
        raise ValueError("Encountered Exception in one or more files")

@task
async def deployment_already_running() -> bool:
    deployment_id = deployment.get_id()
    async with get_client() as client:
        # find any running flows for this deployment
        running_flows = await client.read_flow_runs(
            deployment_filter=DeploymentFilter(
                id=DeploymentFilterId(any_=[deployment_id])
            ),
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(any_=[StateType.RUNNING])
                ),
            ),
        )
    if len(running_flows) > 1:
        return True
    else:
        return False


if __name__ == "__main__":
    file_monitor.serve(name="file-monitor")