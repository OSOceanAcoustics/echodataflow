import asyncio
from datetime import datetime, timedelta
import os
from pathlib import Path
from typing import Any, Coroutine, Dict, Optional, Union
from prefect import flow, task
from prefect.blocks.core import Block
from prefect.deployments import run_deployment
from prefect.client.schemas.objects import FlowRun, StateType

from datetime import datetime

from echodataflow.models.datastore import StorageType
from echodataflow.models.run import EDFRun, FileDetails
from echodataflow.utils.config_utils import load_block


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
    options["file_name"] = os.path.basename(file_path).split(".", maxsplit=1)[0]
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
    retry_threshold: int = 3
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
    new_run = datetime.now().isoformat()
    edfrun: EDFRun = None
    try:
        edfrun = load_block(
                    name="edf-fm-last-run",
                    type=StorageType.EDFRUN,
                )
    except Exception as e:
        print(e)        
        edfrun = EDFRun()
    
    last_run = datetime.fromisoformat(edfrun.last_run_time)
    exceptionFlag = False

    # List all files and their modification times
    all_files = []
    for root, _, files in os.walk(dir_to_watch):
        for file in files:
            file_path = os.path.join(root, file)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            print(file_path)
            if file_mtime > last_run or not edfrun.processed_files.get(file) or not edfrun.processed_files[file].status:
                edfrun.processed_files[file] = FileDetails()
                all_files.append((file_path, file_mtime, file))

    # Sort files by modification time
    all_files.sort(key=lambda x: x[1])
    print("Files To be processed : ",len(all_files))
    
    print(all_files)
    # Skip the most recently modified file
    if all_files and (datetime.now() - timedelta(hours=hour_threshold, minutes=minute_threshold)) > all_files[-1][1]:
        all_files = all_files[:-1]

    futures = []

    if fail_safe:
        for file_path, file_mtime, file in all_files:
            edfrun.processed_files[file].retry_count += 1
            
            # TODO
            # check if retry threshold has reached, if yes, start a new store?
            # Might be tricky though in parallel situation. If appending to a zarr store then FM should switch fail_safe to False
            try:
                futures.append(
                    execute_flow.with_options(tags=["edfFM"], task_run_name=file_path).submit(
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
        for file_path, file_mtime, file in all_files:
            
            edfrun.processed_files[file].retry_count += 1
            
            # TODO
            # check if retry threshold has reached, if yes, start a new store?
            
            status = execute_flow.with_options(tags=["edfFM"], task_run_name=file_path)(
                dataset_config=dataset_config,
                pipeline_config=pipeline_config,
                logging_config=logging_config,
                storage_options=storage_options,
                options=options,
                file_path=file_path,
                json_data_path=json_data_path,
                deployment_name=deployment_name,
            )[1]
            edfrun.processed_files[file].status = status
            edfrun.processed_files[file].process_timestamp = datetime.now().isoformat()
            if not status:                
                exceptionFlag = True
                break
            
    if not exceptionFlag:
        edfrun.last_run_time = new_run

    block = edfrun.save(
            "edf-fm-last-run", overwrite=True
    )
    if isinstance(block, Coroutine):
        block = asyncio.run(block)

    if exceptionFlag:
        raise ValueError("Encountered Exception in one or more files")


if __name__ == "__main__":
    file_monitor.serve()
