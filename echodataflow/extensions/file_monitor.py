from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union
from prefect import flow, task
from prefect.variables import Variable
from prefect.blocks.core import Block
from prefect.deployments import run_deployment
from prefect.client.schemas.objects import FlowRun, StateType

from datetime import datetime

from echodataflow.models.run import EDFRun


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
        return (file_path, False)
    return (file_path, True)


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
    var: Variable = Variable.get(name="last_run")

    if var:
        edfrun = EDFRun(**json.loads(var.value))
    else:
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
            if file_mtime > last_run and file_path not in edfrun.processed_files:
                all_files.append((file_path, file_mtime))

    # Sort files by modification time
    all_files.sort(key=lambda x: x[1])

    # Skip the most recently modified file
    if all_files:
        all_files = all_files[:-1]

    futures = []

    if fail_safe:
        for file_path, file_mtime in all_files:
            edfrun.processed_files.append(file_path)
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
                edfrun.processed_files.remove(file_path)

        tuple_list = [f.result() for f in futures]

        exceptionFlag = True if any(not t for _, t in tuple_list) else False

        tuple_list = [t for t in tuple_list if not t[1]]

        for f, _ in tuple_list:
            edfrun.processed_files.remove(f)

    else:
        for file_path, file_mtime in all_files:
            edfrun.processed_files.append(file_path)
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
            if not status:
                edfrun.processed_files.remove(file_path)
                exceptionFlag = True
                break

    if not exceptionFlag:
        edfrun.last_run_time = new_run

    run_json = json.dumps(edfrun.__dict__)

    Variable.set(name="last_run", value=run_json, overwrite=True)

    if exceptionFlag:
        raise ValueError("Encountered Exception in one or more files")


if __name__ == "__main__":
    file_monitor.serve()
