import os
import json
from pathlib import Path
from echoflow import echoflow_start
from echoflow.stages.echoflow_trigger import echoflow_trigger
from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from typing import Any, Dict, Optional, Union

@flow(name="Docker-Trigger", task_runner=SequentialTaskRunner())
def docker_trigger(
    dataset_config: Union[dict, str, Path],
    pipeline_config: Union[dict, str, Path],
    logging_config: Union[dict, str, Path] = None,
    storage_options: Optional[dict] = None,
    options: Optional[dict] = {},
    json_data_path: Union[str, Path] = None
):
    return echoflow_trigger(
        dataset_config=dataset_config,
        pipeline_config=pipeline_config,
        logging_config=logging_config,
        storage_options=storage_options,
        options=options,
        json_data_path=json_data_path
    )

def convert_to_proper_type(input_str):
    if input_str is None:
        return None

    # Try to parse input as JSON
    try:
        return json.loads(input_str)
    except (ValueError, TypeError):
        pass

    # If it's not valid JSON, check if it's a file path
    if os.path.exists(input_str):
        return Path(input_str)

    # If not JSON or a valid file path, return the input as is
    return input_str

if __name__ == "__main__":
    print("Entered")
    # Get inputs from environment variables or command-line arguments
    dataset_config = convert_to_proper_type(os.getenv('DATASET_CONFIG', '{}'))
    pipeline_config = convert_to_proper_type(os.getenv('PIPELINE_CONFIG', '{}'))
    logging_config = convert_to_proper_type(os.getenv('LOGGING_CONFIG', None))
    storage_options = convert_to_proper_type(os.getenv('STORAGE_OPTIONS', None))
    options = convert_to_proper_type(os.getenv('OPTIONS', '{}'))
    json_data_path = convert_to_proper_type(os.getenv('JSON_DATA_PATH', None))

    # Call the docker_trigger function
    docker_trigger(dataset_config, pipeline_config, logging_config, storage_options, options, json_data_path)