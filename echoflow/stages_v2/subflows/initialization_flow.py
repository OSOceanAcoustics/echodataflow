from pathlib import Path
from typing import Any, Dict, Union
from echoflow.config.models.dataset import Dataset
from echoflow.config.models.pipeline import Recipe
from echoflow.stages_v2.utils.config_utils import club_raw_files, glob_all_files, parse_raw_paths
import importlib
from echoflow.stages_v2.utils.file_utils import make_temp_folder, download_temp_file
from echoflow.stages_v2.utils.function_utils import dynamic_function_call, get_function_arguments

from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import *


@flow(name="Main-Flow", task_runner=SequentialTaskRunner())
def init_flow(
        dataset_config:Dict[str, Any],
        pipeline_config:Dict[str, Any],
        export: bool = False,
        export_path: str = "",
        export_storage_options: Dict[Any, Any] = {},
        ):
    
    pipeline = Recipe(**pipeline_config)
    dataset = Dataset(**dataset_config)

    total_files = glob_all_files(dataset)

    file_dicts = parse_raw_paths(total_files, dataset)

    data = club_raw_files(
        config=dataset,
        raw_dicts=file_dicts,
        raw_url_file=export_path,
        json_storage_options=export_storage_options,
    )

    process_list = pipeline.pipeline
    prefect_config_dict = {}
    try:
        for process in process_list:
            for stage in process.stages:
                print("Executing stage : ",stage)
                function = dynamic_function_call(stage.module, stage.name)
                output = function(dataset, stage, data)
                data = output
                print("Completed stage", stage)
        return output
    except Exception as e:
        print(e)