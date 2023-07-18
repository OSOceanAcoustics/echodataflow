from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Union
from echoflow.config.models.dataset import Dataset
from echoflow.config.models.pipeline import Pipeline, Recipe
from echoflow.stages_v2.utils.config_utils import club_raw_files, glob_all_files, parse_raw_paths, get_prefect_config_dict
import importlib
from echoflow.stages_v2.utils.file_utils import make_temp_folder, download_temp_file
from echoflow.stages_v2.utils.function_utils import dynamic_function_call, get_function_arguments
from echoflow.stages_v2.utils.databse_utils import get_connection, get_last_log, create_log_table

from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from echoflow.config.models.log_object import Log, Log_Data
from echoflow.stages_v2.utils.global_db_logger import add_new_process, get_log, init_global


@flow(name="Main-Flow", task_runner=SequentialTaskRunner())
def init_flow(
        pipeline: Recipe,
        dataset: Dataset,
        export: bool = False,
        export_path: str = "",
        export_storage_options: Dict[Any, Any] = {},
        ):

    total_files = glob_all_files(dataset)

    file_dicts = parse_raw_paths(total_files, dataset)
    data = club_raw_files(
        config=dataset,
        raw_dicts=file_dicts,
        raw_url_file=export_path,
        json_storage_options=export_storage_options,
        return_state=True
    )
    ret_data = data.result()
    print(ret_data)
    return data

    process_list = pipeline.pipeline

    try:
        for process in process_list:
            for stage in process.stages:
                print("Executing stage : ",stage)
                function = dynamic_function_call(stage.module, stage.name)
                if(stage.prefect_config is not None):
                    prefect_config_dict = get_prefect_config_dict(stage.prefect_config)
                    function = function.with_options(**prefect_config_dict)
                output = function(dataset, stage, data)
                data = output
                print("Completed stage", stage)
        return output
    except Exception as e:
        print(e)
