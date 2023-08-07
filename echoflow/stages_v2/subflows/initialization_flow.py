import logging
from typing import Any, Dict

import dask
from distributed import Client

from echoflow.config.models.dataset import Dataset
from echoflow.config.models.pipeline import Recipe
from echoflow.stages_v2.aspects.echoflow_aspect import echoflow
from echoflow.stages_v2.utils.config_utils import club_raw_files, get_prefect_config_dict, glob_all_files, parse_raw_paths
from echoflow.stages_v2.utils.function_utils import dynamic_function_call

from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import *
from prefect_dask import DaskTaskRunner

@flow(name="Main-Flow", task_runner=SequentialTaskRunner())
@echoflow(type="FLOW")
def init_flow(
        pipeline: Recipe,
        dataset: Dataset,
        export: bool = False,
        export_path: str = "",
        export_storage_options: Dict[Any, Any] = {}
        ):
    
    total_files = glob_all_files(config=dataset)

    file_dicts = parse_raw_paths(all_raw_files=total_files, config=dataset)
    data = club_raw_files(
        config=dataset,
        raw_dicts=file_dicts,
        raw_url_file=export_path,
        json_storage_options=export_storage_options
    )

    process_list = pipeline.pipeline

    for process in process_list:
        for stage in process.stages:
            function = dynamic_function_call(stage.module, stage.name)
            prefect_config_dict = get_prefect_config_dict(stage, pipeline)
            if prefect_config_dict:
                function = function.with_options(**prefect_config_dict)
            print("Using", prefect_config_dict)
            print("Executing stage : ",stage)
            output = function(dataset, stage, data)
            data = output
            print("Completed stage", stage)
    client = Client(address=pipeline.scheduler_address)
    client.close()
    return output


