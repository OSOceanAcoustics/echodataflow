from typing import Any, Dict, Union
from echoflow.config.models.dataset import Dataset
from echoflow.config.models.pipeline import Recipe
from echoflow.stages_v2.aspects.singleton_echoflow import Singleton_Echoflow

from echoflow.stages_v2.subflows.initialization_flow import init_flow
from echoflow.stages_v2.utils.config_utils import check_config, extract_config

from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from distributed import Client


@flow(name="Pipeline-Trigger", task_runner=SequentialTaskRunner())
def pipeline_trigger(
    dataset_config: Union[Dict[str, Any], str],
    pipeline_config: Union[Dict[str, Any], str],
    logging_config: Union[Dict[str, Any], str] = {},
):
    if type(dataset_config) == str:
        if not dataset_config.endswith((".yaml", ".yml")):
            raise ValueError("Configuration file must be a YAML!")
        dataset_config_dict = extract_config(dataset_config)
    if type(pipeline_config) == str:
        if not pipeline_config.endswith((".yaml", ".yml")):
            raise ValueError("Configuration file must be a YAML!")
        pipeline_config_dict = extract_config(pipeline_config)
    if type(logging_config) == str:
        if not logging_config.endswith((".yaml", ".yml")):
            raise ValueError("Configuration file must be a YAML!")
        logging_config_dict = extract_config(logging_config)

    # Do any config checks on config dicts
    # Should be done in pydantic class
    check_config(dataset_config_dict, pipeline_config_dict)

    pipeline = Recipe(**pipeline_config_dict)
    dataset = Dataset(**dataset_config_dict)

    Singleton_Echoflow(log_file=logging_config_dict, pipeline=pipeline, dataset=dataset)
    return init_flow(dataset=dataset, pipeline=pipeline)
