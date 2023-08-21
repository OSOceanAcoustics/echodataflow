from typing import Any, Dict, Optional, Union
from echoflow.config.models.datastore import Dataset, StorageType
from echoflow.config.models.pipeline import Recipe
from echoflow.stages_v2.aspects.singleton_echoflow import Singleton_Echoflow
from echoflow.stages_v2.utils.config_utils import load_block

from echoflow.stages_v2.subflows.initialization_flow import init_flow
from echoflow.stages_v2.utils.config_utils import check_config, extract_config, get_storage_options

from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.blocks.core import Block


@flow(name="Pipeline-Trigger", task_runner=SequentialTaskRunner())
def pipeline_trigger(
    dataset_config: Union[Dict[str, Any], str],
    pipeline_config: Union[Dict[str, Any], str],
    logging_config: Union[Dict[str, Any], str] = {},
    storage_options: Optional[Dict[str, Any]] = {}
):

    if type(dataset_config) == str:
        if not dataset_config.endswith((".yaml", ".yml")):
            raise ValueError("Configuration file must be a YAML!")
        dataset_config_dict = extract_config(dataset_config, storage_options)
    if type(pipeline_config) == str:
        if not pipeline_config.endswith((".yaml", ".yml")):
            raise ValueError("Configuration file must be a YAML!")
        pipeline_config_dict = extract_config(pipeline_config, storage_options)
    if type(logging_config) == str:
        if not logging_config.endswith((".yaml", ".yml")):
            raise ValueError("Configuration file must be a YAML!")
        logging_config_dict = extract_config(logging_config, storage_options)

    # Do any config checks on config dicts
    # Should be done in pydantic class
    check_config(dataset_config_dict, pipeline_config_dict)

    pipeline = Recipe(**pipeline_config_dict)
    dataset = Dataset(**dataset_config_dict)

    if not storage_options:
        if dataset.output.storage_options is not None:
            if dataset.output.storage_options.anon == False:
                block = load_block(name=dataset.output.storage_options.block_name, type=dataset.output.storage_options.type)
                dataset.output.storage_options_dict = get_storage_options(block)
            else:
                dataset.output.storage_options_dict = {"anon": dataset.output.storage_options.anon}

        if dataset.args.storage_options is not None: 
            if dataset.args.storage_options.anon == False:
                block = load_block(name=dataset.args.storage_options.block_name, type=dataset.args.storage_options.type)
                dataset.args.storage_options_dict = get_storage_options(block)
            else:
                dataset.args.storage_options_dict = {"anon": dataset.args.storage_options.anon}
    else:
        dataset.output.storage_options_dict = storage_options
        dataset.args.storage_options_dict = storage_options

    Singleton_Echoflow(log_file=logging_config_dict, pipeline=pipeline, dataset=dataset)
    return init_flow(dataset=dataset, pipeline=pipeline)
