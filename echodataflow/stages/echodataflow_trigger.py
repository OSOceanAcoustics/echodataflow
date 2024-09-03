"""
Echodataflow Trigger Flow

This module defines the Prefect Flow that acts as a trigger to initialize and execute the processing pipeline.

Functions:
    echodataflow_trigger(
        dataset_config: Union[Dict, str],
        pipeline_config: Union[Dict, str],
        logging_config: Union[Dict, str] = {},
        storage_options: Optional[Dict] = {}
    )
Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import json
from pathlib import Path
from typing import Optional, Union

from fastapi.encoders import jsonable_encoder
from prefect import flow
from prefect.task_runners import SequentialTaskRunner

from echodataflow.aspects.singleton_echodataflow import Singleton_Echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.pipeline import Recipe
from echodataflow.utils import log_util
from echodataflow.utils.config_utils import (check_config,
                                             handle_storage_options,
                                             parse_dynamic_parameters,
                                             parse_yaml_config)

from .subflows.initialization_flow import init_flow


@flow(name="Echodataflow", task_runner=SequentialTaskRunner())
def echodataflow_trigger(
    dataset_config: Union[dict, str, Path],
    pipeline_config: Union[dict, str, Path],
    logging_config: Union[dict, str, Path] = None,
    storage_options: Optional[dict] = None,
    options: Optional[dict] = {},
    json_data_path: Union[str, Path] = None,
):
    """
    Trigger the initialization and execution of the processing pipeline.

    Args:
        dataset_config (Union[dict, str, Path]): Configuration for the dataset to be
        processed.
        pipeline_config (Union[dict, str, Path]): Configuration for the processing
        pipeline.
        logging_config (Union[dict, str, Path], optional): Configuration for logging.
        Defaults to None.
        storage_options (Optional[dict], optional): Takes block_name and type, similar to yaml.
        Defaults to {}.
        options: Optional[dict]: Set of options for centralized configuration.
        Defaults to {}
        json_data_path (str, Path): Takes external json data path to process the files.
        Defaults to None

    Returns:
        Any: Output data from the pipeline.

    Example:
        # Define dataset, pipeline, and logging configurations
        data_config = ...
        pipeline_config = ...
        logging_config = ...

        # Trigger the pipeline
        pipeline_output = echodataflow_trigger(
            dataset_config=data_config,
            pipeline_config=pipeline_config,
            logging_config=logging_config
        )
        print("Pipeline output:", pipeline_output)
    """

    storage_options = handle_storage_options(storage_options=storage_options)
    
    dataset_config_dict = parse_yaml_config(config=dataset_config, storage_options=storage_options)
    logging_config_dict = parse_yaml_config(config=logging_config, storage_options=storage_options)
    pipeline_config_dict = parse_yaml_config(config=pipeline_config, storage_options=storage_options)
    
    if isinstance(json_data_path, Path):
        json_data_path = str(json_data_path)

    log_util.log(
        msg={
            "msg": f"Dataset Configuration Loaded For This Run",
            "mod_name": __file__,
            "func_name": "Echodataflow Trigger",
        },
        eflogging=dataset_config_dict.get("logging"),
    )
    log_util.log(
        msg={"msg": f"-" * 50, "mod_name": __file__, "func_name": "Echodataflow Trigger"},
        eflogging=dataset_config_dict.get("logging"),
    )
    log_util.log(
        msg={
            "msg": json.dumps(jsonable_encoder(dataset_config_dict)),
            "mod_name": __file__,
            "func_name": "Echodataflow Trigger",
        },
        eflogging=dataset_config_dict.get("logging"),
    )

    log_util.log(
        msg={
            "msg": f"Pipeline Configuration Loaded For This Run",
            "mod_name": __file__,
            "func_name": "Echodataflow Trigger",
        },
        eflogging=dataset_config_dict.get("logging"),
    )
    log_util.log(
        msg={"msg": f"-" * 50, "mod_name": __file__, "func_name": "Echodataflow Trigger"},
        eflogging=dataset_config_dict.get("logging"),
    )
    log_util.log(
        msg={
            "msg": json.dumps(jsonable_encoder(pipeline_config_dict)),
            "mod_name": __file__,
            "func_name": "Echodataflow Trigger",
        },
        eflogging=dataset_config_dict.get("logging"),
    )

    # Do any config checks on config dicts
    # Should be done in pydantic class
    check_config(dataset_config_dict, pipeline_config_dict)
    pipeline = Recipe(**pipeline_config_dict)
    dataset = Dataset(**dataset_config_dict)
    
    if options.get("storage_options_override", False):
        dataset.output.storage_options_dict = storage_options
        dataset.args.storage_options_dict = storage_options
        dataset.args.group.storage_options_dict = storage_options
    else:
        dataset.output.storage_options_dict = handle_storage_options(storage_options=dataset.output.storage_options)
        dataset.args.storage_options_dict = handle_storage_options(storage_options=dataset.args.storage_options)
        dataset.args.group.storage_options_dict = handle_storage_options(storage_options=dataset.args.group.storage_options)
        
    edf = Singleton_Echodataflow(log_file=logging_config_dict, pipeline=pipeline, dataset=dataset)

    dataset = parse_dynamic_parameters(dataset, options=options)

    return init_flow(config=dataset, pipeline=pipeline, json_data_path=json_data_path)