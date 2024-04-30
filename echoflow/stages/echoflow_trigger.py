"""
Echoflow Trigger Flow

This module defines the Prefect Flow that acts as a trigger to initialize and execute the processing pipeline.

Functions:
    echoflow_trigger(
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
from typing import Any, Dict, Optional, Union
from fastapi.encoders import jsonable_encoder

from prefect import flow
from prefect.blocks.core import Block
from prefect.task_runners import SequentialTaskRunner

from echoflow.aspects.singleton_echoflow import Singleton_Echoflow
from echoflow.models.datastore import Dataset
from echoflow.models.pipeline import Recipe
from echoflow.utils import log_util
from echoflow.utils.config_utils import (check_config, extract_config,
                                         get_storage_options, load_block)

from .subflows.initialization_flow import init_flow


@flow(name="Echoflow-Trigger", task_runner=SequentialTaskRunner())
def echoflow_trigger(
    dataset_config: Union[dict, str, Path],
    pipeline_config: Union[dict, str, Path],
    logging_config: Union[dict, str, Path] = None,
    storage_options: Optional[dict] = None,
    options: Optional[dict] = {},
    json_data_path: Union[str, Path] = None
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
        pipeline_output = echoflow_trigger(
            dataset_config=data_config,
            pipeline_config=pipeline_config,
            logging_config=logging_config
        )
        print("Pipeline output:", pipeline_output)
    """
    if storage_options is not None:
        # Check if storage_options is a Block (fsspec storage) and convert it to a dictionary
        if isinstance(storage_options, dict) and storage_options.get("block_name") is not None:
            block = load_block(
                name=storage_options.get("block_name"), type=storage_options.get("type"))
            storage_options = get_storage_options(block)
        
    else:
        storage_options = {}
    
    if isinstance(dataset_config, Path):
        dataset_config = str(dataset_config)
    if isinstance(logging_config, Path):
        logging_config = str(logging_config)
    if isinstance(pipeline_config, Path):
        pipeline_config = str(pipeline_config)
    if isinstance(json_data_path, Path):
        json_data_path = str(json_data_path)

    if isinstance(dataset_config, str):
        if not dataset_config.endswith((".yaml", ".yml")):
            raise ValueError("Configuration file must be a YAML!")
        dataset_config_dict = extract_config(dataset_config, storage_options)
    elif isinstance(dataset_config, dict):
        dataset_config_dict = dataset_config

    if isinstance(pipeline_config, str):
        if not pipeline_config.endswith((".yaml", ".yml")):
            raise ValueError("Configuration file must be a YAML!")
        pipeline_config_dict = extract_config(pipeline_config, storage_options)
    elif isinstance(pipeline_config, dict):
        pipeline_config_dict = pipeline_config

    if isinstance(logging_config, str):
        if not logging_config.endswith((".yaml", ".yml")):
            raise ValueError("Configuration file must be a YAML!")
        logging_config_dict = extract_config(logging_config, storage_options)
    else:
        logging_config_dict = logging_config

    
    log_util.log(msg={'msg':f'Dataset Configuration Loaded For This Run', 'mod_name':__file__, 'func_name':'Echoflow Trigger'}, eflogging=dataset_config_dict.get('logging'))
    log_util.log(msg={'msg':f'-'*50, 'mod_name':__file__, 'func_name':'Echoflow Trigger'}, eflogging=dataset_config_dict.get('logging'))
    log_util.log(msg={'msg':json.dumps(jsonable_encoder(dataset_config_dict)), 'mod_name':__file__, 'func_name':'Echoflow Trigger'}, eflogging=dataset_config_dict.get('logging'))
    print(dataset_config_dict)
    
    log_util.log(msg={'msg':f'Pipeline Configuration Loaded For This Run', 'mod_name':__file__, 'func_name':'Echoflow Trigger'}, eflogging=dataset_config_dict.get('logging'))
    log_util.log(msg={'msg':f'-'*50, 'mod_name':__file__, 'func_name':'Echoflow Trigger'}, eflogging=dataset_config_dict.get('logging'))
    log_util.log(msg={'msg':json.dumps(jsonable_encoder(pipeline_config_dict)), 'mod_name':__file__, 'func_name':'Echoflow Trigger'}, eflogging=dataset_config_dict.get('logging'))
    
    # Do any config checks on config dicts
    # Should be done in pydantic class
    check_config(dataset_config_dict, pipeline_config_dict)
    pipeline = Recipe(**pipeline_config_dict)
    dataset = Dataset(**dataset_config_dict)    

    if options.get('storage_options_override') is not None and options['storage_options_override'] is False:
        storage_options = {}
    if not storage_options:
        if dataset.output.storage_options is not None:
            if dataset.output.storage_options.anon is False:
                block = load_block(
                    name=dataset.output.storage_options.block_name,
                    type=dataset.output.storage_options.type)
                dataset.output.storage_options_dict = get_storage_options(
                    block)
            else:
                dataset.output.storage_options_dict = {
                    "anon": dataset.output.storage_options.anon}

        if dataset.args.storage_options is not None:
            if dataset.args.storage_options.anon is False:
                block = load_block(
                    name=dataset.args.storage_options.block_name, type=dataset.args.storage_options.type)
                dataset.args.storage_options_dict = get_storage_options(block)
            else:
                dataset.args.storage_options_dict = {
                    "anon": dataset.args.storage_options.anon}
        if dataset.args.group is not None:
            if dataset.args.group.storage_options is not None:
                if dataset.args.group.storage_options.anon is False:
                    block = load_block(
                        name=dataset.args.group.storage_options.block_name, type=dataset.args.group.storage_options.type)
                    dataset.args.group.storage_options_dict = get_storage_options(
                        block)
                else:
                    dataset.args.group.storage_options_dict = {
                        "anon": dataset.args.group.storage_options.anon}
    else:
        dataset.output.storage_options_dict = storage_options
        dataset.args.storage_options_dict = storage_options
        dataset.args.group.storage_options_dict = storage_options

    print("\nInitiliazing Singleton Object")
    Singleton_Echoflow(log_file=logging_config_dict,
                       pipeline=pipeline, dataset=dataset)
    
    print("\nReading Configurations")
    return init_flow(config=dataset, pipeline=pipeline, json_data_path=json_data_path)
