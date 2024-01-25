"""
Main Prefect Flow Initialization

This module defines the main Prefect Flow for initializing and executing the processing pipeline.
It includes tasks for extracting paths for raw data files, executing processing stages, and managing Dask clusters.

Functions:
    init_flow(pipeline: Recipe, dataset: Dataset)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import json
import logging
from typing import Any, Dict

import dask
from distributed import Client, LocalCluster
from prefect import flow
from prefect.filesystems import *
from prefect.task_runners import SequentialTaskRunner
from prefect_dask import DaskTaskRunner

from echoflow.aspects.echoflow_aspect import echoflow
from echoflow.models.datastore import Dataset
from echoflow.models.pipeline import Recipe
from echoflow.utils.config_utils import (club_raw_files,
                                         get_prefect_config_dict,
                                         glob_all_files, parse_raw_paths)
from echoflow.utils.file_utils import (cleanup, extract_fs,
                                       get_last_run_output, store_json_output)
from echoflow.utils.function_utils import dynamic_function_call


@flow(name="Init-Flow", task_runner=SequentialTaskRunner())
@echoflow(type="FLOW")
def init_flow(
        pipeline: Recipe,
        config: Dataset,
        json_data_path: Optional[str] = None
):
    """
    Initialize and execute the processing pipeline using Prefect.

    Args:
        pipeline (Recipe): Configuration for the processing pipeline.
        dataset (Dataset): Configuration for the dataset to be processed.

    Returns:
        Any: Output data from the pipeline.

    Example:
        # Define pipeline and dataset configurations
        processing_pipeline = ...
        data_dataset = ...

        # Initialize and execute the processing flow
        pipeline_output = init_flow(
            pipeline=processing_pipeline,
            dataset=data_dataset
        )
        print("Pipeline output:", pipeline_output)
    """
    prefect_config_dict = {}
    file_dicts = []

    if json_data_path is None:  
        if config.args.raw_json_path is None:  
            total_files = glob_all_files(config=config)
            file_dicts = parse_raw_paths(all_raw_files=total_files, config=config)
            print("Files To Be Processed", total_files)
        data = club_raw_files(
            config=config,
            raw_dicts=file_dicts,
            raw_url_file=config.args.raw_json_path,
            json_storage_options=config.output.storage_options_dict
        )
    else:
        file_system = extract_fs(
            json_data_path, storage_options=config.args.storage_options_dict
        )
        with file_system.open(json_data_path) as f:
            data = json.load(f)

    store_json_output(data, config=config, name=config.name)

    process_list = pipeline.pipeline
    client: Client = None
    process = None
    for process in process_list:
        if process.recipe_name == pipeline.active_recipe:
            active_pipeline = process 
    
    prev_stage = None
    for stage in active_pipeline.stages:
        function = dynamic_function_call(stage.module, stage.name)
        prefect_config_dict = get_prefect_config_dict(
            stage, pipeline, prefect_config_dict)

        if pipeline.scheduler_address is not None and pipeline.use_local_dask == False:
            if client is None:
                client = Client(pipeline.scheduler_address)
            prefect_config_dict["task_runner"] = DaskTaskRunner(
                address=client.scheduler.address)
            print(client)
        elif pipeline.use_local_dask == True and prefect_config_dict is not None and prefect_config_dict.get("task_runner") is None:
            if client is None:
                cluster = LocalCluster(n_workers=pipeline.n_workers)
                client = Client(cluster.scheduler_address)
            prefect_config_dict["task_runner"] = DaskTaskRunner(
                address=client.scheduler.address)
            print(client)
            print("Scheduler at : ", client.scheduler.address)

        function = function.with_options(**prefect_config_dict)
        print("-"*50)
        print("\nExecuting stage : ", stage)
        output = function(config, stage, prev_stage)
        print("\nCompleted stage", stage)
        print("-"*50)

        store_json_output(data=output, name=stage.name+"_output", config=config)

        if prev_stage is not None:
            if config.output.retention == False:
                if (prev_stage.options.get("save_offline") is None or prev_stage.options.get("save_offline") == False):
                    cleanup(config, prev_stage, data)
            else:
                if (prev_stage.options.get("save_offline") is not None and prev_stage.options.get("save_offline") == False):
                    cleanup(config, prev_stage, data)
        
        prev_stage = stage

    # Close the local cluster but not the cluster hosted.
    if pipeline.scheduler_address is None and pipeline.use_local_dask == True:
        client.close()
        print("Local Client has been closed")
    
    output = get_last_run_output(data=output, storage_options=config.output.storage_options_dict)

    return output
