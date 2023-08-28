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
import logging
from typing import Any, Dict

import dask
from distributed import Client, LocalCluster

from echoflow.config.models.datastore import Dataset
from echoflow.config.models.pipeline import Recipe
from echoflow.stages.aspects.singleton_echoflow import Singleton_Echoflow
from echoflow.stages.aspects.echoflow_aspect import echoflow
from echoflow.stages.utils.config_utils import club_raw_files, get_prefect_config_dict, glob_all_files, parse_raw_paths
from echoflow.stages.utils.function_utils import dynamic_function_call

from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import *
from prefect_dask import DaskTaskRunner


@flow(name="Main-Flow", task_runner=SequentialTaskRunner())
@echoflow(type="FLOW")
def init_flow(
        pipeline: Recipe,
        dataset: Dataset
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

    if dataset.args.raw_json_path is None:
        total_files = glob_all_files(config=dataset)
        file_dicts = parse_raw_paths(all_raw_files=total_files, config=dataset)

    data = club_raw_files(
        config=dataset,
        raw_dicts=file_dicts,
        raw_url_file=dataset.args.raw_json_path,
        json_storage_options=dataset.output.storage_options_dict
    )

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
                cluster = LocalCluster(n_workers=3)
                client = Client(cluster.scheduler_address)
            prefect_config_dict["task_runner"] = DaskTaskRunner(
                address=client.scheduler.address)
            print(client)

        function = function.with_options(**prefect_config_dict)
        print("-"*50)
        print("\nExecuting stage : ", stage)
        output = function(dataset, data, stage, prev_stage)
        data = output
        print(output)
        print("\nCompleted stage", stage)
        print("-"*50)
        prev_stage = stage

    # Close the local cluster but not the cluster hosted.
    if pipeline.scheduler_address is None and pipeline.use_local_dask == True:
        client.close()
        print("Local Client has been closed")
    return output
