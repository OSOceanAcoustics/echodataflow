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

from collections import defaultdict
import json
import os
from typing import Dict, Optional
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta

from distributed import Client, LocalCluster
from echodataflow.aspects.singleton_echodataflow import Singleton_Echodataflow
from echodataflow.models.output_model import EchodataflowObject, Group, Output
from fastapi.encoders import jsonable_encoder
from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from prefect_dask import DaskTaskRunner

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.pipeline import Pipeline, ProcessingType, Recipe
from echodataflow.utils import log_util
from echodataflow.utils.config_utils import (
    club_raw_files,
    get_prefect_config_dict,
    glob_all_files,
    parse_raw_paths,
    sanitize_external_params,
)
from echodataflow.utils.file_utils import (
    cleanup,
    extract_fs,
    get_last_run_output,
    process_output_groups,
    store_json_output,
)
from echodataflow.utils.function_utils import dynamic_function_call


@flow(name="Initialization", task_runner=SequentialTaskRunner())
@echodataflow(type="FLOW")
def init_flow(pipeline: Recipe, config: Dataset, json_data_path: Optional[str] = None):
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
    output: Output = Output()
    if config.args.urlpath:
        output = get_input_from_url(json_data_path=json_data_path, config=config)
    else:
        output = get_input_from_store(config=config)

    store_json_output(output, config=config, name=config.name)

    process_list = pipeline.pipeline

    process = None
    for process in process_list:
        if process.recipe_name == pipeline.active_recipe:
            active_pipeline = process
            break

    gea = Singleton_Echodataflow.get_instance()

    prev_stage = None
    for stage in active_pipeline.stages:
        if prev_stage and stage.name not in gea.get_possible_next_functions(prev_stage.name):
            raise ValueError(
                stage.name,
                " cannot be executed after ",
                prev_stage.name,
                ". Please consider configuring rules if this validation is wrong.",
            )
        prev_stage = stage

    if pipeline.processing == ProcessingType.DISK:
        return process_stages_disk(
            active_pipeline=active_pipeline, pipeline=pipeline, config=config, output=output
        )
    elif pipeline.processing == ProcessingType.MEMORY:
        raise ValueError("In-memory flow is not yet implemented, switch back to disk processing.")
    else:
        return None


@flow(name="Disk-Processing-Flow")
def process_stages_disk(
    active_pipeline: Pipeline, pipeline: Recipe, config: Dataset, output: Output
):
    prev_stage = None
    prefect_config_dict = {}
    client: Client = None
    groups = output.group
    for stage in active_pipeline.stages:
        error_groups: Dict[str, Group] = defaultdict(Group)

        function = dynamic_function_call(stage.module, stage.name)
        prefect_config_dict = get_prefect_config_dict(stage)
        stage.options["use_dask"] = False

        if pipeline.scheduler_address is not None and pipeline.use_local_dask == False:
            if client is None:
                client = Client(pipeline.scheduler_address)
            prefect_config_dict["task_runner"] = DaskTaskRunner(address=client.scheduler.address)
            log_util.log(
                msg={"msg": f"{client}", "mod_name": __file__, "func_name": "Init Flow"},
                eflogging=config.logging,
            )
            stage.options["use_dask"] = True
            gea = Singleton_Echodataflow().get_instance()
            
            if gea.logger:
                client.subscribe_topic("echodataflow", lambda event: log_util.log_event(event=event))
        elif (
            pipeline.use_local_dask == True
            and prefect_config_dict is not None
            and prefect_config_dict.get("task_runner") is None
        ):
            if client is None:
                cluster = LocalCluster(n_workers=pipeline.n_workers, nanny=False)
                client = Client(cluster.scheduler_address)
            prefect_config_dict["task_runner"] = DaskTaskRunner(address=client.scheduler.address)
            log_util.log(
                msg={"msg": f"{client}", "mod_name": __file__, "func_name": "Init Flow"},
                eflogging=config.logging,
            )
            log_util.log(
                msg={
                    "msg": f"Scheduler at : {client.scheduler.address}",
                    "mod_name": __file__,
                    "func_name": "Init Flow",
                },
                eflogging=config.logging,
            )
            log_util.log(
                msg={
                    "msg": f"Dashboard at : {client.dashboard_link}",
                    "mod_name": __file__,
                    "func_name": "Init Flow",
                },
                eflogging=config.logging,
            )
            stage.options["use_dask"] = True
            gea = Singleton_Echodataflow().get_instance()
            
            if gea.logger:
                client.subscribe_topic("echodataflow", lambda event: log_util.log_event(event))

        if not stage.external_params:
            stage.external_params = {}
        
        if not sanitize_external_params(config, stage.external_params):
            raise ValueError(
                "Sanity Check Failed. One or more external parameters passed have a problem."
            )

        log_util.log(
            msg={"msg": f"-" * 50, "mod_name": __file__, "func_name": "Init Flow"},
            eflogging=config.logging,
        )
        log_util.log(
            msg={
                "msg": f"Executing stage : {stage}",
                "mod_name": __file__,
                "func_name": "Init Flow",
            },
            eflogging=config.logging,
        )

        prefect_config_dict["name"] = stage.name
        prefect_config_dict["flow_run_name"] = stage.name
        function = function.with_options(**prefect_config_dict)

        # Dict of Group
        groups = function(groups, config, stage, prev_stage)

        log_util.log(
            msg={
                "msg": f"Completed stage : {stage}",
                "mod_name": __file__,
                "func_name": "Init Flow",
            },
            eflogging=config.logging,
        )
        log_util.log(
            msg={"msg": f"-" * 50, "mod_name": __file__, "func_name": "Init Flow"},
            eflogging=config.logging,
        )

        groups = process_output_groups(
            name=stage.name, stage=stage, config=config, groups=groups, error_groups=error_groups
        )
        store_json_output(data=output, name=stage.name + "_output", config=config)

        store_json_output(data=error_groups, name=stage.name + "_ErroredGroups", config=config)

        log_util.log(
            msg={
                "msg": json.dumps(jsonable_encoder(output)),
                "mod_name": __file__,
                "func_name": "Init Flow",
            },
            eflogging=config.logging,
        )

        if prev_stage is not None:
            if config.output.retention == False:
                if (
                    prev_stage.options.get("save_offline") is None
                    or prev_stage.options.get("save_offline") == False
                ):
                    cleanup(config, prev_stage)
            else:
                if (
                    prev_stage.options.get("save_offline") is not None
                    and prev_stage.options.get("save_offline") == False
                ):
                    cleanup(config, prev_stage)

        prev_stage = stage

    # Close the local cluster but not the cluster hosted.
    if pipeline.scheduler_address is None and pipeline.use_local_dask == True:
        client.close()
        log_util.log(
            msg={
                "msg": f"Local Client has been closed",
                "mod_name": __file__,
                "func_name": "Init Flow",
            },
            eflogging=config.logging,
        )

    # Incase where the output is too big, this call might be expensive and not feasible on
    # systems with low memory, commenting the call.
    # output = get_last_run_output(data=output, storage_options=config.output.storage_options_dict)

    return output


def get_input_from_url(json_data_path, config: Dataset):
    file_dicts = []
    if json_data_path is None:
        if config.args.raw_json_path is None:
            total_files = glob_all_files(config=config)
            file_dicts = parse_raw_paths(all_raw_files=total_files, config=config)
            log_util.log(
                msg={
                    "msg": f"Files Found in Source",
                    "mod_name": __file__,
                    "func_name": "Init Flow",
                },
                eflogging=config.logging,
            )
            log_util.log(
                msg={
                    "msg": json.dumps(jsonable_encoder(total_files)),
                    "mod_name": __file__,
                    "func_name": "Init Flow",
                },
                eflogging=config.logging,
            )

            log_util.log(
                msg={
                    "msg": f"Files To Be Processed",
                    "mod_name": __file__,
                    "func_name": "Init Flow",
                },
                eflogging=config.logging,
            )
            log_util.log(
                msg={
                    "msg": json.dumps(jsonable_encoder(file_dicts)),
                    "mod_name": __file__,
                    "func_name": "Init Flow",
                },
                eflogging=config.logging,
            )
        data = club_raw_files(
            config=config,
            raw_dicts=file_dicts,
            raw_url_file=config.args.raw_json_path,
            json_storage_options=config.output.storage_options_dict,
        )
    else:
        file_system = extract_fs(json_data_path, storage_options=config.args.storage_options_dict)
        with file_system.open(json_data_path) as f:
            data = json.load(f)

    output: Output = Output()
    for group in data:
        for fdict in group:
            transect_num = fdict.get("transect_num")
            g = output.group.get(transect_num, Group())
            g.group_name = transect_num
            g.instrument = fdict.get("instrument")

            obj = EchodataflowObject(
                file_path=fdict.get("file_path"),
                month=str(fdict.get("month")),
                year=str(fdict.get("year")),
                jday=str(fdict.get("jday")),
                datetime=fdict.get("datetime"),
                group_name=transect_num,
                filename=os.path.basename(fdict.get("file_path")).split(".", maxsplit=1)[0],
            )
            g.data.append(obj)

            output.group[transect_num] = g
    return output


def get_input_from_store(config: Dataset):
    
    store_path = config.args.store_path
    
    store: xr.Dataset = xr.open_zarr(store_path, storage_options=config.output.storage_options_dict)
    samples = store.sizes['ping_time']
    
    if samples >= config.args.window_size:
        
        store_time = pd.to_datetime(store['ping_time'].values)
        end_time = store_time[-1]
        
        start_time = end_time - timedelta(hours=config.args.time_travel_hours, minutes=config.args.time_travel_mins)

        sliced_store = store.sel(ping_time=slice(start_time, end_time))
        
        store_time = pd.to_datetime(sliced_store['ping_time'].values)
        n_frames = len(store_time)

        indices = []
        current_end = n_frames
        current_start = max(0, current_end - config.args.window_size)
        while current_start > 0:    
            current_start = max(0, current_end - config.args.window_size)            
            if current_end - current_start == config.args.window_size:
                start_time = pd.to_datetime(sliced_store['ping_time'].values[current_start], unit="ns")
                end_time = pd.to_datetime(sliced_store['ping_time'].values[current_end - 1], unit="ns")
                indices.append((start_time, end_time))
                
            current_end = current_end - config.args.rolling_size
            
        output: Output = Output()
        for stime, etime in indices:
            g = output.group.get("DefaultGroup", Group())
            g.group_name = "DefaultGroup"
            g.instrument = config.sonar_model
            obj = EchodataflowObject(
                file_path=" ",   
                group_name="DefaultGroup",
                filename=f"win_{int(stime.timestamp())}_{int(etime.timestamp())}",
                start_time=stime.isoformat(timespec='nanoseconds'),
                end_time=etime.isoformat(timespec='nanoseconds')
            )
            obj.stages['store'] = store_path
            g.data.append(obj)

            output.group["DefaultGroup"] = g
        return output
    else:
        raise ValueError("Not enough frames to process, try reducing the window size")
        