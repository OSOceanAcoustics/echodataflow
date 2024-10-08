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
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import scipy
import torch
import xarray as xr
import re
from distributed import Client, LocalCluster
from fastapi.encoders import jsonable_encoder
from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from prefect_dask import DaskTaskRunner

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.aspects.singleton_echodataflow import Singleton_Echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import (EchodataflowObject, Group,
                                              Metadata, Output)
from echodataflow.models.pipeline import Pipeline, ProcessingType, Recipe
from echodataflow.utils import log_util
from echodataflow.utils.config_utils import (club_raw_files, floor_time,
                                             get_prefect_config_dict,
                                             glob_all_files, glob_url,
                                             parse_raw_paths,
                                             sanitize_external_params)
from echodataflow.utils.file_utils import (cleanup, extract_fs, fetch_slice_from_store,
                                           get_last_run_output, get_out_zarr,
                                           process_output_groups,
                                           store_json_output)
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
    elif config.args.storepath:
        output = get_input_from_store(config=config)
    elif config.args.storefolder:
        output = get_input_from_store_folder(config=config)

    # store the json only of the first element
    # copy(deep=True), config=config, name=config.name)

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

        if stage.name == "process_mask_prediction_tensor":
            if "flow_run_name" in prefect_config_dict.keys():
                prefect_config_dict.pop("flow_run_name")
            if "task_runner" in prefect_config_dict.keys():
                prefect_config_dict.pop("task_runner")
            
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
        output.group = groups
        
        store_json_output(data=output.model_copy(deep=True), name=stage.name + "_output", config=config)

        store_json_output(data=error_groups, name=stage.name + "_ErroredGroups", config=config)

        log_util.log(
            msg={
                "msg": json.dumps(jsonable_encoder(output)),
                "mod_name": __file__,
                "func_name": "Init Flow",
            },
            eflogging=config.logging,
        )

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

    cleanup(output=output, config=config, pipeline=active_pipeline)
    
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

            file_info = os.path.basename(fdict.get("file_path")).split(".", maxsplit=1)
            
            obj = EchodataflowObject(
                file_path=fdict.get("file_path"),
                month=str(fdict.get("month")),
                year=str(fdict.get("year")),
                jday=str(fdict.get("jday")),
                datetime=fdict.get("datetime"),
                group_name=transect_num,
                filename=file_info[0],
                file_extension= file_info[-1],
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
                filename=f"win_{stime.strftime('D%Y%m%d-T%H%M%S')}_{etime.strftime('D%Y%m%d-T%H%M%S')}",
                start_time=stime.isoformat(timespec='nanoseconds'),
                end_time=etime.isoformat(timespec='nanoseconds'),
                file_extension="zarr"
            )
            obj.stages['store'] = store_path
            g.data.append(obj)

            output.group["DefaultGroup"] = g
        return output
    else:
        raise ValueError("Not enough frames to process, try reducing the window size")
        

def get_input_from_store_folder(config: Dataset):

    """
    Handles cases:
    1(a). When MVBS needs to point to a single Sv store
    1(b). When prediction needs to point to a single MVBS store
    2. When prediction needs to point to 2 MVBS stores and combine them
    3. When passing one or two stores with score/mask integration
    """
    
    curr_time = datetime.now(timezone.utc)
    end_time = curr_time - timedelta(hours=config.args.time_travel_hours, minutes=config.args.time_travel_mins)
    end_time = end_time.replace(second=0, microsecond=0)

    combo_output = Output()
    
    if isinstance(config.args.store_folder, str):
        # Case 1: Single store folder (1(a) and 1(b))
        store = config.args.store_folder
        return process_store_folder(config, store, end_time)
    
    else:
        # Multiple store folders (Cases 2 and 3)
        stores = config.args.store_folder['datastore']
        num_stores = len(stores)

        if num_stores == 1:
            # Case 1: Single store with optional score
            store_output = process_store_folder(config, stores[0], end_time)
            combo_output = apply_scores_if_needed(config, store_output, end_time)
            
        elif num_stores == 2:
            # Case 2: Combine two stores and possibly apply score        
            combo_output = Output()
            
            store_18 = config.args.storefolder['datastore'][0]
            store_5 = config.args.storefolder['datastore'][1]
                    
            store_18_output = process_store_folder(config, store_18, end_time)
            store_5_output = process_store_folder(config, store_5, end_time)
            
            for name, gr in store_18_output.group.items():
                
                edf_18 = gr.data[0]
                store_18 = fetch_slice_from_store(edf_group=gr, config=config, start_time=edf_18.start_time, end_time=edf_18.end_time)
                
                if not store_5_output.group.get(name):
                    raise ValueError(f"No window found in MVBS store (5 channels); window missing -> {name}")
                
                edf_5 = store_5_output.group[name].data[0]          
                store_5 = fetch_slice_from_store(edf_group=store_5_output.group[name], config=config, start_time=edf_5.start_time, end_time=edf_5.end_time)
                
                edf_5.data_ref, edf_5.data = combine_datasets(store_18, store_5)
                
                # Since we have already loaded the datasets into memory we do not need fetching again
                if gr.metadata and gr.metadata.is_store_folder:
                    gr.metadata.is_store_folder = False
                
                combo_output.group[name] = gr.model_copy()
                combo_output.group[name].data = [edf_5]
                
                log_util.log(
                        msg={"msg": f"{ name }", "mod_name": __file__, "func_name": "Mask"},
                        use_dask=False,
                        eflogging=config.logging,
                    )

                for dim, size in edf_5.data.dims.items():
                    log_util.log(
                        msg={"msg": f"{ dim } : {size}", "mod_name": __file__, "func_name": "Mask"},
                        use_dask=False,
                        eflogging=config.logging,
                    )
                    
            combo_output = apply_scores_if_needed(config, combo_output, end_time)
            
        return combo_output
    
def apply_scores_if_needed(config: Dataset, store_output: Output, end_time: datetime) -> Output:
    """
    Applies score or mask integration to the dataset if scores are provided.
    """
    if config.args.store_folder.get('scores') is not None:
        score_store = config.args.store_folder['scores'][0]
        score_output = process_store_folder(config, score_store, end_time)
            
        for name, gr in store_output.group.items():
            edf = gr.data[0]
            
            if edf.data is None:
                edf.data = fetch_slice_from_store(edf_group=gr, config=config, start_time=edf.start_time, end_time=edf.end_time)
            
            score_edf = score_output.group[name].data[0]
            score_ds = fetch_slice_from_store(edf_group=score_edf.group[name], config=config, start_time=score_edf.start_time, end_time=score_edf.end_time)
            
            # Align time between store and score
            min_time = max(edf.data.ping_time.min(), score_ds.ping_time.min())
            max_time = min(edf.data.ping_time.max(), score_ds.ping_time.max())

            edf.data = edf.data.sel(ping_time=slice(min_time, max_time))
            score_ds = score_ds.sel(ping_time=slice(min_time, max_time))            
            
            # Apply softmax or other score-based logic
            softmax = xr.apply_ufunc(scipy.special.softmax, score_ds, kwargs={'axis': 0}, dask="allowed")
            
            # resampling
            edf.data = edf.data.resample(ping_time="30s").mean()
            softmax = softmax.resample(ping_time="30s").mean()
            
            edf.data = edf.data.assign(softmax=softmax.sel(species="hake")["__xarray_dataarray_variable__"])           

    return store_output


def process_xrd(ds: xr.Dataset, freq_wanted = [120000, 38000, 18000]) -> xr.Dataset:
    ds = ds.sel(depth=slice(None, 590))        
        
    ch_wanted = [int((np.abs(ds["frequency_nominal"]-freq)).argmin()) for freq in freq_wanted]
    ds = ds.isel(
                channel=ch_wanted
            )
    return ds

def combine_datasets(store_18: xr.Dataset, store_5: xr.Dataset) -> Tuple[torch.Tensor, xr.Dataset]:
    ds_32k_120k = None
    ds_18k = None
    combined_ds = None
    try:
        partial_channel_name = ["ES18"]
        ds_18k = extract_channels(store_18, partial_channel_name)        
        partial_channel_name = ["ES38", "ES120"]
        ds_32k_120k = extract_channels(store_5, partial_channel_name)
    except Exception as e:
        partial_channel_name = ["ES18"]
        ds_18k = extract_channels(store_5, partial_channel_name)
        partial_channel_name = ["ES38", "ES120"]
        ds_32k_120k = extract_channels(store_18, partial_channel_name)
        
    if not ds_18k or not ds_32k_120k:
        raise ValueError("Could not find the required channels in the datasets")
    
    ds_18k = process_xrd(ds_18k, freq_wanted=[18000])
    ds_32k_120k = process_xrd(ds_32k_120k, freq_wanted=[120000, 38000])
    
    combined_ds = xr.merge([ds_18k["Sv"], ds_32k_120k["Sv"], 
                            ds_18k['latitude'], ds_18k['longitude'],
                            ds_18k["frequency_nominal"], ds_32k_120k["frequency_nominal"]
                            ])
    combined_ds.attrs = ds_32k_120k.attrs

    combined_ds = (
        combined_ds
        .transpose("channel", "depth", "ping_time")
        .sel(depth=slice(None, 590))
    )

    depth = combined_ds['depth']
    ping_time = combined_ds['ping_time']

    # Create a tensor with R=120 kHz, G=38 kHz, B=18 kHz mapping
    red_channel = extract_channels(combined_ds, ["ES120"])
    green_channel = extract_channels(combined_ds, ["ES38"])
    blue_channel = extract_channels(combined_ds, ["ES18"])

    tensor = xr.concat([red_channel, green_channel, blue_channel], dim='channel')
    tensor['channel'] = ['R', 'G', 'B']
    tensor = tensor.assign_coords({'depth': depth, 'ping_time': ping_time})

    mvbs_tensor = torch.tensor(tensor['Sv'].values, dtype=torch.float32)
    
    return (mvbs_tensor, combined_ds)

def extract_channels(dataset: xr.Dataset, partial_names: List[str]) -> xr.Dataset:
    """
    Extracts multiple channels data from the given xarray dataset using partial channel names.

    Args:
        dataset (xr.Dataset): The input xarray dataset containing multiple channels.
        partial_names (List[str]): The list of partial names of the channels to extract.

    Returns:
        xr.Dataset: The dataset containing only the specified channels data.
    """
    matching_channels = []
    for partial_name in partial_names:
        matching_channels.extend([channel for channel in dataset.channel.values if partial_name in str(channel)])
    
    if len(matching_channels) == 0:
        raise ValueError(f"No channels found matching any of '{partial_names}'")
    
    return dataset.sel(channel=matching_channels)

def process_store_folder(config: Dataset, store: str, end_time: datetime):
    output: Output = Output()
    
    files = sorted(glob_url(path=store, storage_options=config.args.storage_options_dict))
    
    relevant_files = {}
    timestamps = []
    
    if len(files) == 0:
        raise ValueError("No files found in the store folder")
    
    for file in files:
        try:
            basename = os.path.basename(file)
            match = re.search(r'D(\d{8})-T(\d{6})', basename)
            if match:
                date_str = match.group(1)
                time_str = match.group(2)
                file_time = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                relevant_files[file_time] = file
                timestamps.append(file_time)
            
        except ValueError:
            continue
    
    if len(timestamps) == 0:
        raise ValueError('No files detected at source')

    if config.args.time_rounding_flag:
        end_time = floor_time(end_time, config.args.window_mins)
    
    for _ in range(config.args.number_of_windows):
        start_time = end_time - timedelta(hours=config.args.window_hours, minutes=config.args.window_mins)
        start_time = start_time.replace(second=0, microsecond=0)        
        
        start_index = 0
        i = 1
        for i, ts in enumerate(timestamps):        
            if ts >= start_time:
                start_index += i - 1
                break
        else:
            start_index += i - 1 
        
        if start_index <= 0:
            if timestamps[0] <= start_time:
                start_index = max(start_index, 0)
            else:
                end_time = start_time
                continue
        
        end_index = len(timestamps)

        for i, ts in enumerate(reversed(timestamps)):
            if ts <= end_time:
                end_index -= i + 1
                break
        
        relevant_timestamps = timestamps[start_index:end_index+1]
    
        win_relevant_files = [relevant_files.get(key) for key in list(relevant_files.keys()) if key in relevant_timestamps]
        
        gname = f"win_{start_time.strftime('D%Y%m%d-T%H%M%S')}_{end_time.strftime('D%Y%m%d-T%H%M%S')}"
        
        log_util.log(
            msg={
                "msg": f"Range is {start_time} to {end_time}; Found {len(win_relevant_files)} -> {win_relevant_files}" ,
                "mod_name": __file__,
                "func_name": "Init Flow",
            },
            eflogging=config.logging,
        )

        for fpath in win_relevant_files:    
            g = output.group.get(gname, Group())
            g.group_name = gname
            g.instrument = config.sonar_model
            
            g.metadata = Metadata(instrument=config.sonar_model, group_name=gname, is_store_folder=True)
            
            obj = EchodataflowObject(
                out_path=fpath,
                group_name=gname,
                filename="Hake-"+str(start_time.strftime('D%Y%m%d-T%H%M%S')),
                start_time= start_time.replace(tzinfo=None).isoformat(),
                end_time=end_time.replace(tzinfo=None).isoformat()
            )
            g.data.append(obj)

            output.group[gname] = g
        end_time = start_time
        
    return output