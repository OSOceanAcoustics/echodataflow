
"""
Echoflow Apply_mask Task

This module defines a Prefect Flow and associated tasks for the Echoflow Apply_mask stage.

Classes:
    None

Functions:
    echoflow_apply_mask(config: Dataset, stage: Stage, data: Union[str, List[Output]])
    process_apply_mask(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from collections import defaultdict
import os
from typing import Dict, List, Optional, Union

import echopype as ep
from prefect import flow, task

from echoflow.aspects.echoflow_aspect import echoflow
from echoflow.models.datastore import Dataset
from echoflow.models.output_model import Output
from echoflow.models.pipeline import Stage
from echoflow.utils import log_util
from echoflow.utils.file_utils import (get_output, get_working_dir,
                                    get_zarr_list, isFile,
                                    process_output_transects, get_out_zarr)


@flow
@echoflow(processing_stage="apply-mask", type="FLOW")
def echoflow_apply_mask(
        config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    """
    apply mask from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        List[Output]: List of The input dataset with the apply mask data added.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        echodata_outputs = ...

        # Execute the Echoflow apply_mask stage
        apply_mask_output = echoflow_apply_mask(
            config=dataset_config,
            stage=pipeline_stage,
            data=echodata_outputs
        )
        print("Output :", apply_mask_output)
    """
    data: Union[str, List[Output]] = get_output()
    outputs: List[Output] = []
    futures = []
    working_dir = get_working_dir(config=config, stage=stage)

    if type(data) == list:
        if type(data[0].data) == list:
            for output_data in data:
                transect_list = output_data.data
                for ed in transect_list:
                    transect = str(ed.get("out_path")).split(".")[0] + ".Applymask"
                    process_apply_mask_wo = process_apply_mask.with_options(
                        name=transect, task_run_name=transect, retries=3
                    )
                    future = process_apply_mask_wo.submit(
                        config=config, stage=stage, out_data=ed, working_dir=working_dir
                    )
                    futures.append(future)
        else:
            for output_data in data:
                future = process_apply_mask.submit(
                    config=config, stage=stage, out_data=output_data, working_dir=working_dir
                )
                futures.append(future)

        ed_list = [f.result() for f in futures]
        outputs = process_output_transects(name=stage.name, config=config, stage=stage, ed_list=ed_list)
    return outputs


@task
@echoflow()
def process_apply_mask(
    config: Dataset, stage: Stage, out_data: Union[Dict, Output], working_dir: str
):
    """
    Process and apply mask from Echodata object into the dataset.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[Dict, Output]): Processed outputs (xr.Dataset) to apply mask.
        working_dir (str): Working directory for processing.

    Returns:
        The input dataset with the apply mask data added

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and apply mask
        apply_mask_output = process_apply_mask(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print(" Output :", apply_mask_output)
    """
    
    if type(out_data) == dict:
        file_name = str(out_data.get("file_name"))
        transect = str(out_data.get("transect"))        
    else:
        file_name = str(out_data.data.get("file_name"))
        transect = str(out_data.data.get("transect"))
    try:
        log_util.log(msg={'msg':f' ---- Entering ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)

        out_zarr = get_out_zarr(group = stage.options.get('group', True), working_dir=working_dir, transect=transect, file_name=file_name, storage_options=config.output.storage_options_dict)


        log_util.log(msg={'msg':f'Processing file, output will be at {out_zarr}', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)

        if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
            log_util.log(msg={'msg':f'File not found in the destination folder / use_offline flag is False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)

            ed_list = get_zarr_list.fn(transect_data=out_data, storage_options=config.output.storage_options_dict)

            log_util.log(msg={'msg':f'Computing apply_mask', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
            input_feed = defaultdict(str)
            if stage.dependson:
                for _, v in stage.dependson.items():
                    input_feed[v] = out_data.get(v)
            print(input_feed)                
            
            xr_d = ep.mask.apply_mask(source_ds = ed_list[0], mask=input_feed['mask'], storage_options_ds=config.output.storage_options_dict,
                                    storage_options_mask=config.output.storage_options_dict)

            log_util.log(msg={'msg':f'Converting to Zarr', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)

            xr_d.to_zarr(store=out_zarr, mode="w", consolidated=True,
                            storage_options=config.output.storage_options_dict)

        else:
            log_util.log(msg={'msg':f'Skipped processing {file_name}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)

        log_util.log(msg={'msg':f' ---- Exiting ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)

        return {'out_path': out_zarr, 'transect': transect, 'file_name': file_name, 'error': False}
    except Exception as e:
        return {'transect': transect, 'file_name': file_name, 'error': True, 'error_desc': e}        
