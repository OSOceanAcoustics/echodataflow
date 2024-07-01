
"""
Echodataflow Add_depth Task

This module defines a Prefect Flow and associated tasks for the Echodataflow Add_depth stage.

Classes:
    None

Functions:
    echodataflow_add_depth(config: Dataset, stage: Stage, data: Union[str, List[Output]])
    process_add_depth(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import os
from typing import Dict, List, Optional, Union

import echopype as ep
from prefect import flow, task

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import Output
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.config_utils import sanitize_external_params
from echodataflow.utils.file_utils import (get_out_zarr, get_output, get_working_dir,
                                    get_zarr_list, isFile,
                                    process_output_groups)


@flow
@echodataflow(processing_stage="add-depth", type="FLOW")
def echodataflow_add_depth(
        config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    """
    add depth from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        List[Output]: List of The input dataset with the add depth data added.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        echodata_outputs = ...

        # Execute the Echodataflow add_depth stage
        add_depth_output = echodataflow_add_depth(
            config=dataset_config,
            stage=pipeline_stage,
            data=echodata_outputs
        )
        print("Output :", add_depth_output)
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
                    transect = str(ed.get("out_path")).split(".")[0] + ".Adddepth"
                    process_add_depth_wo = process_add_depth.with_options(
                        name=transect, task_run_name=transect, retries=3
                    )
                    future = process_add_depth_wo.submit(
                        config=config, stage=stage, out_data=ed, working_dir=working_dir
                    )
                    futures.append(future)
        else:
            for output_data in data:
                future = process_add_depth.submit(
                    config=config, stage=stage, out_data=output_data, working_dir=working_dir
                )
                futures.append(future)

        ed_list = [f.result() for f in futures]

        outputs = process_output_groups(name=stage.name, stage=stage, config=config, ed_list=ed_list)

    return outputs


@task
@echodataflow()
def process_add_depth(
    config: Dataset, stage: Stage, out_data: Union[Dict, Output], working_dir: str
):
    """
    Process and add depth from Echodata object into the dataset.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[Dict, Output]): Processed outputs (xr.Dataset) to add depth.
        working_dir (str): Working directory for processing.

    Returns:
        The input dataset with the add depth data added

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and add depth
        add_depth_output = process_add_depth(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print(" Output :", add_depth_output)
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
            
            log_util.log(msg={'msg':f'Computing TS', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
            xr_d_loc = ep.consolidate.add_depth(ds=ed_list[0], depth_offset=stage.external_params.get('depth_offset'), 
                                                tilt=stage.external_params.get('tilt'),
                                                downward=stage.external_params.get('downward'))
            log_util.log(msg={'msg':f'Converting to Zarr', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
            xr_d_loc.to_zarr(store=out_zarr, mode="w", consolidated=True,
                            storage_options=config.output.storage_options_dict)
        else:
            log_util.log(msg={'msg':f'Skipped processing {file_name}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
        log_util.log(msg={'msg':f' ---- Exiting ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        return {'out_path': out_zarr, 'transect': transect, 'file_name': file_name, 'error': False}
    except Exception as e:
        return {'transect': transect, 'file_name': file_name, 'error': True, 'error_desc': e}
