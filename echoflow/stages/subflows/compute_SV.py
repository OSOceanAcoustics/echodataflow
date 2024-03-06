"""
Echoflow Compute SV Stage

This module defines a Prefect Flow and associated tasks for the Echoflow Compute SV stage.
The stage involves computing the Sv (volume backscattering strength) from echodata.

Classes:
    None

Functions:
    echoflow_compute_SV(config: Dataset, stage: Stage, data: List[Output])
    process_compute_SV(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import os
from typing import Any, Dict, List, Optional, Union

import echopype as ep
from prefect import flow, task

from echoflow.aspects.echoflow_aspect import echoflow
from echoflow.models.datastore import Dataset
from echoflow.models.output_model import Output
from echoflow.models.pipeline import Stage
from echoflow.utils import log_util
from echoflow.utils.file_utils import (get_ed_list, get_out_zarr, get_output,
                                       get_working_dir, isFile,
                                       process_output_transects)


@flow
@echoflow(processing_stage="Compute-SV", type="FLOW")
def echoflow_compute_SV(config: Dataset, stage: Stage, prev_stage: Optional[Stage]):
    """
    Compute volume backscattering strength (Sv) from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        List[Output]: List of computed Sv outputs.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        echodata_outputs = ...

        # Execute the Echoflow Compute SV stage
        computed_sv_outputs = echoflow_compute_SV(
            config=dataset_config,
            stage=pipeline_stage,
            data=echodata_outputs
        )
        print("Computed Sv outputs:", computed_sv_outputs)
    """
    data: List[Output] = get_output()
    outputs: List[Output] = []
    futures = []
    working_dir = get_working_dir(config=config, stage=stage)
    # [Output(data=[{'out_path': '/Users/soham/Desktop/EchoWorkSpace/echoflow/notebooks/combined_files/echoflow_combine_echodata/7.zarr', 'transect': 7}], passing_params={}), Output(data=[{'out_path': '/Users/soham/Desktop/EchoWorkSpace/echoflow/notebooks/combined_files/echoflow_combine_echodata/10.zarr', 'transect': 10}], passing_params={})]

    if type(data) == list:
        if type(data[0].data) == list:
            for output_data in data:
                transect_list = output_data.data
                for ed in transect_list:
                    transect = str(ed.get("out_path")).split(".")[0] + ".SV"
                    process_compute_SV_wo = process_compute_SV.with_options(
                        name=transect, task_run_name=transect, retries=3
                    )
                    future = process_compute_SV_wo.submit(
                        config=config, stage=stage, out_data=ed, working_dir=working_dir
                    )
                    futures.append(future)
        else:
            for output_data in data:
                future = process_compute_SV.submit(
                    config=config, stage=stage, out_data=output_data, working_dir=working_dir
                )
                futures.append(future)

        ed_list = [f.result() for f in futures]
        outputs = process_output_transects(name=stage.name, config=config, ed_list=ed_list)

    return outputs



@task
@echoflow()
def process_compute_SV(
    config: Dataset, stage: Stage, out_data: Union[Dict, Output], working_dir: str
):
    """
    Process and compute volume backscattering strength (Sv) from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[Dict, Output]):  Processed echodata output.
        working_dir (str): Working directory for processing.

    Returns:
        Output: Computed Sv output information.

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and compute Sv
        computed_sv_output = process_compute_SV(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print("Computed Sv output:", computed_sv_output)
    """
    
    if type(out_data) == dict:
        file_name = str(out_data.get("file_name")).split(".")[0] + "_SV.zarr"
        transect = str(out_data.get("transect")) 
    else:
        file_name = str(out_data.data.get("file_name")).split(".")[0] + "_SV.zarr"
        transect = str(out_data.data.get("transect"))
        
    log_util.log(msg={'msg':f' ---- Entering ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
    
    out_zarr = get_out_zarr(group = stage.options.get('group', True), working_dir=working_dir, transect=transect, file_name=file_name, storage_options=config.output.storage_options_dict)
    
    log_util.log(msg={'msg':f'Processing file, output will be at {out_zarr}', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
    
    if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
        
        log_util.log(msg={'msg':f'File not found in the destination folder / use_offline flag is False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
        ed_list = get_ed_list.fn(
                config=config, stage=stage, transect_data=out_data)
        
        log_util.log(msg={'msg':f'Computing SV', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
        xr_d_sv = ep.calibrate.compute_Sv(echodata=ed_list[0])
        
        log_util.log(msg={'msg':f'Converting to Zarr', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
        xr_d_sv.to_zarr(store=out_zarr, mode="w", consolidated=True,
                        storage_options=config.output.storage_options_dict)
    else:
        log_util.log(msg={'msg':f'Skipped processing {file_name}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
    log_util.log(msg={'msg':f' ---- Exiting ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
    return {'out_path': out_zarr, 'transect': transect, 'file_name': file_name, 'error': False}
