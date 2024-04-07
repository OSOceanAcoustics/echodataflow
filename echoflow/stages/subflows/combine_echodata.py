"""
Echoflow Combine Echodata Stage

This module defines a Prefect Flow and associated tasks for the Echoflow Combine Echodata stage.
The stage involves combining echodata files into a single zarr file, organized by transects.

Classes:
    None

Functions:
    echoflow_combine_echodata(config: Dataset, stage: Stage, data: List[Output])
    process_combine_echodata(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import os
from typing import Any, Dict, List, Optional, Union

from echopype import combine_echodata, echodata, open_converted
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
@echoflow(processing_stage="Combine-Echodata", type="FLOW")
def echoflow_combine_echodata(
    config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    """
    Combine echodata files into a single zarr file organized by transects.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        List[Output]: List of combined outputs organized by transects.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        processed_data = ...

        # Execute the Echoflow Combine Echodata stage
        combined_outputs = echoflow_combine_echodata(
            config=dataset_config,
            stage=pipeline_stage,
            data=processed_data
        )
        print("Combined outputs:", combined_outputs)
    """
    futures = []
    outputs: List[Output] = []

    data: List[Output] = get_output()

    working_dir = get_working_dir(config=config, stage=stage)

    if type(data) == list:
        if type(data[0].data) == list:
            for output_obj in data:
                transect = str(output_obj.data[0].get("transect")) + ".zarr"
                process_combine_echodata_wo = process_combine_echodata.with_options(
                    name=transect, task_run_name=transect, retries=3
                )
                future = process_combine_echodata_wo.submit(
                    config=config, stage=stage, out_data=output_obj.data, working_dir=working_dir
                )
                futures.append(future)
        else:
            future = process_combine_echodata.submit(
                config=config, stage=stage, out_data=data, working_dir=working_dir)
            futures.append(future)

        ed_list = [f.result() for f in futures]
        outputs = process_output_transects(name=stage.name, config=config, stage=stage, ed_list=ed_list)
    return outputs


@task
@echoflow()
def process_combine_echodata(
    config: Dataset,
    stage: Stage,
    out_data: Union[List[Dict], List[Output]],
    working_dir: str
):
    """
    Process and combine echodata files into a single zarr file.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[List[Dict], List[Output]]): List of processed outputs to be combined.
        working_dir (str): Working directory for processing.

    Returns:
        Output: Combined output information.

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and combine echodata
        combined_output = process_combine_echodata(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print("Combined output:", combined_output)
    """
    log_util.log(msg={'msg':f' ---- Entering ----', 'mod_name':__file__, 'func_name':'Combine Echodata'}, use_dask=stage.options['use_dask'], eflogging=config.logging)
       
    ed_list = []
    if type(out_data) == list and type(out_data[0]) == dict:
        file_name = str(out_data[0].get("transect"))
        out_zarr = get_out_zarr(group = stage.options.get('group', True), working_dir=working_dir, transect=str(out_data[0].get(
            "transect")), file_name=file_name + ".zarr", storage_options=config.output.storage_options_dict)        
        try:    
            log_util.log(msg={'msg':f'Processing file, output will be at {out_zarr}', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
            if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
                log_util.log(msg={'msg':f'File not found in the destination folder / use_offline flag is False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
                ed_list = get_ed_list.fn(
                    config=config, stage=stage, transect_data=out_data)
                
                log_util.log(msg={'msg':f'Combining Zarr', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
                
                ceds = combine_echodata(echodata_list=ed_list)
            
                log_util.log(msg={'msg':f'Converting to Zarr', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
                ceds.to_zarr(
                    save_path=out_zarr,
                    overwrite=True,
                    output_storage_options=dict(
                        config.output.storage_options_dict),
                    compute=False
                )
                del ceds
            else:
                log_util.log(msg={'msg':f'Skipped processing {file_name}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
                
            log_util.log(msg={'msg':f' ---- Exiting ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
            return {'out_path': out_zarr, 'transect': file_name, 'file_name': file_name + ".zarr", 'error': False}
        except Exception as e:
            return {'transect': file_name, 'file_name': file_name + ".zarr", 'error': True, 'error_desc': e}
    else:        
        out_zarr = get_out_zarr(group = stage.options.get('group', True), working_dir=working_dir, transect="default", file_name="Default_Transect.zarr", storage_options=config.output.storage_options_dict)
        try:
            log_util.log(msg={'msg':f'Processing file, output will be at {out_zarr}', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
            if stage.options.get("use_offline") == False or isFile(out_zarr) == False:
            
                log_util.log(msg={'msg':f'File not found in the destination folder / use_offline flag is False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
                for output_obj in out_data:
                    ed_list.extend(get_ed_list.fn(
                        config=config, stage=stage, transect_data=output_obj))
                    
                log_util.log(msg={'msg':f'Combining Zarr', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
                
                ceds = combine_echodata(echodata_list=ed_list)
                
                log_util.log(msg={'msg':f'Converting to Zarr', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
                
                ceds.to_zarr(
                    save_path=out_zarr,
                    overwrite=True,
                    output_storage_options=dict(
                        config.output.storage_options_dict),
                    compute=False
                )
                del ceds
            else:
                log_util.log(msg={'msg':f'Skipped processing {file_name}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
                
            log_util.log(msg={'msg':f' ---- Exiting ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            return {'out_path': out_zarr, 'transect': "Default_Transect",
                            'file_name': 'Default_Transect.zarr', 'error': False}
        except Exception as e:
            return {'transect': 'Default_Transect', 'file_name': 'Default_Transect.zarr', 'error': True, 'error_desc': e}
