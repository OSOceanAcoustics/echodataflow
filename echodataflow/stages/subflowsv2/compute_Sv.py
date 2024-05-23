"""
Echodataflow Compute SV Stage

This module defines a Prefect Flow and associated tasks for the Echodataflow Compute SV stage.
The stage involves computing the Sv (volume backscattering strength) from echodata.

Classes:
    None

Functions:
    echodataflow_compute_Sv(config: Dataset, stage: Stage, data: List[Output])
    process_compute_Sv(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from typing import Optional

import dask
import echopype as ep
from prefect import task

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import EchodataflowObject, ErrorObject, Group
from echodataflow.models.pipeline import Stage
import dask.bag as db

from echodataflow.utils import log_util

@dask.delayed
@echodataflow(processing_stage="Compute-Sv")
def echodataflow_compute_Sv(group: Group, config: Dataset, stage: Stage, prev_stage: Optional[Stage]):
    # Create a Dask Bag from the list of files
    bag = db.from_sequence(group.data)
    
    # Map the open_file function to each file in the bag
    processed_files = bag.map(compute_sv, group=group, config=config, stage=stage, prev_stage=prev_stage)
    
    # Compute the bag to execute operations    
    group.data = processed_files.compute()  # This will execute in parallel across your Dask cluster
    
    return group        
   

@echodataflow(processing_stage="Compute-Sv")
def compute_sv(file: EchodataflowObject, group: Group, config: Dataset, stage: Stage, prev_stage: Stage):
    
    # Function to open a single file
    try:
        log_util.log(
        msg={
            "msg": f" ---- Entering ----",
            "mod_name": __file__,
            "func_name": file.filename,
        },
        use_dask=True,
        eflogging=config.logging,
        )
        log_util.log(
        msg={
            "msg": f" ---- Entering ----",
            "mod_name": __file__,
            "func_name": file.filename,
        },
        use_dask=True,
        eflogging=config.logging,
        )
        sv = ep.calibrate.compute_Sv(echodata=file.stages.get(prev_stage.name))        
        file.stages[stage.name] = sv
        del file.stages[prev_stage.name]
    except Exception as e:
        file.error = ErrorObject(errorFlag=True, error_desc=str(e))
    finally:        
        return file

# @task
# @echodataflow()
# def process_compute_Sv(
#     config: Dataset, stage: Stage, out_data: Union[Dict, Output], working_dir: str
# ):
#     """
#     Process and compute volume backscattering strength (Sv) from echodata.

#     Args:
#         config (Dataset): Configuration for the dataset being processed.
#         stage (Stage): Configuration for the current processing stage.
#         out_data (Union[Dict, Output]):  Processed echodata output.
#         working_dir (str): Working directory for processing.

#     Returns:
#         Output: Computed Sv output information.

#     Example:
#         # Define configuration, processed data, and working directory
#         dataset_config = ...
#         pipeline_stage = ...
#         processed_outputs = ...
#         working_directory = ...

#         # Process and compute Sv
#         computed_sv_output = process_compute_Sv(
#             config=dataset_config,
#             stage=pipeline_stage,
#             out_data=processed_outputs,
#             working_dir=working_directory
#         )
#         print("Computed Sv output:", computed_sv_output)
#     """
    
#     if type(out_data) == dict:
#         file_name = str(out_data.get("file_name")).split(".")[0] + "_SV.zarr"
#         transect = str(out_data.get("transect")) 
#     else:
#         file_name = str(out_data.data.get("file_name")).split(".")[0] + "_SV.zarr"
#         transect = str(out_data.data.get("transect"))
#     try:
#         log_util.log(msg={'msg':f' ---- Entering ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
#         out_zarr = get_out_zarr(group = stage.options.get('group', True), working_dir=working_dir, transect=transect, file_name=file_name, storage_options=config.output.storage_options_dict)
        
#         log_util.log(msg={'msg':f'Processing file, output will be at {out_zarr}', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
#         if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
            
#             log_util.log(msg={'msg':f'File not found in the destination folder / use_offline flag is False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
#             ed_list = get_ed_list.fn(
#                     config=config, stage=stage, transect_data=out_data)
            
#             log_util.log(msg={'msg':f'Computing SV', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
#             xr_d_sv = ep.calibrate.compute_Sv(echodata=ed_list[0])
            
#             log_util.log(msg={'msg':f'Converting to Zarr', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
#             xr_d_sv.to_zarr(store=out_zarr, mode="w", consolidated=True,
#                             storage_options=config.output.storage_options_dict)
#         else:
#             log_util.log(msg={'msg':f'Skipped processing {file_name}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
#         log_util.log(msg={'msg':f' ---- Exiting ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
#         return {'out_path': out_zarr, 'transect': transect, 'file_name': file_name, 'error': False}
#     except Exception as e:
#         return {'transect': transect, 'file_name': file_name, 'error': True, 'error_desc': e}   
