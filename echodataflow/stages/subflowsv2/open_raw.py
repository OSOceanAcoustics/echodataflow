"""
Echodataflow Open Raw Stage

This module defines a Prefect Flow and associated tasks for the Echodataflow Open Raw stage.
The stage involves processing raw sonar data files, converting them to zarr format, and
organizing the processed data based on transects.

Classes:
    None

Functions:
    echodataflow_open_raw(config: Dataset, stage: Stage, data: Union[str, List[List[Dict[str, Any]]]])
    process_raw(raw, working_dir: str, config: Dataset, stage: Stage)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from typing import Optional
import dask
import dask.bag as db
from echopype import open_raw
from prefect import task

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import EchodataflowObject, ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util



@dask.delayed
@echodataflow(processing_stage="Open-Raw")
def echodataflow_open_raw(group: Group, config: Dataset, stage: Stage, prev_stage: Optional[Stage]):
    
    
    if any([ed.error.errorFlag for ed in group.data]):
        return group
    
    # Create a Dask Bag from the list of files
    bag = db.from_sequence(group.data)    
    # Map the open_file function to each file in the bag
    processed_files = bag.map(open_file, group=group, config=config, stage=stage)
    
    # Compute the bag to execute operations    
    group.data = processed_files.compute()  # This will execute in parallel across your Dask cluster
    
    return group


@echodataflow(processing_stage="Open-Raw")
def open_file(file: EchodataflowObject, group: Group, config: Dataset, stage: Stage):
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
        ed = open_raw(raw_file=file.file_path, sonar_model=group.instrument,
                    storage_options=config.args.storage_options_dict)
        file.stages[stage.name] = ed
    
    except Exception as e:
        file.error = ErrorObject(errorFlag=True, error_desc=str(e))
    finally:        
        log_util.log(
            msg={"msg": f" ---- Exiting ----", "mod_name": __file__, "func_name": file.filename},
            use_dask=True,
            eflogging=config.logging,
        )
        return file
    

# @task()
# @echodataflow()
# def process_raw(raw, working_dir: str, config: Dataset, stage: Stage):
#     """
#     Process a single raw sonar data file.

#     Args:
#         raw (str): Path to the raw data file.
#         working_dir (str): Working directory for processing.
#         config (Dataset): Configuration for the dataset being processed.
#         stage (Stage): Configuration for the current processing stage.

#     Returns:
#         Dict[str, Any]: Processed output information.

#     Example:
#         # Define raw data, working directory, and configurations
#         raw_file = "path/to/raw_data.raw"
#         working_directory = "path/to/working_dir"
#         dataset_config = ...
#         pipeline_stage = ...

#         # Process the raw data
#         processed_output = process_raw(
#             raw=raw_file,
#             working_dir=working_directory,
#             config=dataset_config,
#             stage=pipeline_stage
#         )
#         print("Processed output:", processed_output)
#     """
    
#     log_util.log(msg={'msg':f' ---- Entering ----', 'mod_name':__file__, 'func_name':os.path.basename(raw.get("file_path"))}, use_dask=stage.options['use_dask'], eflogging=config.logging)
   
#     temp_file = download_temp_file(raw, working_dir, stage, config)
#     local_file = temp_file.get("local_path")
#     local_file_name = os.path.basename(temp_file.get("local_path"))
#     try:
#         log_util.log(msg={'msg':f'Dowloaded RAW file at {local_file}', 'mod_name':__file__, 'func_name':local_file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
#         out_zarr = get_out_zarr(group = stage.options.get('group', True), working_dir=working_dir, transect=str(
#                 raw.get("transect_num")), file_name=local_file_name.replace(".raw", ".zarr"), storage_options=config.output.storage_options_dict)
        
#         log_util.log(msg={'msg':f'Processing RAW file, output will be at {out_zarr}', 'mod_name':__file__, 'func_name':local_file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
        
#         if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
#             log_util.log(msg={'msg':f'File not found in the destination folder / use_offline flag is False.', 'mod_name':__file__, 'func_name':local_file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
#             ed = open_raw(raw_file=local_file, sonar_model=raw.get(
#                 "instrument"), storage_options=config.output.storage_options_dict)
            
#             log_util.log(msg={'msg':f'Converting to Zarr', 'mod_name':__file__, 'func_name':local_file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
#             ed.to_zarr(
#                 save_path=str(out_zarr),
#                 overwrite=True,
#                 output_storage_options=config.output.storage_options_dict,
#                 compute=False
#             )
#             del ed
            
#             if stage.options.get("save_raw_file") == False:
#                 log_util.log(msg={'msg':f'Deleting local raw file since `save_raw_file` flag is false', 'mod_name':__file__, 'func_name':local_file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
#                 local_file.unlink()
#         else:
#             log_util.log(msg={'msg':f'Skipped processing {local_file_name}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False', 'mod_name':__file__, 'func_name':local_file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
                
#         log_util.log(msg={'msg':f' ---- Exiting ----', 'mod_name':__file__, 'func_name':local_file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
#         return {'out_path': out_zarr, 'transect': raw.get("transect_num"), 'file_name': local_file_name, 'error': False}
#     except Exception as e:
#         return {'transect': raw.get("transect_num"), 'file_name': local_file_name, 'error': True, 'error_desc': e}   
