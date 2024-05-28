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
from echodataflow.utils.file_utils import extract_fs


@dask.delayed
@echodataflow(processing_stage="Open-Raw")
def echodataflow_open_raw(group: Group, config: Dataset, stage: Stage, prev_stage: Optional[Stage]):

    if any([ed.error.errorFlag for ed in group.data]):
        return group

    bag = db.from_sequence(group.data)
    
    processed_files = bag.map(open_file, group=group, config=config, stage=stage)

    group.data = processed_files.compute()
    
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
        if file.file_path.startswith("file://"):
            file.file_path = file.file_path[len("file://"):]
            
        ed = open_raw(
            raw_file=file.file_path,
            sonar_model=group.instrument,
            storage_options=config.args.storage_options_dict,
        )
        file.stages[stage.name] = ed
        log_util.log(
            msg={"msg": f" Success ", "mod_name": __file__, "func_name": file.filename},
            use_dask=True,
            eflogging=config.logging,
        )
    except Exception as e:
        log_util.log(
            msg={"msg": str(e), "mod_name": __file__, "func_name": file.filename},
            use_dask=True,
            eflogging=config.logging,
        )
        file.error = ErrorObject(errorFlag=True, error_desc=str(e))
    finally:
        log_util.log(
            msg={"msg": f" ---- Exiting ----", "mod_name": __file__, "func_name": file.filename},
            use_dask=True,
            eflogging=config.logging,
        )
        return file