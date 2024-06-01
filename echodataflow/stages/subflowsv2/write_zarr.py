import gc
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
from echodataflow.utils.file_utils import extract_fs, get_out_zarr, get_working_dir, isFile

@dask.delayed
def write_zarr(group: Group, config: Dataset, stage: Stage, prev_stage: Optional[Stage]):
    log_util.log(
            msg={
                "msg": f" ---- Entering ----",
                "mod_name": __file__,
                "func_name": group.group_name,
            },
            use_dask=True,
            eflogging=config.logging,
        )
    working_dir = get_working_dir(stage=stage, config=config)
    
    for edf in group.data:
        if edf.error and not edf.error.errorFlag and edf.stages.get(prev_stage.name):
            if stage.options and stage.options.get('store_mode', False):
                filename = config.name + ".zarr"
                mode = "a"      
                append_dim='ping_time'
            else:
                filename = edf.filename + ".zarr"
                mode = "w"
                append_dim = None
                
            out_zarr = get_out_zarr(
                group=stage.options.get("group", True),
                working_dir=working_dir,
                transect=str(group.group_name),
                file_name= filename,
                storage_options=config.output.storage_options_dict,
            )
            if not isFile(out_zarr, config.output.storage_options_dict):
                append_dim = None
            
            log_util.log(
            msg={
                "msg": f" {mode} {append_dim}",
                "mod_name": __file__,
                "func_name": group.group_name,
            },
            use_dask=True,
            eflogging=config.logging,
            )
            
            edf.stages[prev_stage.name].to_zarr(
                store=out_zarr,
                mode=mode,
                consolidated=True,
                storage_options=config.output.storage_options_dict,
                append_dim=append_dim,
            )
            edf.out_path = out_zarr
            edf.stages[stage.name] = edf.stages[prev_stage.name]   
            del edf.stages[prev_stage.name] 
    return group