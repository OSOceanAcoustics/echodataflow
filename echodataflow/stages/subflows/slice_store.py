
"""
Echodataflow Slice_store Task

This module defines a Prefect Flow and associated tasks for the Echodataflow Slice_store stage.

Classes:
    None

Functions:
    echodataflow_slice_store(config: Dataset, stage: Stage, data: Union[str, List[Output]])
    process_slice_store(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from typing import Dict, Optional

from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.variables import Variable
import xarray as xr

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import get_out_zarr, get_working_dir


@flow(task_runner=SequentialTaskRunner())
@echodataflow(processing_stage="slice-store", type="FLOW")
def slice_store(groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]):  
    # TODO
    # Add more logging
    # Determine starting index for each slice beforehand to execute this parallely
    
    log_util.log(
            msg={
                "msg": f" ---- Entering ----",
                "mod_name": __file__,
                "func_name": "write_output",
            },
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )
    working_dir = get_working_dir(stage=stage, config=config)
    
    try:
        for _, group in groups.items():
            for edf in group.data:
                file_name = edf.filename + "_StoreSlice.zarr"
                
                out_zarr = get_out_zarr(
                    group=stage.options.get("group", True),
                    working_dir=working_dir,
                    transect=str(group.group_name),
                    file_name= file_name,
                    storage_options=config.output.storage_options_dict,
                )
            
                # out_path will have sv and 'store' will have path to store
                identifier = None
                if edf.stages.get("Sv_store"):
                    identifier = "sv"
                    storepath = edf.stages.get("Sv_store")
                elif edf.stages.get("MVBS_store"):
                    identifier = "mvbs"
                    storepath = edf.stages.get("MVBS_store")
                else:
                    identifier = "store"
                    storepath = edf.stages.get("store")
                
                store = xr.open_zarr(storepath, storage_options=config.output.storage_options_dict)                
                
                # get last processed index
                var: Variable = Variable.get(name="last_"+ identifier +"_index", default=Variable(name="last_"+ identifier +"_index", value=0))
                
                # slice the store from the last processed index
                sub = store.isel(ping_time=slice(int(var.value), None))
                
                # resample sub and store the new last processed index
                sam = sub['ping_time'].resample(ping_time=stage.options.get("ping_time_bin", "10s"))                
                last_v = max([v.start for v in sam.groups.values()])
                
                nextsub = sub.isel(ping_time=slice(0, last_v))
       
                nextsub.compute().to_zarr(
                    store=out_zarr,
                    mode="w",
                    consolidated=True,
                    storage_options=config.output.storage_options_dict,
                    safe_chunks=False,                    
                )
                
                edf.out_path = out_zarr
                edf.error = ErrorObject(errorFlag=False)     
                Variable.set(name="last_"+ identifier +"_index", value=str(last_v + int(var.value)), overwrite=True)           
    except Exception as e:
        print(e)
        raise e
    return groups