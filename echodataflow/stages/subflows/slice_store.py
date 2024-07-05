
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
import pandas as pd

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import get_out_zarr, get_working_dir


@flow
@echodataflow(processing_stage="slice-store", type="FLOW")
def slice_store(groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]):  
    
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

                identifier = None
                # out_path will have sv and 'store' will have path to store
                if edf.stages.get("Sv_store"):
                    identifier = "sv"
                    storepath = edf.stages.get("Sv_store")
                elif edf.stages.get("MVBS_store"):
                    identifier = "mvbs"
                    storepath = edf.stages.get("MVBS_store")
                else:
                    identifier = "store"
                    storepath = edf.stages.get("store")
                
                if stage.dependson:
                    for k, v in stage.dependson.items():
                        if k == "store":
                            log_util.log(
                                msg={
                                    "msg": f"Working on {v}",
                                    "mod_name": __file__,
                                    "func_name": "write_output",
                                },
                                use_dask=stage.options["use_dask"],
                                eflogging=config.logging,
                            )
                            if v.lower() == "mvbs":
                                identifier = "mvbs"
                                storepath = edf.stages.get("MVBS_store")
                            elif v.lower() == "sv":
                                identifier = "sv"
                                storepath = edf.stages.get("Sv_store")

                log_util.log(
                    msg={
                        "msg": f"Working on {identifier}",
                        "mod_name": __file__,
                        "func_name": "write_output",
                    },
                    use_dask=stage.options["use_dask"],
                    eflogging=config.logging,
                )
                
                nextsub = None
                store = xr.open_zarr(storepath, storage_options=config.output.storage_options_dict)                
                
                if edf.start_time and edf.end_time:
                    log_util.log(
                        msg={
                            "msg": f"Start Time: {edf.start_time} End Time: {edf.end_time}",
                            "mod_name": __file__,
                            "func_name": "write_output",
                        },
                        use_dask=stage.options["use_dask"],
                        eflogging=config.logging,
                    )
                    nextsub = store.sel(ping_time=slice(pd.to_datetime(edf.start_time, unit="ns"), pd.to_datetime(edf.end_time, unit="ns")))
                else:
                    # get last processed index
                    var: Variable = Variable.get(name="last_"+ identifier +"_index", default=Variable(name="last_"+ identifier +"_index", value=0))
                    log_util.log(
                        msg={
                            "msg": f"Slicing from last known index {var.value}",
                            "mod_name": __file__,
                            "func_name": "write_output",
                        },
                        use_dask=stage.options["use_dask"],
                        eflogging=config.logging,
                    )
                    # slice the store from the last processed index
                    sub = store.isel(ping_time=slice(int(var.value), None))
                    
                    # resample sub and store the new last processed index
                    sam = sub['ping_time'].resample(ping_time=stage.external_params.get("ping_time_bin", "10s"))                
                    last_v = max([v.start for v in sam.groups.values()])
                    log_util.log(
                        msg={
                            "msg": f"Slicing till {last_v}",
                            "mod_name": __file__,
                            "func_name": "write_output",
                        },
                        use_dask=stage.options["use_dask"],
                        eflogging=config.logging,
                    )
                    nextsub = sub.isel(ping_time=slice(0, last_v))
                    
                time_diffs = pd.Series(nextsub['ping_time'].values).diff()

                one_hour = pd.Timedelta(hours=stage.external_params.get('ping_diff_threshold', 1))
                time_diff_exceeds_one_hour = time_diffs > one_hour
                
                log_util.log(
                    msg={
                        "msg": f"Writing to zarr at {out_zarr}",
                        "mod_name": __file__,
                        "func_name": "write_output",
                    },
                    use_dask=stage.options["use_dask"],
                    eflogging=config.logging,
                )
    
                nextsub.compute().to_zarr(
                    store=out_zarr,
                    mode="w",
                    consolidated=True,
                    storage_options=config.output.storage_options_dict,
                    safe_chunks=False,                    
                )
                
                if any(time_diff_exceeds_one_hour):
                        log_util.log(
                            msg={
                                "msg": f"Time difference between some pings exceeded the set threshold of {stage.external_params.get('ping_diff_threshold', 1)} hour(s)",
                                "mod_name": __file__,
                                "func_name": "write_output",
                            },
                            use_dask=stage.options["use_dask"],
                            eflogging=config.logging,
                        )
                        edf.error = ErrorObject(errorFlag=True, 
                                                error_desc=f"Time difference between any two pings crossed the threshold of {stage.external_params.get('ping_diff_threshold', 1)} hour(s)", 
                                                error_type="INTERNAL")        
                else:
                    edf.out_path = out_zarr
                    edf.error = ErrorObject(errorFlag=False)     
                
                if not edf.start_time or not edf.end_time:
                    Variable.set(name="last_"+ identifier +"_index", value=str(last_v + int(var.value)), overwrite=True)           
                edf.stages[stage.name] = out_zarr
    except Exception as e:
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e
        )
        edf.error = ErrorObject(errorFlag=True, error_desc=str(e))  
    return groups