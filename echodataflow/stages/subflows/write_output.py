from typing import Dict, Optional
from prefect import flow
import xarray as xr
from prefect.task_runners import ThreadPoolTaskRunner
import zarr.sync
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import get_out_zarr, get_working_dir, get_zarr_list, isFile
from numcodecs import Zlib
import zarr.storage

@flow(task_runner=ThreadPoolTaskRunner(max_workers=1))
def write_output(groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]):
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
                if edf.out_path:

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
                        mode = "w"
                    
                    log_util.log(
                    msg={
                        "msg": f" {mode} {append_dim} For File {edf.out_path}",
                        "mod_name": __file__,
                        "func_name": group.group_name,
                    },
                    use_dask=stage.options["use_dask"],
                    eflogging=config.logging,
                    )
                                    
                    ed_list: xr.Dataset = get_zarr_list.fn(
                    transect_data=edf, storage_options=config.output.storage_options_dict
                    )[0]
                    
                    encoding = {}
                    if mode == "w":
                        for k, _ in ed_list.data_vars.items():
                            encoding[k] = {"compressor": Zlib(level=1)}
                    else:
                        encoding = None
                    
                    if stage.options and stage.options.get("compute", False): 
                        if mode == "a":  
                            store = xr.open_mfdataset([out_zarr, edf.out_path], storage_options=config.output.storage_options_dict, 
                                                      engine="zarr",
                                                      data_vars="minimal",
                                                      coords="minimal",
                                                      compat='override').compute()
                        else:
                            store = ed_list
                            
                        log_util.log(
                        msg={
                            "msg": "Computing Store",
                            "mod_name": __file__,
                            "func_name": group.group_name,
                        },
                        use_dask=stage.options["use_dask"],
                        eflogging=config.logging,
                        )
                        
                        store.to_zarr(
                            store=out_zarr,
                            mode="w",
                            consolidated=True,
                            storage_options=config.output.storage_options_dict,
                            append_dim=None,
                            synchronizer=zarr.sync.ThreadSynchronizer(),
                            encoding=encoding,
                            safe_chunks=False
                        )
                    else:
                        ed_list.to_zarr(
                            store=out_zarr,
                            mode=mode,
                            consolidated=True,
                            storage_options=config.output.storage_options_dict,
                            append_dim=append_dim,
                            synchronizer=zarr.sync.ThreadSynchronizer(),
                            encoding=encoding,
                            safe_chunks=False
                        )
                        
                        
                    if ("echodataflow_compute_Sv" in edf.stages.keys() or "edf_Sv_pipeline" in edf.stages.keys()) and "Sv_store" not in edf.stages.keys():
                        edf.stages["Sv_store"] = out_zarr                    
                    elif "echodataflow_compute_MVBS" in edf.stages.keys() and "MVBS_store" not in edf.stages.keys():
                        edf.stages["MVBS_store"] = out_zarr   
                    else:
                        edf.stages["store"] = out_zarr   
                    
    except Exception as e:
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": filename},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e
        )
        edf.error = ErrorObject(errorFlag=True, error_desc=str(e))  
    return groups