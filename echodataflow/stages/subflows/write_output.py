from typing import Dict, Optional
from prefect import flow
import xarray as xr
from prefect.task_runners import SequentialTaskRunner
import zarr.sync
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import get_out_zarr, get_working_dir, get_zarr_list, isFile
from numcodecs import Zlib
import zarr.storage

# TODO
# Hardcode to use Sequential runner
# Coerce time if required

@flow(task_runner=SequentialTaskRunner())
def write_output(groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]):      
    try:
        zarr.storage.default_compressor = Zlib(level=3)
    except Exception as e:
        print(e)
        pass
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
                    
                    ed_list.to_zarr(
                        store=out_zarr,
                        mode=mode,
                        consolidated=True,
                        storage_options=config.output.storage_options_dict,
                        append_dim=append_dim,
                        synchronizer=zarr.sync.ThreadSynchronizer(),
                        safe_chunks=False
                    )
                    
                    if prev_stage.name == "echodataflow_compute_Sv":
                        edf.stages["Sv_store"] = out_zarr                    
                    elif prev_stage.name == "echodataflow_compute_MVBS":
                        edf.stages["MVBS_store"] = out_zarr   
                    else:
                        edf.stages["store"] = out_zarr   
                    
    except Exception as e:
        print(e)
        raise e
    return groups