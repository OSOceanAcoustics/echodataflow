"""
Echodataflow Compute MVBS Stage

This module defines a Prefect Flow and associated tasks for the Echodataflow Compute MVBS stage.
The stage involves computing the MVBS (Mean Volume Backscattering Strength) from echodata.

Classes:
    None

Functions:
    echodataflow_compute_MVBS(config: Dataset, stage: Stage, data: Union[str, List[Output]])
    process_compute_MVBS(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""

from collections import defaultdict
import logging
from typing import Dict, Optional

import dask
from distributed import as_completed
import echopype as ep
from prefect import flow, task
import xarray as xr
import pandas as pd

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import EchodataflowObject, ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import get_out_zarr, get_working_dir, get_zarr_list, isFile


@flow
@echodataflow(processing_stage="Compute-MVBS", type="FLOW")
def echodataflow_compute_MVBS(
    groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    """
    Compute Mean Volume Backscattering Strength (MVBS) from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        List[Output]: List of computed MVBS outputs.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        echodata_outputs = ...

        # Execute the Echodataflow Compute MVBS stage
        computed_mvbs_outputs = echodataflow_compute_MVBS(
            config=dataset_config,
            stage=pipeline_stage,
            data=echodata_outputs
        )
        print("Computed MVBS outputs:", computed_mvbs_outputs)
    """
    working_dir = get_working_dir(stage=stage, config=config)

    futures = defaultdict(list)

    for name, gr in groups.items():
        if gr.metadata and gr.metadata.is_store_folder and len(gr.data) > 0:
            edf = gr.data[0]
            store = xr.open_mfdataset(paths=[ed.out_path for ed in gr.data], engine="zarr",
                                        combine="by_coords",
                                        data_vars="minimal",
                                        coords="minimal",
                                        compat="override").compute()
            edf.data = store.sel(ping_time=slice(pd.to_datetime(edf.start_time, unit="ns"), pd.to_datetime(edf.end_time, unit="ns")))
            gr.data = [edf]
            del store
            
        # TODO ed.out_path.split(".")[0] -> change to filename
        
        for ed in gr.data:
            gname = ed.out_path.split(".")[0] + ".MVBS"
            new_process = process_compute_mvbs.with_options(
                task_run_name=gname, name=gname, retries=3
            )
            future = new_process.submit(
                ed=ed, working_dir=working_dir, config=config, stage=stage
            )
            futures[name].append(future)

    for name, flist in futures.items():
        try:
            results = []
            for f in flist:
                res = f.result()
                results.append(res)
                del f
            groups[name].data = results
        except Exception as e:
            groups[name].data[0].error = ErrorObject(errorFlag=True, error_desc=str(e))
        del res
        del results

    return groups


@task
@echodataflow()
def process_compute_mvbs(ed: EchodataflowObject, config: Dataset, stage: Stage, working_dir: str):
    """
    Process and compute Mean Volume Backscattering Strength (MVBS) from xarray dataset.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[Dict, Output]): Processed outputs (xr.Dataset) to compute MVBS.
        working_dir (str): Working directory for processing.

    Returns:
        Output: Computed MVBS output information.

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and compute MVBS
        computed_mvbs_output = process_compute_MVBS(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print("Computed MVBS output:", computed_mvbs_output)
    """
    file_name = ed.filename + "_MVBS.zarr"
    transect = ed.group_name
    try:
        log_util.log(
            msg={"msg": " ---- Entering ----", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )

        out_zarr = get_out_zarr(
            group=stage.options.get("group", True),
            working_dir=working_dir,
            transect=transect,
            file_name=file_name,
            storage_options=config.output.storage_options_dict,
        )

        log_util.log(
            msg={
                "msg": f"Processing file, output will be at {out_zarr}",
                "mod_name": __file__,
                "func_name": file_name,
            },
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )

        if (
            stage.options.get("use_offline") == False
            or isFile(out_zarr, config.output.storage_options_dict) == False
        ):
            log_util.log(
                msg={
                    "msg": f"File not found in the destination folder / use_offline flag is False",
                    "mod_name": __file__,
                    "func_name": file_name,
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            ed_list = get_zarr_list.fn(
                transect_data=ed, storage_options=config.output.storage_options_dict
            )

            log_util.log(
                msg={"msg": f"Computing MVBS", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            external_kwargs = stage.external_params                
            
            xr_d_mvbs: xr.Dataset = ep.commongrid.compute_MVBS(
                ds_Sv=ed_list[0],
                **external_kwargs
            )
            log_util.log(
                msg={"msg": f"Converting to Zarr", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            xr_d_mvbs.to_zarr(
                store=out_zarr,
                mode="w",
                consolidated=True,
                storage_options=config.output.storage_options_dict,
            )
            del xr_d_mvbs
            del ed_list
        else:
            log_util.log(
                msg={
                    "msg": f"Skipped processing {file_name}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False",
                    "mod_name": __file__,
                    "func_name": file_name,
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

        log_util.log(
            msg={"msg": f" ---- Exiting ----", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )
        ed.out_path = out_zarr
        ed.error = ErrorObject(errorFlag=False)
        ed.stages[stage.name] = out_zarr
    except Exception as e:
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e
        )
        ed.error = ErrorObject(errorFlag=True, error_desc=str(e))
    finally:
        return ed
