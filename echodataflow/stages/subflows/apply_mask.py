
"""
Echodataflow Apply_mask Task

This module defines a Prefect Flow and associated tasks for the Echodataflow Apply_mask stage.

Classes:
    None

Functions:
    echodataflow_apply_mask(config: Dataset, stage: Stage, data: Union[str, List[Output]])
    process_apply_mask(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from collections import defaultdict
from typing import Dict, Optional

from echopype.utils.io import validate_source
from echopype.mask import apply_mask
from prefect import flow, task
import xarray as xr
from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import EchodataflowObject, ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import (
    get_working_dir,
    get_zarr_list,
    isFile,
    get_out_zarr,
)


@flow
@echodataflow(processing_stage="apply-mask", type="FLOW")
def echodataflow_apply_mask(
    groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    """
    apply mask from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        List[Output]: List of The input dataset with the apply mask data added.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        echodata_outputs = ...

        # Execute the Echodataflow apply_mask stage
        apply_mask_output = echodataflow_apply_mask(
            config=dataset_config,
            stage=pipeline_stage,
            data=echodata_outputs
        )
        print("Output :", apply_mask_output)
    """
    working_dir = get_working_dir(config=config, stage=stage)

    futures = defaultdict(list)

    for name, gr in groups.items():
        for ed in gr.data:
            gname = ed.out_path.split(".")[0] + ".applymask"
            new_process = process_apply_mask.with_options(
                task_run_name=gname, name=gname, retries=3
                    )
            future = new_process.submit(
                ed=ed, working_dir=working_dir, config=config, stage=stage
                    )
            futures[name].append(future)

    for name, flist in futures.items():
        try:
            groups[name].data = [f.result() for f in flist]
        except Exception as e:
            groups[name].data[0].error = ErrorObject(errorFlag=True, error_desc=str(e))

    return groups


@task
@echodataflow()
def process_apply_mask(ed: EchodataflowObject, config: Dataset, stage: Stage, working_dir: str):
    """
    Process and apply mask from Echodata object into the dataset.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[Dict, Output]): Processed outputs (xr.Dataset) to apply mask.
        working_dir (str): Working directory for processing.

    Returns:
        The input dataset with the apply mask data added

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and apply mask
        apply_mask_output = process_apply_mask(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print(" Output :", apply_mask_output)
    """
    file_name = ed.filename + "_applymask.zarr"
    try:
        log_util.log(
            msg={"msg": " ---- Entering ----", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )

        out_zarr = get_out_zarr(
            group=stage.options.get("group", True),
            working_dir=working_dir,
            transect=ed.group_name,
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
                msg={
                    "msg": f"Computing apply_mask",
                    "mod_name": __file__,
                    "func_name": file_name,
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )
        
            input_feed = defaultdict(str)
            if stage.dependson:
                for _, v in stage.dependson.items():
                    input_feed[v] = ed.stages.get("mask")

            mask, file_type = validate_source(input_feed["mask"], config.output.storage_options_dict)
            mask: xr.DataArray = xr.open_dataarray(mask, engine=file_type, chunks={}, **config.output.storage_options_dict)
            
            if "depth" in ed_list[0].coords:
                ed_list[0] = ed_list[0].sel(depth=slice(None, 590))
            
            # temporary fix
            if "range_sample" not in ed_list[0].coords:
                ed_list[0] = ed_list[0].swap_dims({"depth": "range_sample"}).rename_vars({"depth": "range_sample"})
                mask = mask.swap_dims({"depth": "range_sample"})
            
            external_kwargs = stage.external_params                
                
            xr_d = apply_mask(
                source_ds=ed_list[0],
                mask=mask,
                storage_options_ds=config.output.storage_options_dict,
                storage_options_mask=config.output.storage_options_dict,
                **external_kwargs
            )
            log_util.log(
                msg={"msg": f"Converting to Zarr", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            xr_d.compute().to_zarr(
                store=out_zarr,
                mode="w",
                consolidated=True,
                storage_options=config.output.storage_options_dict,
            )

        else:
            log_util.log(
                msg={
                    "msg": f"Skipped processing {ed.filename}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False",
                    "mod_name": __file__,
                    "func_name": ed.filename,
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

        log_util.log(
            msg={"msg": f" ---- Exiting ----", "mod_name": __file__, "func_name": ed.filename},
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
