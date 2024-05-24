
"""
Echodataflow Add_location Task

This module defines a Prefect Flow and associated tasks for the Echodataflow Add_location stage.

Classes:
    None

Functions:
    echodataflow_add_location(config: Dataset, stage: Stage, data: Union[str, List[Output]])
    process_add_location(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from collections import defaultdict
from typing import Dict, Optional

import echopype as ep
from prefect import flow, task

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import EchodataflowObject, ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import get_ed_list, get_out_zarr, get_working_dir, isFile



@flow
@echodataflow(processing_stage="add-location", type="FLOW")
def echodataflow_add_location(
        groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    """
    add location from echodata.

    Args:
        groups: (Dict): Dictionary of group objects to be processed.
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        Dict[str, Group]: Dictionary of groups with the add location data added.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        echodata_outputs = ...

        # Execute the Echodataflow add_location stage
        add_location_output = echodataflow_add_location(
            config=dataset_config,
            stage=pipeline_stage,
            data=echodata_outputs
        )
        print("Output :", add_location_output)
    """

    working_dir = get_working_dir(stage=stage, config=config)

    futures = defaultdict(list)

    for name, gr in groups.items():
        for ed in gr.data:
            gname = ed.out_path.split(".")[0] + ".Addlocation"
            new_process = process_add_location.with_options(
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
def process_add_location(
    ed: EchodataflowObject, config: Dataset, stage: Stage, working_dir: str
):
    """
    Process and add location from Echodata object into the dataset.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[Dict, Output]): Processed outputs (xr.Dataset) to add location.
        working_dir (str): Working directory for processing.

    Returns:
        The input dataset with the add location data added

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and add location
        add_location_output = process_add_location(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print(" Output :", add_location_output)
    """

    file_name = ed.filename + "_add location.zarr"

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

            ed_list = get_ed_list.fn(config=config, stage=stage, transect_data=ed)

            log_util.log(
                msg={"msg": f"Computing SV", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            xr_d = ep.consolidate.add_location(ds=ed_list[0], echodata=stage.external_params.get('echodata'), nmea_sentence=stage.external_params.get('nmea_sentence'))

            log_util.log(
                msg={"msg": f"Converting to Zarr", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            xr_d.to_zarr(
                store=out_zarr,
                mode="w",
                consolidated=True,
                storage_options=config.output.storage_options_dict,
            )
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
    except Exception as e:
        ed.error = ErrorObject(errorFlag=True, error_desc=e)
    finally:
        return ed  
