"""
Echodataflow Frequency_differencing Task

This module defines a Prefect Flow and associated tasks for the Echodataflow Frequency_differencing stage.

Classes:
    None

Functions:
    echodataflow_frequency_differencing(config: Dataset, stage: Stage, data: Union[str, List[Output]])
    process_frequency_differencing(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

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
from echodataflow.utils.file_utils import (
    get_working_dir,
    get_zarr_list,
    isFile,
    get_out_zarr,
)


@flow
@echodataflow(processing_stage="frequency-differencing", type="FLOW")
def echodataflow_frequency_differencing(
    groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    """
    frequency differencing from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        List[Output]: List of The input dataset with the frequency differencing data added.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        echodata_outputs = ...

        # Execute the Echodataflow frequency_differencing stage
        frequency_differencing_output = echodataflow_frequency_differencing(
            config=dataset_config,
            stage=pipeline_stage,
            data=echodata_outputs
        )
        print("Output :", frequency_differencing_output)
    """
    working_dir = get_working_dir(config=config, stage=stage)

    futures = defaultdict(list)

    for name, gr in groups.items():
        for ed in gr.data:
            gname = ed.out_path.split(".")[0] + ".freqdiff"
            new_processed_raw = process_frequency_differencing.with_options(
                task_run_name=gname, name=gname, retries=3
            )
            future = new_processed_raw.submit(
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
def process_frequency_differencing(
    ed: EchodataflowObject, config: Dataset, stage: Stage, working_dir: str
):
    """
    Process and frequency differencing from Echodata object into the dataset.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[Dict, Output]): Processed outputs (xr.Dataset) to frequency differencing.
        working_dir (str): Working directory for processing.

    Returns:
        The input dataset with the frequency differencing data added

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and frequency differencing
        frequency_differencing_output = process_frequency_differencing(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print(" Output :", frequency_differencing_output)
    """
    file_name = ed.filename + "_fd.zarr"
    try:
        log_util.log(
            msg={"msg": f" ---- Entering ----", "mod_name": __file__, "func_name": ed.filename},
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
                "func_name": ed.filename,
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
                    "func_name": ed.filename,
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            ed_list = get_zarr_list.fn(
                transect_data=ed, storage_options=config.output.storage_options_dict
            )

            log_util.log(
                msg={
                    "msg": f"Computing frequency_differencing",
                    "mod_name": __file__,
                    "func_name": ed.filename,
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            xr_d = ep.mask.frequency_differencing(
                source_Sv=ed_list[0],
                freqABEq=stage.external_params.get("freqABEq"),
                storage_options=config.output.storage_options_dict,
            )

            log_util.log(
                msg={"msg": f"Converting to Zarr", "mod_name": __file__, "func_name": ed.filename},
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

        ed.stages["mask"] = out_zarr
        ed.error = ErrorObject(errorFlag=False)
    except Exception as e:
        ed.error = ErrorObject(errorFlag=True, error_desc=e)
    finally:
        return ed
