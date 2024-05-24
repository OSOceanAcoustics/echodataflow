"""
Echodataflow Combine Echodata Stage

This module defines a Prefect Flow and associated tasks for the Echodataflow Combine Echodata stage.
The stage involves combining echodata files into a single zarr file, organized by transects.

Classes:
    None

Functions:
    echodataflow_combine_echodata(config: Dataset, stage: Stage, data: List[Output])
    process_combine_echodata(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""

from collections import defaultdict
from typing import Dict, Optional

from echopype import combine_echodata
from prefect import flow, task

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import EchodataflowObject, ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import (
    get_ed_list,
    get_out_zarr,
    get_working_dir,
    isFile,
)


@flow
@echodataflow(processing_stage="Combine-Echodata", type="FLOW")
def echodataflow_combine_echodata(
    groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    """
    Combine echodata files into a single zarr file organized by transects.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        List[Output]: List of combined outputs organized by transects.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        processed_data = ...

        # Execute the Echodataflow Combine Echodata stage
        combined_outputs = echodataflow_combine_echodata(
            config=dataset_config,
            stage=pipeline_stage,
            data=processed_data
        )
        print("Combined outputs:", combined_outputs)
    """

    working_dir = get_working_dir(stage=stage, config=config)

    futures = defaultdict()

    for name, gr in groups.items():
        gname = gr.group_name.split(".")[0] + ".zarr"
        new_processed_raw = process_combine_echodata.with_options(
            task_run_name=gname, name=gname, retries=3
        )
        future = new_processed_raw.submit(
            group=gr, working_dir=working_dir, config=config, stage=stage
        )
        futures[name] = future

    for name, f in futures.items():
        try:
            res = f.result()
            print(res)
            groups[name] = res
        except Exception as e:
            groups[name].data[0].error = ErrorObject(errorFlag=True, error_desc=str(e))
    return groups


@task
@echodataflow()
def process_combine_echodata(group: Group, config: Dataset, stage: Stage, working_dir: str):
    """
    Process and combine echodata files into a single zarr file.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[List[Dict], List[Output]]): List of processed outputs to be combined.
        working_dir (str): Working directory for processing.

    Returns:
        Output: Combined output information.

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and combine echodata
        combined_output = process_combine_echodata(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print("Combined output:", combined_output)
    """
    file_name = group.group_name
    log_util.log(
        msg={"msg": f" ---- Entering ----", "mod_name": __file__, "func_name": "Combine Echodata"},
        use_dask=stage.options["use_dask"],
        eflogging=config.logging,
    )
    ed_list = []
    out_zarr = get_out_zarr(
        group=stage.options.get("group", True),
        working_dir=working_dir,
        transect=file_name,
        file_name=file_name + ".zarr",
        storage_options=config.output.storage_options_dict,
    )
    try:
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

            ed_list = get_ed_list.fn(config=config, stage=stage, transect_data=group.data)

            log_util.log(
                msg={"msg": f"Combining Zarr", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            ceds = combine_echodata(echodata_list=ed_list)

            log_util.log(
                msg={"msg": f"Converting to Zarr", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            ceds.to_zarr(
                save_path=out_zarr,
                overwrite=True,
                output_storage_options=dict(config.output.storage_options_dict),
                compute=False,
            )
            del ceds
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

        ed = EchodataflowObject(
            filename=group.group_name, out_path=out_zarr, group_name=file_name
        )
        ed.out_path = out_zarr
        ed.error = ErrorObject(errorFlag=False)
        group.data = [ed]
    except Exception as e:
        ed = group.data[0]
        ed.error = ErrorObject(errorFlag=True, error_desc=str(e))
    finally:
        return group
