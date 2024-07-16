"""
Echodataflow Open Raw Stage

This module defines a Prefect Flow and associated tasks for the Echodataflow Open Raw stage.
The stage involves processing raw sonar data files, converting them to zarr format, and
organizing the processed data based on transects.

Classes:
    None

Functions:
    echodataflow_open_raw(config: Dataset, stage: Stage, data: Union[str, List[List[Dict[str, Any]]]])
    process_raw(raw, working_dir: str, config: Dataset, stage: Stage)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""

from collections import defaultdict
import logging
import os
from typing import Dict, Optional

from prefect import flow, task
import echopype as ep

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import EchodataflowObject, ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import (
    download_temp_file,
    extract_fs,
    get_out_zarr,
    get_working_dir,
    isFile,
)


@flow
@echodataflow(processing_stage="Sv-Pipeline", type="FLOW")
def edf_Sv_pipeline(
    groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    """
    Process raw sonar data files and convert them to zarr format.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        List[Output]: List of processed outputs organized based on transects.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        raw_data = ...

        # Execute the Echodataflow Open Raw stage
        processed_outputs = echodataflow_open_raw(
            config=dataset_config,
            stage=pipeline_stage,
            data=raw_data
        )
        print("Processed outputs:", processed_outputs)
    """

    working_dir = get_working_dir(stage=stage, config=config)

    futures = defaultdict(list)

    if stage.options.get("group") is None:
        stage.options["group"] = True

    for name, gr in groups.items():
        for raw in gr.data:
            new_processed_raw = process_Sv_pipeline.with_options(
            task_run_name=raw.file_path, name=raw.file_path, retries=3
            )
            future = new_processed_raw.submit(raw, gr, working_dir, config, stage)
            futures[name].append(future)

    
    for name, flist in futures.items():
        results = []
        res = None
        try:
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


@task()
@echodataflow()
def process_Sv_pipeline(
    raw: EchodataflowObject, group: Group, working_dir: str, config: Dataset, stage: Stage
):
    """
    Process a single raw sonar data file.

    Args:
        raw (str): Path to the raw data file.
        working_dir (str): Working directory for processing.
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.

    Returns:
        Dict[str, Any]: Processed output information.

    Example:
        # Define raw data, working directory, and configurations
        raw_file = "path/to/raw_data.raw"
        working_directory = "path/to/working_dir"
        dataset_config = ...
        pipeline_stage = ...

        # Process the raw data
        processed_output = process_raw(
            raw=raw_file,
            working_dir=working_directory,
            config=dataset_config,
            stage=pipeline_stage
        )
        print("Processed output:", processed_output)
    """
    
    log_util.log(
        msg={
            "msg": f" ---- Entering ----",
            "mod_name": __file__,
            "func_name": os.path.basename(raw.file_path),
        },
        use_dask=stage.options["use_dask"],
        eflogging=config.logging,
    )
   
    download_temp_file(raw, working_dir, stage, config)
    local_file = raw.local_path
    raw.filename = os.path.basename(raw.local_path).split(".", maxsplit=1)[0]
    file_name = raw.filename + ".zarr"
    try:
        log_util.log(
            msg={
                "msg": f"Dowloaded RAW file at {local_file}",
                "mod_name": __file__,
                "func_name": raw.filename,
            },
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )

        out_zarr = get_out_zarr(
            group=stage.options.get("group", True),
            working_dir=working_dir,
            transect=str(raw.group_name),
            file_name=file_name,
            storage_options=config.output.storage_options_dict,
        )

        log_util.log(
            msg={
                "msg": f"Processing RAW file, output will be at {out_zarr}",
                "mod_name": __file__,
                "func_name": raw.filename,
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
                    "msg": f"File not found in the destination folder / use_offline flag is False.",
                    "mod_name": __file__,
                    "func_name": raw.filename,
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )
            
            raw_kwargs = stage.external_params.get("open_raw", {})
            Sv_kwargs = stage.external_params.get("compute_Sv", {})
            depth_kwargs = stage.external_params.get("add_depth", {})
            location_kwargs = stage.external_params.get("add_location", {})
            
            ed = ep.open_raw(
                raw_file=local_file,
                storage_options=config.output.storage_options_dict,
                **raw_kwargs
            )
            
            xr_d_sv = ep.calibrate.compute_Sv(echodata=ed, **Sv_kwargs)
            
            xr_d = ep.consolidate.add_depth(
                ds=xr_d_sv,
                **depth_kwargs
            )
                        
            ed["Platform"] = ed["Platform"].drop_duplicates("time1")
                
            xr_d = ep.consolidate.add_location(
                ds=xr_d,
                echodata=ed,
                **location_kwargs
            )
            
            log_util.log(
                msg={"msg": f"Converting to Zarr", "mod_name": __file__, "func_name": raw.filename},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            xr_d.to_zarr(
                store=out_zarr,
                mode="w",
                consolidated=True,
                storage_options=config.output.storage_options_dict,
            )
            
            del xr_d_sv
            del xr_d
            del ed
            del local_file
            
            if stage.options.get("save_raw_file") == False:
                log_util.log(
                    msg={
                        "msg": f"Deleting local raw file since `save_raw_file` flag is false",
                        "mod_name": __file__,
                        "func_name": raw.filename,
                    },
                    use_dask=stage.options["use_dask"],
                    eflogging=config.logging,
                )
                fs = extract_fs(local_file, storage_options=config.output.storage_options_dict)
                fs.rm(local_file, recursive=True)
        else:
            log_util.log(
                msg={
                    "msg": f"Skipped processing {raw.filename}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False",
                    "mod_name": __file__,
                    "func_name": raw.filename,
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

        log_util.log(
            msg={"msg": f" ---- Exiting ----", "mod_name": __file__, "func_name": raw.filename},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )
        raw.out_path = out_zarr
        raw.error = ErrorObject(errorFlag=False)
        raw.stages[stage.name] = out_zarr
    except Exception as e:
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e,
            level=logging.ERROR,
        )
        raw.error = ErrorObject(errorFlag=True, error_desc=str(e))
    finally:
        return raw
