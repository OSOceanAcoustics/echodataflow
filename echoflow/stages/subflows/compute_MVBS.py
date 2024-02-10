"""
Echoflow Compute MVBS Stage

This module defines a Prefect Flow and associated tasks for the Echoflow Compute MVBS stage.
The stage involves computing the MVBS (Mean Volume Backscattering Strength) from echodata.

Classes:
    None

Functions:
    echoflow_compute_MVBS(config: Dataset, stage: Stage, data: Union[str, List[Output]])
    process_compute_MVBS(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import os
from typing import Dict, List, Optional, Union

import echopype as ep
from prefect import flow, task

from echoflow.aspects.echoflow_aspect import echoflow
from echoflow.models.datastore import Dataset
from echoflow.models.output_model import Output
from echoflow.models.pipeline import Stage
from echoflow.utils.file_utils import (get_out_zarr, get_output, get_working_dir,
                                       get_zarr_list, isFile,
                                       process_output_transects)


@flow
@echoflow(processing_stage="Compute-MVBS", type="FLOW")
def echoflow_compute_MVBS(
        config: Dataset, stage: Stage, prev_stage: Optional[Stage]
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

        # Execute the Echoflow Compute MVBS stage
        computed_mvbs_outputs = echoflow_compute_MVBS(
            config=dataset_config,
            stage=pipeline_stage,
            data=echodata_outputs
        )
        print("Computed MVBS outputs:", computed_mvbs_outputs)
    """
    data: Union[str, List[Output]] = get_output()
    outputs: List[Output] = []
    futures = []
    working_dir = get_working_dir(config=config, stage=stage)

    if type(data) == list:
        if type(data[0].data) == list:
            for output_data in data:
                transect_list = output_data.data
                for ed in transect_list:
                    transect = str(ed.get("out_path")).split(".")[0] + ".MVBS"
                    process_compute_MVBS_wo = process_compute_MVBS.with_options(
                        name=transect, task_run_name=transect, retries=3
                    )
                    future = process_compute_MVBS_wo.submit(
                        config=config, stage=stage, out_data=ed, working_dir=working_dir
                    )
                    futures.append(future)
        else:
            for output_data in data:
                future = process_compute_MVBS.submit(
                    config=config, stage=stage, out_data=output_data, working_dir=working_dir
                )
                futures.append(future)

        ed_list = [f.result() for f in futures]
        outputs = process_output_transects(name=stage.name, config=config, ed_list=ed_list)
    return outputs


@task
@echoflow()
def process_compute_MVBS(
    config: Dataset, stage: Stage, out_data: Union[Dict, Output], working_dir: str
):
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

    if type(out_data) == dict:
        file_name = str(out_data.get("file_name")).split(".")[0] + "_MVBS.zarr"
        transect = str(out_data.get("transect"))
        
    else:
        file_name = str(out_data.data.get("file_name")).split(".")[0] + "_MVBS.zarr"
        transect = str(out_data.data.get("transect"))
        
    out_zarr = get_out_zarr(group = stage.options.get('group', True), working_dir=working_dir, transect=transect, file_name=file_name, storage_options=config.output.storage_options_dict)
    if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
        ed_list = get_zarr_list.fn(transect_data=out_data, storage_options=config.output.storage_options_dict)
        xr_d_mvbs = ep.commongrid.compute_MVBS(
                    ds_Sv=ed_list[0],
                    range_bin=stage.external_params.get(
                        "range_meter_bin"),
                    ping_time_bin=stage.external_params.get("ping_time_bin")
                )
        xr_d_mvbs.to_zarr(store=out_zarr, mode="w", consolidated=True,
                        storage_options=config.output.storage_options_dict)
    return {'out_path': out_zarr, 'transect': transect, 'file_name': file_name, 'error': False}
