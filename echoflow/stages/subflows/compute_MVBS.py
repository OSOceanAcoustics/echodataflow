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
from typing import Dict, List, Union
import echopype as ep

from echoflow.config.models.datastore import Dataset

from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage

from prefect import flow, task

from echoflow.stages.aspects.echoflow_aspect import echoflow
from echoflow.stages.utils.file_utils import get_working_dir, get_zarr_list, isFile


@flow
@echoflow(processing_stage="compute-mvbs", type="FLOW")
def echoflow_compute_MVBS(
        config: Dataset, stage: Stage, data: Union[str, List[Output]]
):
    """
    Compute Mean Volume Backscattering Strength (MVBS) from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        data (Union[str, List[Output]]): Data to be processed (echodata outputs).

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
    outputs: List[Output] = []
    futures = []
    working_dir = get_working_dir(config=config, stage=stage)

    if type(data) == list:
        if type(data[0].data) == list:
            for output_data in data:
                transect = str(output_data.data[0].get("out_path")) + ".MVBS"
                process_compute_MVBS_wo = process_compute_MVBS.with_options(
                    name=transect, task_run_name=transect
                )
                future = process_compute_MVBS_wo.submit(
                    config=config, stage=stage, out_data=output_data.data, working_dir=working_dir
                )
                futures.append(future)
        else:
            future = process_compute_MVBS.submit(
                config=config, stage=stage, out_data=data, working_dir=working_dir
            )
            futures.append(future)

        outputs = [f.result() for f in futures]
    return outputs


@task
def process_compute_MVBS(
    config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str
):
    """
    Process and compute Mean Volume Backscattering Strength (MVBS) from xarray dataset.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[List[Dict], List[Output]]): List of processed outputs (echodata) to compute MVBS.
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
    ed_list = []
    xr_d_list = []
    if type(out_data) == list and type(out_data[0]) == dict:
        for ed in out_data:
            ed_list = get_zarr_list.fn(
                config=config, stage=stage, transect_data=ed)
            file_name = str(ed.get("file_name")).split(".")[0] + "_MVBS.zarr"
            out_zarr = os.path.join(working_dir, str(
                ed.get("transect")), file_name)
            if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
                xr_d_mvbs = ep.commongrid.compute_MVBS(
                    ds_Sv=ed_list[0],
                    range_meter_bin=stage.external_params.get(
                        "range_meter_bin"),
                    ping_time_bin=stage.external_params.get("ping_time_bin")
                )
                xr_d_mvbs.to_zarr(store=out_zarr, mode="w", consolidated=True,
                                  storage_options=config.output.storage_options_dict)
            xr_d_output = {'out_path': out_zarr, 'transect': str(
                ed.get("transect")), 'file_name': file_name}
            xr_d_list.append(xr_d_output)

    else:
        for output_obj in out_data:
            ed_list = get_zarr_list.fn(
                config=config, stage=stage, transect_data=output_obj)
            file_name = str(output_obj.data.get("file_name")
                            ).split(".")[0] + "_MVBS.zarr"
            out_zarr = os.path.join(working_dir, str(output_obj.data.get("transect")), str(
                output_obj.data.get("file_name")).split(".")[0] + "_SV.zarr")
            if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
                xr_d_mvbs = ep.commongrid.compute_MVBS(
                    ds_Sv=ed_list[0],
                    range_meter_bin=stage.external_params.get(
                        "range_meter_bin"),
                    ping_time_bin=stage.external_params.get("ping_time_bin")
                )
                xr_d_mvbs.to_zarr(store=out_zarr, mode="w", consolidated=True,
                                  storage_options=config.output.storage_options_dict)
            xr_d_output = {'out_path': out_zarr, 'transect': str(
                output_obj.data.get("transect")), 'file_name': file_name}
            xr_d_list.append(xr_d_output)

    output = Output()
    output.data = xr_d_list
    return output
