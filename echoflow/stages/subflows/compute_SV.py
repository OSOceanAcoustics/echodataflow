"""
Echoflow Compute SV Stage

This module defines a Prefect Flow and associated tasks for the Echoflow Compute SV stage.
The stage involves computing the Sv (volume backscattering strength) from echodata.

Classes:
    None

Functions:
    echoflow_compute_SV(config: Dataset, stage: Stage, data: List[Output])
    process_compute_SV(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import os
from typing import Any, Dict, List, Union
import echopype as ep
from echoflow.config.models.datastore import Dataset

from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage
from echoflow.stages.aspects.echoflow_aspect import echoflow

from prefect import flow, task

from echoflow.stages.utils.file_utils import get_ed_list, get_working_dir, isFile


@flow
@echoflow(processing_stage="compute-sv", type="FLOW")
def echoflow_compute_SV(config: Dataset, stage: Stage, data: List[Output]):
    """
    Compute volume backscattering strength (Sv) from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        data (List[Output]): List of processed echodata outputs.

    Returns:
        List[Output]: List of computed Sv outputs.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        echodata_outputs = ...

        # Execute the Echoflow Compute SV stage
        computed_sv_outputs = echoflow_compute_SV(
            config=dataset_config,
            stage=pipeline_stage,
            data=echodata_outputs
        )
        print("Computed Sv outputs:", computed_sv_outputs)
    """
    outputs: List[Output] = []
    futures = []
    working_dir = get_working_dir(config=config, stage=stage)
    # [Output(data=[{'out_path': '/Users/soham/Desktop/EchoWorkSpace/echoflow/notebooks/combined_files/echoflow_combine_echodata/7.zarr', 'transect': 7}], passing_params={}), Output(data=[{'out_path': '/Users/soham/Desktop/EchoWorkSpace/echoflow/notebooks/combined_files/echoflow_combine_echodata/10.zarr', 'transect': 10}], passing_params={})]

    if type(data) == list:
        if type(data[0].data) == list:
            for output_data in data:
                transect = str(output_data.data[0].get("out_path")) + ".SV"
                process_compute_SV_wo = process_compute_SV.with_options(
                    name=transect, task_run_name=transect
                )
                future = process_compute_SV_wo.submit(
                    config=config, stage=stage, out_data=output_data.data, working_dir=working_dir
                )
                futures.append(future)
        else:
            future = process_compute_SV.submit(
                config=config, stage=stage, out_data=data, working_dir=working_dir
            )
            futures.append(future)

        outputs = [f.result() for f in futures]
    return outputs


@task
def process_compute_SV(
    config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str
):
    """
    Process and compute volume backscattering strength (Sv) from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[List[Dict], List[Output]]): List of processed echodata outputs.
        working_dir (str): Working directory for processing.

    Returns:
        Output: Computed Sv output information.

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and compute Sv
        computed_sv_output = process_compute_SV(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print("Computed Sv output:", computed_sv_output)
    """
    ed_list = []
    xr_d_list = []
    if type(out_data) == list and type(out_data[0]) == dict:
        for ed in out_data:
            ed_list = get_ed_list.fn(
                config=config, stage=stage, transect_data=ed)
            file_name = str(ed.get("file_name")).split(".")[0] + "_SV.zarr"
            out_zarr = os.path.join(working_dir, str(
                ed.get("transect")), file_name)
            if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
                print("Did not use offline")
                xr_d_sv = ep.calibrate.compute_Sv(echodata=ed_list[0])
                xr_d_sv.to_zarr(store=out_zarr, mode="w", consolidated=True,
                                storage_options=config.output.storage_options_dict)
            xr_d_output = {'out_path': out_zarr, 'transect': str(
                ed.get("transect")), 'file_name': file_name}
            xr_d_list.append(xr_d_output)

    else:
        for output_obj in out_data:
            ed_list = get_ed_list.fn(
                config=config, stage=stage, transect_data=output_obj)
            file_name = str(output_obj.data.get("file_name")
                            ).split(".")[0] + "_SV.zarr"
            out_zarr = os.path.join(working_dir, str(output_obj.data.get("transect")), str(
                output_obj.data.get("file_name")).split(".")[0] + "_SV.zarr")
            if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
                print("Did not use offline")
                xr_d_sv = ep.calibrate.compute_Sv(echodata=ed_list[0])
                xr_d_sv.to_zarr(store=out_zarr, mode="w", consolidated=True,
                                storage_options=config.output.storage_options_dict)
            xr_d_output = {'out_path': out_zarr, 'transect': str(
                output_obj.data.get("transect")), 'file_name': file_name}
            xr_d_list.append(xr_d_output)

    output = Output()
    output.data = xr_d_list
    return output
