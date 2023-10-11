"""
Echoflow Open Raw Stage

This module defines a Prefect Flow and associated tasks for the Echoflow Open Raw stage.
The stage involves processing raw sonar data files, converting them to zarr format, and
organizing the processed data based on transects.

Classes:
    None

Functions:
    echoflow_open_raw(config: Dataset, stage: Stage, data: Union[str, List[List[Dict[str, Any]]]])
    process_raw(raw, working_dir: str, config: Dataset, stage: Stage)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from echopype import open_raw
from prefect import flow, task

from echoflow.aspects.echoflow_aspect import echoflow
from echoflow.models.datastore import Dataset
from echoflow.models.output_model import Output
from echoflow.models.pipeline import Stage
from echoflow.utils.file_utils import (download_temp_file, get_output,
                                       get_working_dir, isFile,
                                       process_output_transects)


@flow
@echoflow(processing_stage="Open-Raw", type="FLOW")
def echoflow_open_raw(config: Dataset, stage: Stage, prev_stage: Optional[Stage]):
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

        # Execute the Echoflow Open Raw stage
        processed_outputs = echoflow_open_raw(
            config=dataset_config,
            stage=pipeline_stage,
            data=raw_data
        )
        print("Processed outputs:", processed_outputs)
    """

    data: Union[str, List[List[Dict[str, Any]]]] = get_output("Raw")

    working_dir = get_working_dir(stage=stage, config=config)

    ed_list = []
    futures = []
    outputs: List[Output] = []

    # Dealing with single file
    if type(data) == str or type(data) == Path:
        ed = process_raw(data, working_dir, config, stage)
        output = Output()
        output.data = ed
        outputs.append(output)
    else:
        for raw_dicts in data:
            for raw in raw_dicts:
                new_processed_raw = process_raw.with_options(
                    task_run_name=raw.get("file_path"), name=raw.get("file_path"), retries=3
                )
                future = new_processed_raw.submit(
                    raw, working_dir, config, stage)
                futures.append(future)

        ed_list = [f.result() for f in futures]

        outputs = process_output_transects(name=stage.name, config=config, ed_list=ed_list)
    return outputs


@task()
@echoflow()
def process_raw(raw, working_dir: str, config: Dataset, stage: Stage):
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
    temp_file = download_temp_file(raw, working_dir, stage, config)
    local_file = temp_file.get("local_path")
    local_file_name = os.path.basename(temp_file.get("local_path"))
    out_zarr = os.path.join(working_dir, str(
        raw.get("transect_num")), local_file_name.replace(".raw", ".zarr"))
    if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
        ed = open_raw(raw_file=local_file, sonar_model=raw.get(
            "instrument"), storage_options=config.output.storage_options_dict)
        ed.to_zarr(
            save_path=str(out_zarr),
            overwrite=True,
            output_storage_options=config.output.storage_options_dict,
            compute=False
        )
        del ed

        if stage.options.get("save_raw_file") == False:
            local_file.unlink()

    return {'out_path': out_zarr, 'transect': raw.get("transect_num"), 'file_name': local_file_name, 'error': False}
