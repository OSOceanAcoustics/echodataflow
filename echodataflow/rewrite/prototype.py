from pathlib import Path
import datetime
import asyncio
from yaml import safe_load

import numpy as np
import pandas as pd
import xarray as xr
import numpy as np

import echopype as ep

from prefect import deploy, flow, task, get_run_logger, get_client
from prefect.variables import Variable
from prefect_shell import ShellOperation
from prefect_dask import DaskTaskRunner
from prefect.concurrency.sync import concurrency

from prefect.states import Cancelled
from prefect import runtime
from prefect.client.schemas.filters import FlowRunFilter



import boto3
from botocore import UNSIGNED
from botocore.config import Config

from utils import (
    get_MVBS_tensor,
    get_hake_model,
    round_up_mins,
    get_slice_start_end_times,
    # deployment_already_running
)

import torch
from src.model.BinaryHakeModel import BinaryHakeModel



async def set_concurrency_limit():
    async with get_client() as client:
        await client.create_concurrency_limit(tag="raw2Sv", concurrency_limit=4)

asyncio.run(set_concurrency_limit())


# Turn on verbose logging for echopype
# otherwise all logging will be muted
ep.utils.log.verbose()


# Set up paths
context = "SH2306"

if context == "SH2306":
    data_path = Path("/Users/wujung/code_git/echodataflow/temp_data")  # SH2306
    raw_path = data_path / "raw"  # SH2306
    Sv_csv_filename = "SH2306_Sv_files.csv"
    MVBS_csv_filename = "SH2306_MVBS_files.csv"
    prediction_csv_filename = "SH2306_prediction_files.csv"
else:
    data_path = Path("/Users/wujung/code_git/echodataflow/temp_data_replay")  # gear trial replay
    raw_path = Path("/Users/wujung/code_git/echodataflow/temp_data_replay/raw")  # gear trial replay
    Sv_csv_filename = "gear_trial_replay_Sv_files.csv"
    MVBS_csv_filename = "gear_trial_replay_MVBS_files.csv"
    prediction_csv_filename = "gear_trial_replay_prediction_files.csv"

Sv_path = data_path / "Sv"
MVBS_path = data_path / "MVBS"
prediction_path = data_path / "prediction"

if not Sv_path.exists():
    Sv_path.mkdir(parents=True, exist_ok=True)
if not MVBS_path.exists():
    MVBS_path.mkdir(parents=True, exist_ok=True)
if not prediction_path.exists():
    prediction_path.mkdir(parents=True, exist_ok=True)


# Initiate counter for raw file copy
df_raw = pd.read_csv(
    data_path / "SH2306_raw_files.txt",
    # Path(__file__).parent / "../../temp_raw/SH2306_raw_files.txt", 
    sep=r'\s+',  # multiple spaces as delimiter
    header=None,  # no header
    names=["date", "time", "size", "filename"]  # assign column names
)

# Info dataframe for which raw files to convert to Sv
Sv_csv_path = data_path / Sv_csv_filename
if not Sv_csv_path.exists():
    df_Sv = pd.DataFrame(
        columns=["raw_filename", "Sv_filename", "first_ping_time", "last_ping_time"]
    )
    df_Sv.to_csv(Sv_csv_path)

# Info dataframe for MVBS files
MVBS_csv_path = data_path / MVBS_csv_filename
if not MVBS_csv_path.exists():
    df_MVBS = pd.DataFrame(
        columns=["MVBS_filename", "first_ping_time", "last_ping_time"]
    )
    df_MVBS.to_csv(MVBS_csv_path)

# Info dataframe for prediction files
prediction_csv_path = data_path / prediction_csv_filename
if not prediction_csv_path.exists():
    df_prediction = pd.DataFrame(
        columns=["prediction_filename_postfix", "score_filename", "softmax_filename", "first_ping_time", "last_ping_time"]
    )
    df_prediction.to_csv(prediction_csv_path)


@task(log_prints=True)
async def deployment_already_running() -> bool:
    # Check if the deployment is already running
    async with get_client() as client:
        # Get all running flows for this deployment using simpler filters
        running_flows = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                deployment_id={"any_": [runtime.deployment.id]},
                state={"type": {"any_": ["RUNNING"]}}
            )
        )
        if len(running_flows) > 1:
            return True
        else:
            return False


@flow(log_prints=True)
def copy_raw():
    print("Copy raw files to simulate data generation")
    print(f"Executed at {datetime.datetime.now(datetime.UTC)}")
    counter = Variable.get("counter_raw_copy", default=None)

    # Get filename from dataframe
    filename = df_raw.iloc[counter]['filename']
    print(f"Copying file #{counter}: {filename}")

    # Configure anonymous access
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    # Copy file
    bucket = "noaa-wcsd-pds"
    s3_path = f"data/raw/Bell_M._Shimada/SH2306/EK80/{filename}"    
    s3.download_file(bucket, s3_path, raw_path / f"{filename}")

    # Increment ocunter
    Variable.set("counter_raw_copy", value=counter+1, overwrite=True)


@flow(
    log_prints=True,
    task_runner=DaskTaskRunner(address="tcp://127.0.0.1:52476")
)
async def flow_raw2Sv(parallel: bool = False, encode_mode: str = "power"):

    # Check if the deployment is already running
    already_running = await deployment_already_running()
    if already_running:
        async with get_client() as client:
            await client.set_flow_run_state(
                flow_run_id=runtime.flow_run.id,
                state=Cancelled(message="Another instance of this flow is already running")
            )
            return  # exit the flow early

    # Load info dataframe containing raw to Sv correspondence
    df_Sv = pd.read_csv(
        Sv_csv_path,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"]
    )
    df_Sv.sort_values(
        by="first_ping_time",
        inplace=True,
        ignore_index=True
    )
    print(df_Sv)
    raw_files_in_folder = set([ff.name for ff in raw_path.glob("*.raw")])
    raw_files_in_df = set(df_Sv["raw_filename"].tolist())
    last_raw_filename = df_Sv.iloc[-1]["raw_filename"] if not df_Sv.empty else None
    if last_raw_filename:
        df_Sv = df_Sv[:-1]  # drop the most recent file processed

    # Find new files to process
    new_files = raw_files_in_folder.difference(raw_files_in_df)
    print(f"Found {len(new_files)} new files to process")
    if last_raw_filename:
        print(f"Reprocess {last_raw_filename}")
        new_files.add(last_raw_filename)

    if parallel:
        # Convert raw files to Sv in parallel
        print("Processing raw files in parallel")
        future_all = []
        for nf in new_files:
            new_processed_raw = task_raw2Sv.with_options(
                task_run_name=nf, name=nf, retries=3
            )
            future = new_processed_raw.submit(raw_path / nf)
            future_all.append(future)

        results = []
        for nf, ff in zip(new_files, future_all):
            result = [nf] + list(ff.result())
            results.append(result)

    else:
        # Convert raw files to Sv sequentially
        print("Processing raw files sequentially")
        results = []
        for nf in new_files:
            Sv_filename, first_ping_time, last_ping_time = task_raw2Sv.with_options(
                task_run_name=nf, name=nf, retries=3
            )(
                raw_path=raw_path / nf,
                encode_mode=encode_mode
            )
            results.append([nf, Sv_filename, first_ping_time, last_ping_time])

    # Add new entries to df_Sv
    if len(results) > 0:
        df_new = pd.DataFrame(
            results,
            columns=["raw_filename", "Sv_filename", "first_ping_time", "last_ping_time"]
        )
        print(df_new)
        
        # Concatenate with existing df_Sv and save
        df_Sv = pd.concat([df_Sv, df_new], ignore_index=True)
        df_Sv.sort_values(
            by=["first_ping_time"],
            inplace=True,
            ignore_index=True
        )
        df_Sv.to_csv(Sv_csv_path, date_format="%Y-%m-%dT%H:%M:%S.%f")
        print(f"Added {len(new_files)} new entries to tracking CSV")


@task(
    log_prints=True,
    tags=["raw2Sv"],
)
def task_raw2Sv(raw_path: str, encode_mode: str = "power"):
    """
    Convert raw sonar data to Sv and save to zarr format.
    """
    # Convert raw file, consolidate Sv and save to zarr
    ed = ep.open_raw(
        raw_file=raw_path,
        sonar_model="EK80",  # can be raw_kwargs
    )

    # Compute Sv and consolidate depth and location
    ds_Sv = ep.calibrate.compute_Sv(
        echodata=ed,
        waveform_mode="CW",  # can be Sv_kwargs
        encode_mode=encode_mode,
    )
    ds_Sv = ep.consolidate.add_depth(
        ds=ds_Sv,
        depth_offset=9.5,  # can be depth_kwargs
    )
    ed["Platform"] = ed["Platform"].drop_duplicates("time1")
    ds_Sv = ep.consolidate.add_location(
        ds=ds_Sv,
        echodata=ed,
        nmea_sentence="GGA",  # can be location_kwargs
    )
    
    # Save to zarr
    out_path = Sv_path / f"{raw_path.stem}_Sv.zarr"
    ds_Sv.to_zarr(
        store=out_path,
        mode="w",
        consolidated=True,
        # storage_options=config.output.storage_options_dict,
    )

    return (
        out_path.name, 
        pd.to_datetime(ds_Sv["ping_time"][0].values),
        pd.to_datetime(ds_Sv["ping_time"][-1].values)
    )


@flow(log_prints=True)
def flow_create_MVBS(
    time_offset_seconds: float = 0.0,
    slice_mins: int = 10,
    num_slices: int = 3,
):
    """
    Process raw files to create MVBS files of specified length.

    Parameters
    ----------
    time_offset_seconds : float
        The time offset in seconds from current time to set the end time for MVBS computation.
    slice_mins : int
        Length of each slice in minutes.
    num_slices : int
        The number of slices to create.
    """
    logger = get_run_logger()

    # with concurrency("create_MVBS", occupy=1):
    # Set end_time to current time - time_offset_seconds
    end_time = round_up_mins(
        datetime.datetime.now() - datetime.timedelta(seconds=time_offset_seconds),
        slice_mins=slice_mins,
    )

    logger.info(
        "flow started with parameters:\n"
        f"- end_time: {end_time}\n"
        f"- slice_mins: {slice_mins}\n"
        f"- num_slices: {num_slices}\n"
    )

    # Load Sv and MVBS info dataframes
    df_Sv = pd.read_csv(
        Sv_csv_path,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"]
    )
    df_MVBS = pd.read_csv(
        MVBS_csv_path,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"]
    )

    # Compute slice time range
    start_time, end_time = get_slice_start_end_times(
        end_time=end_time, slice_mins=slice_mins, num_slices=num_slices
    )
    
    # Sequentially create MVBS slices
    for snum in range(num_slices):
        logger.info(f"Slice {snum+1}: {start_time[snum]} to {end_time[snum]}")

        # Get Sv files in the specified time range
        Sv_filenames = df_Sv[
            (pd.to_datetime(df_Sv["last_ping_time"]) >= start_time[snum]) &
            (pd.to_datetime(df_Sv["first_ping_time"]) <= end_time[snum])
        ]["Sv_filename"].tolist()
        logger.info(f"Found {len(Sv_filenames)} Sv files in the specified time range")

        # If no Sv files found, skip this slice
        if len(Sv_filenames) == 0:
            logger.info(f"No Sv files found for slice {snum+1}, skipping")
            continue

        # Create MVBS for this slice
        try:
            MVBS_filename = f"MVBS_{start_time[snum].strftime("%Y%m%dT%H%M%S")}.zarr"
            first_ping_time, last_ping_time = task_create_MVBS.with_options(
                task_run_name=MVBS_filename, name=MVBS_filename,
            )(MVBS_filename=MVBS_filename, start_time=start_time[snum], end_time=end_time[snum], Sv_filenames=Sv_filenames)

            # Add MVBS slice info to dataframe
            if MVBS_filename in df_MVBS["MVBS_filename"].values:
                logger.info(f"MVBS file {MVBS_filename} already exists, updating first and last ping times")
                idx_to_add = df_MVBS.index[df_MVBS["MVBS_filename"] == MVBS_filename]
            else:
                logger.info(f"Adding new MVBS file {MVBS_filename} to tracking dataframe")
                idx_to_add = len(df_MVBS)
            df_MVBS.loc[idx_to_add] = [MVBS_filename, first_ping_time, last_ping_time]
        except Exception as e:
            logger.error(f"Error during MVBS creation for slice {snum+1}: {e}")

    # Save updated MVBS info dataframe
    df_MVBS.to_csv(MVBS_csv_path, date_format="%Y-%m-%dT%H:%M:%S")


@task(log_prints=True)
def task_create_MVBS(MVBS_filename: str, start_time: pd.Timestamp, end_time: pd.Timestamp, Sv_filenames: list[str]):
    """
    Create MVBS from Sv files in the specified time range.

    Parameters
    ----------
    MVBS_filename : str
        The name of the MVBS file to create.
    start_time : pd.Timestamp
        The start time for the MVBS slice.
    end_time : pd.Timestamp
        The end time for the MVBS slice.
    """

    # Combine Sv files into a single dataset
    ds_Sv = xr.open_mfdataset(
        [Sv_path / svf for svf in Sv_filenames],
        parallel=True,
        coords="minimal",
        data_vars="minimal",
        compat='override',
        chunks={"channel": 1, "ping_time": 1000, "range_sample": -1},
        engine="zarr",  # use zarr engine for reading
    ).sel(
        # slice start/end, end exclusive
        ping_time=slice(start_time, end_time-pd.to_timedelta("1nanoseconds"))
    )

    # Compute MVBS for the slice
    ds_MVBS = ep.commongrid.compute_MVBS(
        ds_Sv=ds_Sv,
        range_var="depth",
        range_bin='1m',
        ping_time_bin='5s',
        reindex=False,
        fill_value=np.nan,
    )

    # Save to zarr: 1 chunk along each dimension
    ds_MVBS.chunk({"channel": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=MVBS_path / MVBS_filename,  # existing file will be overwritten
        mode="w",
        consolidated=True,
        # storage_options=config.output.storage_options_dict,
    )

    return (
        pd.to_datetime(ds_MVBS["ping_time"][0].values),
        pd.to_datetime(ds_MVBS["ping_time"][-1].values)
    )


@flow(log_prints=True)
def flow_predict_hake(
    time_offset_seconds: float = 0.0,
    slice_mins: int = 10,
    num_slices: int = 3,
    temperature: float = 0.5,
):
    """
    Predict on MVBS files of specified length.

    Parameters
    ----------
    time_offset_seconds : float
        The time offset in seconds from current time to set the end time for MVBS computation.
    slice_mins : int
        Length of each slice in minutes.
    num_slices : int
        The number of slices to create.
    temperature : float
        Temperature parameter for softmax scaling in prediction.
    """
    logger = get_run_logger()

    # Load binary hake models with weights
    model_epoch = 85
    model_folder = "/Users/wujung/code_git/echodataflow/temp_model/model_160_epochs/model_weights"
    model_path = f"{model_folder}/binary_hake_model_1.0m_bottom_offset_1.0m_depth_2017_2019_epoch_{model_epoch:03d}.ckpt"
    model = get_hake_model(model_path)

    # Set end_time to current time - time_offset_seconds
    end_time = round_up_mins(
        datetime.datetime.now() - datetime.timedelta(seconds=time_offset_seconds),
        slice_mins=slice_mins,
    )

    logger.info(
        "flow started with parameters:\n"
        f"- end_time: {end_time}\n"
        f"- slice_mins: {slice_mins}\n"
        f"- num_slices: {num_slices}\n"
        f"- temperature: {temperature}\n"
    )

    # Compute slice time range
    start_time, end_time = get_slice_start_end_times(
        end_time=end_time, slice_mins=slice_mins, num_slices=num_slices
    )

    # Load Sv and MVBS info dataframes
    df_MVBS = pd.read_csv(
        MVBS_csv_path,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"]
    )
    df_prediction = pd.read_csv(
        prediction_csv_path,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"]
    )


    # Sequentially predict over combined MVBS slices
    for snum in range(num_slices):
        logger.info(f"Slice {snum+1}: {start_time[snum]} to {end_time[snum]}")

        # Get MVBS files in the specified time range
        MVBS_filenames = df_MVBS[
            (pd.to_datetime(df_MVBS["last_ping_time"]) >= start_time[snum]) &
            (pd.to_datetime(df_MVBS["first_ping_time"]) <= end_time[snum])
        ]["MVBS_filename"].tolist()
        logger.info(f"Found {len(MVBS_filenames)} MVBS files in the specified time range")

        # Skip prediction if no MVBS files found
        if len(MVBS_filenames) == 0:
            logger.info(f"No MVBS files found for slice {snum+1}, skipping")
            continue        

        # Predict on the MVBS files
        try:
            predict_filename_postfix=f"{start_time[snum].strftime("%Y%m%dT%H%M%S")}"
            score_filename, softmax_filename, first_ping_time, last_ping_time = task_predict_hake.with_options(
                task_run_name=f"predict_{predict_filename_postfix}",
                name=f"predict_{predict_filename_postfix}",
            )(
                predict_filename_postfix=predict_filename_postfix,
                MVBS_filenames=MVBS_filenames,
                start_time=start_time[snum],
                end_time=end_time[snum],
                model=model,
                temperature=temperature
            )

            # Add prediction slice info to dataframe
            if predict_filename_postfix in df_prediction["prediction_filename_postfix"].values:
                logger.info(f"Prediction file {predict_filename_postfix} already exists, updating first and last ping times")
                idx_to_add = df_prediction.index[df_prediction["prediction_filename_postfix"] == predict_filename_postfix]
            else:
                logger.info(f"Adding new prediction file {predict_filename_postfix} to tracking dataframe")
                idx_to_add = len(df_prediction)
            df_prediction.loc[idx_to_add] = [predict_filename_postfix, score_filename, softmax_filename, first_ping_time, last_ping_time]
        except Exception as e:
            logger.error(f"Error during prediction for slice {snum+1}: {e}")

        # Save updated prediction info dataframe
        df_prediction.to_csv(prediction_csv_path, date_format="%Y-%m-%dT%H:%M:%S")


@task(log_prints=True)
def task_predict_hake(
    predict_filename_postfix: str,
    MVBS_filenames: list[str],
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    model: BinaryHakeModel,
    temperature: int = 0.5,
):
    """
    Predict on a single MVBS file.

    Parameters
    ----------
    predict_filename_postfix : str
        Postfix for the prediction filename, typically a timestamp.
    MVBS_filenames : list[str]
        List of MVBS filenames to process.
    start_time : pd.Timestamp
        The start time for the MVBS computation.
    end_time : pd.Timestamp
        The end time for the MVBS computation.
    model : BinaryHakeModel
        The trained model to use for prediction.
    temperature : float
        Temperature parameter for softmax scaling in prediction.
    """
    # Combine MVBS files into a single dataset
    ds_MVBS_combine = xr.open_mfdataset(
        [MVBS_path / mvbsf for mvbsf in MVBS_filenames],
        parallel=True,
        coords="minimal",
        data_vars="minimal",
        compat='override',
        chunks={"channel": -1, "ping_time": -1, "depth": -1},  # load everything into 1 chunk
        engine="zarr",  # use zarr engine for reading
    ).sel(
        # slice start/end, end exclusive
        ping_time=slice(start_time, end_time-pd.to_timedelta("10milliseconds"))
    )

    # Prepare input tensor: slice depth and ensure order of coordinates
    input_tensor = get_MVBS_tensor(ds_MVBS_combine)

    # Predict using the model
    score_tensor = model(input_tensor).detach().squeeze(0)
    score_tensor_softmax = torch.nn.functional.softmax(score_tensor / temperature, dim=0)

    # Assemble output DataArrays
    da_score = xr.DataArray(
        score_tensor,
        coords={
            "class": ["background", "hake"],
            "depth": ds_MVBS_combine["depth"].sel(depth=slice(None, 590)).values,
            "ping_time": ds_MVBS_combine["ping_time"].values,
        }
    )
    da_score_softmax = xr.DataArray(
        score_tensor_softmax,
        coords={
            "class": ["background", "hake"],
            "depth": ds_MVBS_combine["depth"].sel(depth=slice(None, 590)).values,
            "ping_time": ds_MVBS_combine["ping_time"].values,
        }
    )

    # Save to zarr
    score_filename = f"score_{predict_filename_postfix}.zarr"
    softmax_filename = f"softmax_{predict_filename_postfix}.zarr"
    da_score.chunk({"class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=prediction_path / score_filename,
        mode="w",
        consolidated=True,
    )
    da_score_softmax.chunk({"class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=prediction_path / softmax_filename,
        mode="w",
        consolidated=True,
    )

    return (
        score_filename,
        softmax_filename,
        pd.to_datetime(ds_MVBS_combine["ping_time"][0].values),
        pd.to_datetime(ds_MVBS_combine["ping_time"][-1].values)
    )


@flow(timeout_seconds=600, log_prints=True)
def flow_file_upload(
    src_dir: str = str(MVBS_path),
    dest_dir: str = "osn_sdsc_hake:/agr230002-bucket01/prefect_test",
):
    """
    Upload files via rlcone.
    """
    print("test")

    # for long running operations, you can use a context manager
    with ShellOperation(
        commands=[
            f"rclone sync -vP {src_dir} {dest_dir}"
        ],
        working_dir=src_dir,
    ) as file_upload_operation:

        # Trigger runs the process in the background
        file_upload_process = file_upload_operation.trigger()

        # Wait for the process to finish
        file_upload_process.wait_for_completion()

        # Print results
        output_lines = file_upload_process.fetch_result()
        print(output_lines)


# @flow(log_prints=True) #, timeout_seconds=600)
# def flow_compute_hake_NASC(
#     time_offset_seconds: float = 0.0,
#     slice_mins: int = 10,
#     num_slices: int = 3,
# ):
#     """
#     Compute NASC by combining MVBS and hake prediction.
#     """
#     logger = get_run_logger()

#     # Set end_time to current time - time_offset_seconds
#     end_time = round_up_mins(
#         datetime.datetime.now() - datetime.timedelta(seconds=time_offset_seconds),
#         slice_mins=slice_mins,
#     )

#     logger.info(
#         "flow started with parameters:\n"
#         f"- end_time: {end_time}\n"
#         f"- slice_mins: {slice_mins}\n"
#         f"- num_slices: {num_slices}\n"
#     )

#     # Load Sv and MVBS info dataframes
#     df_Sv = pd.read_csv(
#         Sv_csv_path,
#         index_col=0,
#         date_format="ISO8601",
#         parse_dates=["first_ping_time", "last_ping_time"]
#     )
#     df_MVBS = pd.read_csv(
#         MVBS_csv_path,
#         index_col=0,
#         date_format="ISO8601",
#         parse_dates=["first_ping_time", "last_ping_time"]
#     )

#     # Compute slice time range
#     start_time, end_time = get_slice_start_end_times(
#         end_time=end_time, slice_mins=slice_mins, num_slices=num_slices
#     )
    


if __name__ == "__main__":

    # Load variables from config
    with open(Path(__file__).parent / "config.yaml", "r") as file:
        config = safe_load(file)

    # Set init variables
    init_dict = config.pop("init")
    Variable.set("flow_start_time", init_dict["flow_start_time"], overwrite=True)
    Variable.set("counter_raw_copy", init_dict["counter_raw_copy"], overwrite=True)
    if init_dict["flow_start_time"] is None:
        curr_time_offset = datetime.timedelta(seconds=0)
    else:
        curr_time_offset = (
            datetime.datetime.now()
            - datetime.datetime.strptime(
                init_dict["flow_start_time"], "%Y%m%dT%H%M%S"
            )
        )

    # Set interval dict
    interval_dict = {}
    for flow_name in config.keys():
        if flow_name != "init":
            interval_dict[flow_name] = config[flow_name].pop("interval", None)

    # Add time_offset_seconds to create_MVBS and predict_hake config dict
    for flow_name in ["create_MVBS", "predict_hake"]:
        config[flow_name]["time_offset_seconds"] = curr_time_offset.total_seconds()

    deploy(
        copy_raw.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:copy_raw"
        ).to_deployment(
            name="copy-raw",
            # cron=f"*/{interval_dict["copy_raw"]} * * * *",
        ),
        flow_raw2Sv.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_raw2Sv",
        ).to_deployment(
            name="raw2Sv",
            parameters=config["raw2Sv"],
            # cron=f"*/{interval_dict["raw2Sv"]} * * * *",
        ),
        flow_create_MVBS.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_create_MVBS",
        ).to_deployment(
            name="create-MVBS",
            parameters=config["create_MVBS"],
            # cron=f"*/{interval_dict["create_MVBS"]} * * * *",
        ),
        flow_predict_hake.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_predict_hake",
        ).to_deployment(
            name="predict-hake",
            parameters=config["predict_hake"],
            # cron=f"*/{interval_dict["predict_hake"]} * * * *",
        ),
        flow_file_upload.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_file_upload",
        ).to_deployment(
            name="file-upload",
        ),
        work_pool_name="local",
    )
