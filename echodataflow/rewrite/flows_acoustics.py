from pathlib import Path
import datetime
import asyncio

import numpy as np
import pandas as pd
import xarray as xr
import numpy as np

import echopype as ep

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from prefect import flow, task, get_run_logger, get_client
from prefect_dask import DaskTaskRunner
from prefect.states import Cancelled, Failed
from prefect import runtime
from prefect.variables import Variable

from helpers import deployment_already_running
from utils import (
    get_MVBS_tensor,
    get_hake_model,
    round_up_mins,
    get_slice_start_end_times,
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


# # Set up paths
# context = "SH2306"

# if context == "SH2306":
#     data_path = Path("/Users/feresa/code_git/echodataflow/temp_data")  # SH2306
#     raw_path = data_path / "raw"  # SH2306
#     Sv_csv_filename = "SH2306_Sv_files.csv"
#     MVBS_csv_filename = "SH2306_MVBS_files.csv"
#     prediction_csv_filename = "SH2306_prediction_files.csv"
# else:
#     data_path = Path("/Users/feresa/code_git/echodataflow/temp_data_replay")  # gear trial replay
#     raw_path = Path("/Users/feresa/code_git/echodataflow/temp_data_replay/raw")  # gear trial replay
#     Sv_csv_filename = "gear_trial_replay_Sv_files.csv"
#     MVBS_csv_filename = "gear_trial_replay_MVBS_files.csv"
#     prediction_csv_filename = "gear_trial_replay_prediction_files.csv"

# Sv_path = data_path / "Sv"
# MVBS_path = data_path / "MVBS"
# prediction_path = data_path / "prediction"
# NASC_path = data_path / "NASC"

# if not Sv_path.exists():
#     Sv_path.mkdir(parents=True, exist_ok=True)
# if not MVBS_path.exists():
#     MVBS_path.mkdir(parents=True, exist_ok=True)
# if not prediction_path.exists():
#     prediction_path.mkdir(parents=True, exist_ok=True)
# if not NASC_path.exists():
#     NASC_path.mkdir(parents=True, exist_ok=True)


# # Initiate counter for raw file copy
# df_raw = pd.read_csv(
#     data_path / "SH2306_raw_files.txt",
#     # Path(__file__).parent / "../../temp_raw/SH2306_raw_files.txt", 
#     sep=r'\s+',  # multiple spaces as delimiter
#     header=None,  # no header
#     names=["date", "time", "size", "filename"]  # assign column names
# )

# # Info dataframe for which raw files to convert to Sv
# Sv_csv_path = data_path / Sv_csv_filename
# if not Sv_csv_path.exists():
#     df_Sv = pd.DataFrame(
#         columns=["raw_filename", "Sv_filename", "first_ping_time", "last_ping_time"]
#     )
#     df_Sv.to_csv(Sv_csv_path)

# # Info dataframe for MVBS files
# MVBS_csv_path = data_path / MVBS_csv_filename
# if not MVBS_csv_path.exists():
#     df_MVBS = pd.DataFrame(
#         columns=["MVBS_filename", "first_ping_time", "last_ping_time"]
#     )
#     df_MVBS.to_csv(MVBS_csv_path)

# # Info dataframe for prediction files
# prediction_csv_path = data_path / prediction_csv_filename
# if not prediction_csv_path.exists():
#     df_prediction = pd.DataFrame(
#         columns=["prediction_filename_postfix", "score_filename", "softmax_filename", "first_ping_time", "last_ping_time"]
#     )
#     df_prediction.to_csv(prediction_csv_path)


# @flow(log_prints=True)
# def flow_copy_raw(
#     # df_raw,  # DataFrame containing raw file metadata
#     # raw_path: str,  # Path to copy raw files to
#     s3_path: str = "data/raw/Bell_M._Shimada/SH2306/EK80",  # S3 path for raw files
# ):
#     print("Copy raw files to simulate data generation")
#     print(f"Executed at {datetime.datetime.now(datetime.UTC)}")
#     counter = Variable.get("counter_raw_copy", default=None)

#     # Get filename from dataframe
#     filename = df_raw.iloc[counter]['filename']
#     print(f"Copying file #{counter}: {filename}")

#     # Configure anonymous access
#     s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
#     # Copy file
#     bucket = "noaa-wcsd-pds"
#     s3_path = f"{s3_path}/{filename}"    
#     s3.download_file(bucket, s3_path, raw_path / f"{filename}")

#     # Increment ocunter
#     Variable.set("counter_raw_copy", value=counter+1, overwrite=True)



@flow(
    log_prints=True,
    task_runner=DaskTaskRunner()
)
async def flow_raw2Sv(
    parallel: bool = False,
    encode_mode: str = "power",
    waveform_mode: str = "CW",
    depth_offset: float = 9.5,
    sonar_model: str = "EK80",
    nmea_sentence: str = "GGA",
    path_main: str = "",
    path_raw: str = "",
    file_Sv_csv: str = "Sv_files.csv",
):

    # Check if the deployment is already running
    already_running = await deployment_already_running()
    if already_running:
        async with get_client() as client:
            await client.set_flow_run_state(
                flow_run_id=runtime.flow_run.id,
                state=Cancelled(message="Another instance of this flow is already running")
            )
            return  # exit the flow early

    # Assemble paths
    path_Sv_zarr = Path(path_main) / "Sv"
    file_Sv_csv = Path(path_main) / file_Sv_csv
    path_raw = Path(path_raw)

    # Set up folder to store converted Sv zarr
    if not path_Sv_zarr.exists():
        path_Sv_zarr.mkdir(parents=True, exist_ok=True)
    path_Sv_zarr = str(path_Sv_zarr)  # convert backto string to pass into task

    # Load info dataframe containing raw to Sv correspondence
    if not file_Sv_csv.exists():
        df_Sv = pd.DataFrame(
            columns=["raw_filename", "Sv_filename", "first_ping_time", "last_ping_time"]
        )
        df_Sv.to_csv(file_Sv_csv)
    else:
        df_Sv = pd.read_csv(
            file_Sv_csv,
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

    raw_files_in_folder = set([ff.name for ff in path_raw.glob("*.raw")])
    if df_Sv.empty:
        raw_files_in_df = set()
    else:
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

    # Bundle up task_raw2Sv parameters
    task_kwargs = dict(
        path_Sv_zarr=path_Sv_zarr,
        encode_mode=encode_mode,
        waveform_mode=waveform_mode,
        depth_offset=depth_offset,
        sonar_model=sonar_model,
        nmea_sentence=nmea_sentence,
    )

    if parallel:
        # Convert raw files to Sv in parallel
        print("Processing raw files in parallel")
        future_all = []
        for nf in new_files:
            new_processed_raw = task_raw2Sv.with_options(
                task_run_name=nf, name=nf, retries=3
            )
            future = new_processed_raw.submit(path_raw / nf, **task_kwargs)
            future_all.append(future)

        results = []
        for nf, ff in zip(new_files, future_all):
            result = [nf] + list(ff.result())
            results.append(result)

    else:
        # Convert raw files to Sv sequentially
        errors = []
        print("Processing raw files sequentially")
        results = []
        for nf in new_files:
            try:
                Sv_filename, first_ping_time, last_ping_time = task_raw2Sv.with_options(
                    task_run_name=nf, name=nf, retries=3
                )(
                    raw_path=path_raw / nf, **task_kwargs
                )
                results.append([nf, Sv_filename, first_ping_time, last_ping_time])
            except Exception as e:
                errors.append(e)
                print(f"Error converting {nf}: {e}")

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
        df_Sv.to_csv(file_Sv_csv, date_format="%Y-%m-%dT%H:%M:%S.%f")
        print(f"Added {len(new_files)} new entries to tracking CSV")

    # Set flow to Failed state if any errors occurred
    if len(errors) > 0:
        error_msg = f"{len(errors)} errors during raw to Sv conversion out of {len(new_files)} files"
        async with get_client() as client:
            await client.set_flow_run_state(
                flow_run_id=runtime.flow_run.id,
                state=Failed(message=error_msg)
            )
        raise Exception(error_msg)


@task(
    log_prints=True,
    tags=["raw2Sv"],
)
def task_raw2Sv(
    raw_path: str,
    encode_mode: str = "power",
    waveform_mode: str = "CW",
    depth_offset: float = 9.5,  # in meters
    sonar_model: str = "EK80",
    nmea_sentence: str = "GGA",
    path_Sv_zarr: str = "PATH_TO_STORE_SV_ZARR",
):
    """
    Convert raw sonar data to Sv and save to zarr format.
    """
    # Convert raw file, consolidate Sv and save to zarr
    ed = ep.open_raw(
        raw_file=raw_path,
        sonar_model=sonar_model,
    )

    # Compute Sv and consolidate depth and location
    ds_Sv = ep.calibrate.compute_Sv(
        echodata=ed,
        waveform_mode=waveform_mode,
        encode_mode=encode_mode,
    )
    ds_Sv = ep.consolidate.add_depth(
        ds=ds_Sv,
        depth_offset=depth_offset,
    )
    ed["Platform"] = ed["Platform"].drop_duplicates("time1")
    ds_Sv = ep.consolidate.add_location(
        ds=ds_Sv,
        echodata=ed,
        nmea_sentence=nmea_sentence,
    )
    
    # Save to zarr
    out_path = Path(path_Sv_zarr) / f"{Path(raw_path).stem}_Sv.zarr"
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
async def flow_create_MVBS(
    time_offset_seconds: float = 0.0,
    slice_mins: int = 10,
    num_slices: int = 3,
    path_main: str = "",
    file_Sv_csv: str = "Sv_files.csv",
    file_MVBS_csv: str = "MVBS_files.csv",
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
    ).astimezone(datetime.timezone.utc)  # convert to UTC

    logger.info(
        "flow started with parameters:\n"
        f"- end_time: {end_time}\n"
        f"- slice_mins: {slice_mins}\n"
        f"- num_slices: {num_slices}\n"
    )

    # Compute slice time range
    start_time, end_time = get_slice_start_end_times(
        end_time=end_time, slice_mins=slice_mins, num_slices=num_slices
    )

    # Assemble paths
    file_Sv_csv = Path(path_main) / file_Sv_csv
    file_MVBS_csv = Path(path_main) / file_MVBS_csv
    path_Sv_zarr = Path(path_main) / "Sv"
    path_MVBS_zarr = Path(path_main) / "MVBS"

    # Validate zarr store paths
    if not path_Sv_zarr.exists():
        raise ValueError("Sv zarr store does not exist, check raw2Sv flow!")
    if not path_MVBS_zarr.exists():
        path_MVBS_zarr.mkdir(parents=True, exist_ok=True)
    path_Sv_zarr = str(path_Sv_zarr)  # convert back to string to pass into task
    path_MVBS_zarr = str(path_MVBS_zarr)  # convert back to string to pass into task

    # Load Sv and MVBS info dataframes
    if not file_Sv_csv.exists():
        raise ValueError("Sv info csv does not exist, check raw2Sv flow!")
    df_Sv = pd.read_csv(
        file_Sv_csv,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"]
    )
    # Convert last_ping_time and first_ping_time to UTC
    if df_Sv["last_ping_time"].dt.tz is None:
        df_Sv["last_ping_time"] = df_Sv["last_ping_time"].dt.tz_localize("UTC")
    if df_Sv["first_ping_time"].dt.tz is None:
        df_Sv["first_ping_time"] = df_Sv["first_ping_time"].dt.tz_localize("UTC")

    if not file_MVBS_csv.exists():
        df_MVBS = pd.DataFrame(
            columns=["MVBS_filename", "first_ping_time", "last_ping_time"]
        )
        df_MVBS.to_csv(file_MVBS_csv)
    else:
        df_MVBS = pd.read_csv(
            file_MVBS_csv,
            index_col=0,
            date_format="ISO8601",
            parse_dates=["first_ping_time", "last_ping_time"]
        )

    # Sequentially create MVBS slices
    errors = []
    for snum in range(num_slices):
        logger.info(f"Slice {snum+1}: {start_time[snum]} to {end_time[snum]}")

        # Get Sv files in the specified time range
        Sv_filenames = sorted(
        df_Sv[
                (pd.to_datetime(df_Sv["last_ping_time"]) >= start_time[snum]) &
                (pd.to_datetime(df_Sv["first_ping_time"]) <= end_time[snum])
            ]["Sv_filename"].tolist()
        )
        logger.info(
            f"Found {len(Sv_filenames)} Sv files in the specified time range: \n"
            + "".join([f"- {svf}\n" for svf in Sv_filenames])
        )

        # If no Sv files found, skip this slice
        if len(Sv_filenames) == 0:
            logger.info(f"No Sv files found for slice {snum+1}, skipping")
            continue

        # Create MVBS for this slice
        try:
            MVBS_filename = f"MVBS_{start_time[snum].strftime("%Y%m%dT%H%M%S")}.zarr"
            first_ping_time, last_ping_time = task_create_MVBS.with_options(
                task_run_name=MVBS_filename,
                name=MVBS_filename,
            )(
                start_time=start_time[snum],
                end_time=end_time[snum],
                path_MVBS_zarrr=path_MVBS_zarr,
                MVBS_filename=MVBS_filename,
                path_Sv_zarr=path_Sv_zarr,
                Sv_filenames=Sv_filenames,
            )

            # Add MVBS slice info to dataframe
            if MVBS_filename in df_MVBS["MVBS_filename"].values:
                logger.info(f"MVBS file {MVBS_filename} already exists, updating first and last ping times")
                idx_to_add = df_MVBS.index[df_MVBS["MVBS_filename"] == MVBS_filename]
            else:
                logger.info(f"Adding new MVBS file {MVBS_filename} to tracking dataframe")
                idx_to_add = len(df_MVBS)
            df_MVBS.loc[idx_to_add] = [MVBS_filename, first_ping_time, last_ping_time]
        except Exception as e:
            errors.append(e)
            logger.error(f"Error during MVBS creation for slice {snum+1}: {e}")

    # Save updated MVBS info dataframe
    df_MVBS.to_csv(file_MVBS_csv, date_format="%Y-%m-%dT%H:%M:%S")

    # Set flow to Failed state if any errors occurred
    if len(errors) > 0:
        error_msg = f"{len(errors)} errors during MVBS creation out of {num_slices} slices"
        async with get_client() as client:
            await client.set_flow_run_state(
                flow_run_id=runtime.flow_run.id,
                state=Failed(message=error_msg)
            )
        raise Exception(error_msg)


@task(log_prints=True)
def task_create_MVBS(
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    path_MVBS_zarrr: str,
    MVBS_filename: str,
    path_Sv_zarr: str,
    Sv_filenames: list[str],
):
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
    logger = get_run_logger()

    # Remove timezone info for slicing
    start_time = start_time.replace(tzinfo=None)
    end_time = end_time.replace(tzinfo=None)

    # Combine Sv files into a single dataset
    ds_Sv = xr.open_mfdataset(
        [Path(path_Sv_zarr) / svf for svf in Sv_filenames],
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
    print("MVBS filename:", MVBS_filename)
    logger.info(f"Saving MVBS to {MVBS_filename}")
    ds_MVBS.chunk({"channel": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=Path(path_MVBS_zarrr) / MVBS_filename,  # existing file will be overwritten
        mode="w",
        consolidated=True,
        # storage_options=config.output.storage_options_dict,
    )

    return (
        pd.to_datetime(ds_MVBS["ping_time"][0].values),
        pd.to_datetime(ds_MVBS["ping_time"][-1].values)
    )


@flow(log_prints=True)
async def flow_predict_hake(
    time_offset_seconds: float = 0.0,
    slice_mins: int = 10,
    num_slices: int = 3,
    temperature: float = 0.5,
    softmax_threshold: float = 0.5,
    path_main: str = "",
    file_MVBS_csv: str = "",
    file_prediction_csv: str = ""
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
    model_folder = "/Users/feresa/code_git/echodataflow/temp_model/model_160_epochs/model_weights"
    model_path = f"{model_folder}/binary_hake_model_1.0m_bottom_offset_1.0m_depth_2017_2019_epoch_{model_epoch:03d}.ckpt"
    model = get_hake_model(model_path)

    # Set end_time to current time - time_offset_seconds
    end_time = round_up_mins(
        datetime.datetime.now() - datetime.timedelta(seconds=time_offset_seconds),
        slice_mins=slice_mins,
    ).astimezone(datetime.timezone.utc)  # convert to UTC

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

    # Assemble paths
    file_MVBS_csv = Path(path_main) / file_MVBS_csv
    file_prediction_csv = Path(path_main) / file_prediction_csv
    path_MVBS_zarr = Path(path_main) / "MVBS"
    path_prediction_zarr = Path(path_main) / "prediction"
    path_NASC_zarr = Path(path_main) / "NASC"
    if not path_MVBS_zarr.exists():
        raise ValueError("MVBS zarr store does not exist, check create_MVBS flow!")
    if not path_prediction_zarr.exists():
        path_prediction_zarr.mkdir(parents=True, exist_ok=True)
    if not path_NASC_zarr.exists():
        path_NASC_zarr.mkdir(parents=True, exist_ok=True)
    # convert back to string to pass into task
    path_MVBS_zarr = str(path_MVBS_zarr)
    path_prediction_zarr = str(path_prediction_zarr)
    path_NASC_zarr = str(path_NASC_zarr)

    # Load Sv and MVBS info dataframes
    if not file_MVBS_csv.exists():
        raise ValueError("MVBS info csv does not exist, check create_MVBS flow!")
    df_MVBS = pd.read_csv(
        file_MVBS_csv,
        index_col=0,
        date_format="ISO8601",
        parse_dates=["first_ping_time", "last_ping_time"]
    )
    # Convert last_ping_time and first_ping_time to UTC
    if df_MVBS["last_ping_time"].dt.tz is None:
        df_MVBS["last_ping_time"] = df_MVBS["last_ping_time"].dt.tz_localize("UTC")
    if df_MVBS["first_ping_time"].dt.tz is None:
        df_MVBS["first_ping_time"] = df_MVBS["first_ping_time"].dt.tz_localize("UTC")

    if not file_prediction_csv.exists():
        df_prediction = pd.DataFrame(
            columns=["prediction_filename_postfix", "score_filename", "softmax_filename", "first_ping_time", "last_ping_time"]
        )
        df_prediction.to_csv(file_prediction_csv)
    else:
        df_prediction = pd.read_csv(
            file_prediction_csv,
            index_col=0,
            date_format="ISO8601",
            parse_dates=["first_ping_time", "last_ping_time"]
        )
    # Convert last_ping_time and first_ping_time to UTC
    if df_prediction["last_ping_time"].dt.tz is None:
        df_prediction["last_ping_time"] = df_prediction["last_ping_time"].dt.tz_localize("UTC")
    if df_prediction["first_ping_time"].dt.tz is None:
        df_prediction["first_ping_time"] = df_prediction["first_ping_time"].dt.tz_localize("UTC")

    # Sequentially predict over combined MVBS slices
    errors = []
    for snum in range(num_slices):
        logger.info(f"Slice {snum+1}: {start_time[snum]} to {end_time[snum]}")

        # Get MVBS files in the specified time range
        MVBS_filenames = sorted(
            df_MVBS[
                (pd.to_datetime(df_MVBS["last_ping_time"]) >= start_time[snum]) &
                (pd.to_datetime(df_MVBS["first_ping_time"]) <= end_time[snum])
            ]["MVBS_filename"].tolist()
        )
        logger.info(
            f"Found {len(MVBS_filenames)} MVBS files in the specified time range: \n"
            + "".join([f"- {mvbsf}\n" for mvbsf in MVBS_filenames])
        )

        # Skip prediction if no MVBS files found
        if len(MVBS_filenames) == 0:
            logger.info(f"No MVBS files found for slice {snum+1}, skipping")
            continue        

        # Predict on the MVBS files and compute NASC
        try:
            predict_filename_postfix=f"{start_time[snum].strftime("%Y%m%dT%H%M%S")}"

            # predict hake on the MVBS files
            (
                # used for task_compute_NASC_direct
                ds_MVBS_combine,
                da_score_softmax,
                # used for book keeping
                score_filename,
                softmax_filename,
                first_ping_time,
                last_ping_time
            ) = task_predict_hake.with_options(
                task_run_name=f"predict_{predict_filename_postfix}",
                name=f"predict_{predict_filename_postfix}",
            )(
                predict_filename_postfix=predict_filename_postfix,
                MVBS_filenames=MVBS_filenames,
                start_time=start_time[snum],
                end_time=end_time[snum],
                model=model,
                temperature=temperature,
                path_MVBS_zarr=path_MVBS_zarr,
                path_prediction_zarr=path_prediction_zarr,
            )

            # Compute NASC directly from the prediction
            task_compute_NASC.with_options(
                task_run_name=f"NASC_{predict_filename_postfix}",
                name=f"NASC_{predict_filename_postfix}",                
            )(
                NASC_filename=f"NASC_{predict_filename_postfix}.zarr",
                ds_MVBS_combine=ds_MVBS_combine,
                da_score_softmax=da_score_softmax,
                softmax_threshold=softmax_threshold,
                path_NASC_zarr=path_NASC_zarr,  # use the same path as predictions
            )

            # Add prediction slice info to dataframe
            # Only add if NASC is also computed
            if predict_filename_postfix in df_prediction["prediction_filename_postfix"].values:
                logger.info(f"Prediction file {predict_filename_postfix} already exists, updating first and last ping times")
                idx_to_add = df_prediction.index[df_prediction["prediction_filename_postfix"] == predict_filename_postfix]
            else:
                logger.info(f"Adding new prediction file {predict_filename_postfix} to tracking dataframe")
                idx_to_add = len(df_prediction)
            df_prediction.loc[idx_to_add] = [predict_filename_postfix, score_filename, softmax_filename, first_ping_time, last_ping_time]
        except Exception as e:
            errors.append(e)
            logger.error(f"Error during prediction for slice {snum+1}: {e}")
        
        # Save updated prediction info dataframe
        df_prediction.to_csv(file_prediction_csv, date_format="%Y-%m-%dT%H:%M:%S")

    # Set flow to Failed state if any errors occurred
    if len(errors) > 0:
        error_msg = f"{len(errors)} errors during prediction out of {num_slices} slices"
        async with get_client() as client:
            await client.set_flow_run_state(
                flow_run_id=runtime.flow_run.id,
                state=Failed(message=error_msg)
            )
        raise Exception(error_msg)  # Stop the flow execution


@task(log_prints=True)
def task_predict_hake(
    predict_filename_postfix: str,
    MVBS_filenames: list[str],
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    model: BinaryHakeModel,
    temperature: int = 0.5,
    path_MVBS_zarr: str = "PATH_TO_MVBS_ZARR",
    path_prediction_zarr: str = "PATH_TO_PREDICTION_ZARR",
):
    """
    Predict on a single MVBS file.

    This function combines multiple MVBS files into a single dataset,
    convert it to the input tensor, and feed it into the model for prediction.

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
    # Remove timezone info for slicing
    start_time = start_time.replace(tzinfo=None)
    end_time = end_time.replace(tzinfo=None)

    # Combine MVBS files into a single dataset
    ds_MVBS_combine = xr.open_mfdataset(
        [Path(path_MVBS_zarr) / mvbsf for mvbsf in MVBS_filenames],
        parallel=True,
        coords="minimal",
        data_vars="minimal",
        compat='override',
        chunks={"channel": -1, "ping_time": -1, "depth": -1},  # load everything into 1 chunk
        engine="zarr",  # use zarr engine for reading
    ).sel(
        # slice start/end, end exclusive
        ping_time=slice(start_time, end_time-pd.to_timedelta("10milliseconds")),
        depth=slice(None, 590)  # slice to what the model expects
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
            "scatterer_class": ["background", "hake"],
            "depth": ds_MVBS_combine["depth"].values,
            "ping_time": ds_MVBS_combine["ping_time"].values,
        },
        name="score",
    )
    da_score_softmax = xr.DataArray(
        score_tensor_softmax,
        coords={
            "scatterer_class": ["background", "hake"],
            "depth": ds_MVBS_combine["depth"].values,
            "ping_time": ds_MVBS_combine["ping_time"].values,
        },
        name="softmax_score",
    )

    # Save to zarr
    score_filename = f"score_{predict_filename_postfix}.zarr"
    softmax_filename = f"softmax_{predict_filename_postfix}.zarr"
    da_score.chunk({"scatterer_class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=Path(path_prediction_zarr) / score_filename,
        mode="w",
        consolidated=True,
    )
    da_score_softmax.chunk({"scatterer_class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=Path(path_prediction_zarr) / softmax_filename,
        mode="w",
        consolidated=True,
    )

    return (
        ds_MVBS_combine,
        da_score_softmax,
        score_filename,
        softmax_filename,
        pd.to_datetime(ds_MVBS_combine["ping_time"][0].values),
        pd.to_datetime(ds_MVBS_combine["ping_time"][-1].values)
    )


@task(log_prints=True)
def task_compute_NASC(
    NASC_filename: str,
    ds_MVBS_combine: xr.Dataset,
    da_score_softmax: xr.DataArray,
    softmax_threshold: float = 0.5,
    path_NASC_zarr: str = "PATH_TO_SAVE_NASC_ZARR",
):
    logger = get_run_logger()

    # Prepare softmax score dimensions for apply_mask
    da_score_softmax = (
        da_score_softmax
        .sel(scatterer_class="hake")  # only need hake class
        .transpose("ping_time", "depth")  # TODO: remove once update echopype to 0.10.2
        .drop_vars("scatterer_class")
    )

    # Apply mask based on threshold
    ds_MVBS_combine_masked = ep.mask.apply_mask(
        source_ds=ds_MVBS_combine,
        mask=da_score_softmax > softmax_threshold,
        var_name="Sv",
        fill_value=np.nan,
    )

    # Compute NASC from MVBS and hake prediction
    ds_NASC = ep.commongrid.compute_NASC(
        ds_Sv=ds_MVBS_combine_masked,
        range_bin="10m",
        dist_bin="0.5nmi"
    )

    # Save to zarr
    logger.info(f"Saving NASC to zarr: {NASC_filename}")
    ds_NASC.to_zarr(
        store=Path(path_NASC_zarr) / NASC_filename,
        mode="w",
        consolidated=True,
    )


# @flow(log_prints=True) #, timeout_seconds=600)
# async def flow_compute_NASC(
#     time_offset_seconds: float = 0.0,
#     slice_mins: int = 10,
#     num_slices: int = 3,
#     softmax_threshold: float = 0.5,
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

#     # Load MVBS and prediction info dataframes
#     df_MVBS = pd.read_csv(
#         MVBS_csv_path,
#         index_col=0,
#         date_format="ISO8601",
#         parse_dates=["first_ping_time", "last_ping_time"]
#     )
#     df_prediction = pd.read_csv(
#         prediction_csv_path,
#         index_col=0,
#         date_format="ISO8601",
#         parse_dates=["first_ping_time", "last_ping_time"]
#     )

#     # Compute slice time range
#     start_time, end_time = get_slice_start_end_times(
#         end_time=end_time, slice_mins=slice_mins, num_slices=num_slices
#     )

#     # Sequentially compute NASC
#     errors = []
#     for snum in range(num_slices):
#         logger.info(f"Slice {snum+1}: {start_time[snum]} to {end_time[snum]}")

#         # Get MVBS files in the specified time range
#         MVBS_filenames = df_MVBS[
#             (pd.to_datetime(df_MVBS["last_ping_time"]) >= start_time[snum]) &
#             (pd.to_datetime(df_MVBS["first_ping_time"]) <= end_time[snum])
#         ]["MVBS_filename"].tolist()
#         logger.info(f"Found {len(MVBS_filenames)} MVBS files in the specified time range")

#         # Get softmax files in the specified time range
#         softmax_filenames = df_prediction[
#             (pd.to_datetime(df_prediction["last_ping_time"]) >= start_time[snum]) &
#             (pd.to_datetime(df_prediction["first_ping_time"]) <= end_time[snum])
#         ]["softmax_filename"].tolist()
#         logger.info(f"Found {len(softmax_filenames)} softmax files in the specified time range")

#         # If no MVBS or softmax files found, skip this slice
#         if len(MVBS_filenames) == 0 or len(softmax_filenames) == 0:
#             logger.info(f"No MVBS or softmax files found for slice {snum+1}, skipping")
#             continue

#         # Compute NASC for this slice
#         try:
#             NASC_filename = f"NASC_{start_time[snum].strftime('%Y%m%dT%H%M%S')}.zarr"
#             task_compute_NASC_load_file.with_options(
#                 task_run_name=NASC_filename,
#                 name=NASC_filename,
#             )(
#                 NASC_filename=NASC_filename,
#                 MVBS_filenames=MVBS_filenames,
#                 softmax_filenames=softmax_filenames,
#                 start_time=start_time[snum],
#                 end_time=end_time[snum],
#                 softmax_threshold=softmax_threshold
#             )

#         except Exception as e:
#             errors.append(e)
#             logger.error(f"Error during NASC computation for slice {snum+1}: {e}")

#     # Set flow to Failed state if any errors occurred
#     if len(errors) > 0:
#         error_msg = f"{len(errors)} errors during NASC computation out of {num_slices} slices"
#         async with get_client() as client:
#             await client.set_flow_run_state(
#                 flow_run_id=runtime.flow_run.id,
#                 state=Failed(message=error_msg)
#             )
#         raise Exception(error_msg)


# @task(log_prints=True)
# def task_compute_NASC_load_file(
#     NASC_filename: str,
#     MVBS_filenames: list[str],
#     softmax_filenames: list[str],
#     start_time: pd.Timestamp,
#     end_time: pd.Timestamp,
#     softmax_threshold: float = 0.5,
# ):
#     """
#     Compute NASC from MVBS and hake prediction files.

#     Parameters
#     ----------
#     MVBS_filenames : list[str]
#         List of MVBS filenames to process.
#     softmax_filenames : list[str]
#         List of softmax filenames to process.
#     start_time : pd.Timestamp
#         The start time for the NASC computation.
#     end_time : pd.Timestamp
#         The end time for the NASC computation.
#     threshold : float
#         Threshold for hake detection.
#     """
#     logger = get_run_logger()

#     # Combine MVBS files into a single dataset
#     ds_MVBS_combine = xr.open_mfdataset(
#         [MVBS_path / mvbsf for mvbsf in MVBS_filenames],
#         parallel=True,
#         coords="minimal",
#         data_vars="minimal",
#         compat='override',
#         chunks={"channel": -1, "ping_time": -1, "depth": -1},  # load everything into 1 chunk
#         engine="zarr",  # use zarr engine for reading
#     ).sel(
#         # slice start/end, end exclusive
#         ping_time=slice(start_time, end_time-pd.to_timedelta("10milliseconds")),
#         depth=slice(None, 590)  # slice depth to match hake model input
#     ).transpose("channel", "ping_time", "depth")  # transpose to match dimension order of mask
#                                                   # TODO: remove once update echopype to 0.10.2

#     # Combine softmax files into a single dataset
#     da_softmax_combine = xr.open_mfdataset(
#         [prediction_path / softmaxf for softmaxf in softmax_filenames],
#         parallel=True,
#         coords="minimal",
#         data_vars="minimal",
#         compat='override',
#         chunks={"scatterer_class": -1, "ping_time": -1, "depth": -1},  # load everything into 1 chunk
#         engine="zarr",  # use zarr engine for reading
#     ).sel(
#         # slice start/end, end exclusive
#         ping_time=slice(start_time, end_time-pd.to_timedelta("10milliseconds"))
#     ).to_dataarray()
#     da_softmax_combine = (
#         da_softmax_combine.isel(variable=0).sel(scatterer_class="hake")
#         .transpose("ping_time", "depth")  # TODO: remove once update echopype to 0.10.2
#     ).drop_vars(["scatterer_class", "variable"])
    
#     # Apply mask based on threshold
#     ds_MVBS_combine_masked = ep.mask.apply_mask(
#         source_ds=ds_MVBS_combine,
#         mask=da_softmax_combine > softmax_threshold,
#         var_name="Sv",
#         fill_value=np.nan,
#     )

#     # Compute NASC from MVBS and hake prediction
#     ds_NASC = ep.commongrid.compute_NASC(
#         ds_Sv=ds_MVBS_combine_masked,
#         range_bin="10m",
#         dist_bin="0.5nmi"
#     )

#     # Save to zarr
#     logger.info(f"Saving NASC to zarr: {NASC_filename}")
#     ds_NASC.to_zarr(
#         store=NASC_path / NASC_filename,
#         mode="w",
#         consolidated=True,
#         # storage_options=config.output.storage_options_dict,
#     )
