from pathlib import Path
import datetime
from yaml import safe_load

import numpy as np
import pandas as pd
import xarray as xr
import numpy as np

import echopype as ep

from prefect import deploy, flow, task, get_run_logger
from prefect.variables import Variable
from prefect_shell import ShellOperation

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from utils import get_MVBS_tensor, get_hake_model, round_up_minutes, extract_mins

import torch
from src.model.BinaryHakeModel import BinaryHakeModel


# Turn on verbose logging for echopype
# otherwise all logging will be muted
ep.utils.log.verbose()


# Set up paths
data_path = Path("/Users/wujung/code_git/echodataflow/temp_data")
raw_path = data_path / "raw"
Sv_path = data_path / "Sv"
MVBS_path = data_path / "MVBS"
prediction_path = data_path / "prediction"


# Initiate counter for raw file copy
df_raw = pd.read_csv(
    data_path / "SH2306_raw_files.txt",
    # Path(__file__).parent / "../../temp_raw/SH2306_raw_files.txt", 
    sep=r'\s+',  # multiple spaces as delimiter
    header=None,  # no header
    names=["date", "time", "size", "filename"]  # assign column names
)

# Info dataframe for which raw files to convert to Sv
Sv_csv_path = data_path / "SH2306_Sv_files.csv"
if not Sv_csv_path.exists():
    df_Sv = pd.DataFrame(
        columns=["raw_filename", "Sv_filename", "first_ping_time", "last_ping_time"]
    )
    df_Sv.to_csv(Sv_csv_path)

# Info dataframe for MVBS files
MVBS_csv_path = data_path / "SH2306_MVBS_files.csv"
if not MVBS_csv_path.exists():
    df_MVBS = pd.DataFrame(
        columns=["MVBS_filename", "first_ping_time", "last_ping_time"]
    )
    df_MVBS.to_csv(MVBS_csv_path)



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


@flow(log_prints=True)
def flow_raw2Sv():
    # Load info dataframe containing raw to Sv correspondence
    df_Sv = pd.read_csv(Sv_csv_path, index_col=0)
    raw_files_in_folder = set([ff.name for ff in raw_path.glob("*.raw")])
    raw_files_in_df = set(df_Sv["raw_filename"].tolist())

    # Find new files to process
    new_files = raw_files_in_folder.difference(raw_files_in_df)
    print(f"Found {len(new_files)} new files to process")

    # Convert raw files to Sv in parallel
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

    # Add new entries to df_Sv
    if len(results) > 0:
        df_new = pd.DataFrame(
            results,
            columns=["raw_filename", "Sv_filename", "first_ping_time", "last_ping_time"]
        )
        
        # Concatenate with existing df_Sv and save
        df_Sv = pd.concat([df_Sv, df_new], ignore_index=True)
        df_Sv.to_csv(Sv_csv_path, date_format="%Y-%m-%dT%H:%M:%S.%f")
        print(f"Added {len(new_files)} new entries to tracking CSV")


@task(log_prints=True)#, task_run_name="{raw_path.name}")
def task_raw2Sv(raw_path: str):
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
        encode_mode="power",
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

    return out_path.name, ds_Sv["ping_time"][0].values, ds_Sv["ping_time"][-1].values


@flow(log_prints=True)
def flow_create_MVBS(
    end_time: str = "2023-08-04T16:00:00",
    slice_time_min: int = 10,
    num_slices: int = 3,
):
    """
    Process raw files to create MVBS files of specified length.

    Parameters
    ----------
    end_time : str
        The end time for the MVBS computation in ISO format (e.g., "2023-10-01 12:00:00").
    slice_time_min : int
        The length of each slice in mins in a format recognized by pandas.
    num_slices : int
        The number of slices to create.

    Examples
    --------
    >>> process_MVBS(end_time="2023-10-01 12:00:00", slice_time_len="20min", num_slices=3)
    """
    logger = get_run_logger()

    # Load parameters from variables or set defaults
    slice_time_min = Variable.get("create_mvbs__slice_time_min", default=slice_time_min)
    num_slices = Variable.get("create_mvbs__num_slices", default=num_slices)

    # Set end_time to current time - offset if not provided
    curr_time_offset_seconds = Variable.get("curr_time_offset_seconds", default=0)
    if Variable.get("flow_start_time") is not None:
        end_time = round_up_minutes(
            (
                datetime.datetime.now()
                - datetime.timedelta(seconds=curr_time_offset_seconds)
            ),
            interval=slice_time_min,
        )

    logger.info(
        "flow started with parameters:\n"
        f"- end_time: {end_time}\n"
        f"- slice_time_min: {slice_time_min}\n"
        f"- num_slices: {num_slices}\n"
    )

    # Load Sv and MVBS info dataframes
    df_Sv = pd.read_csv(Sv_csv_path, index_col=0)
    df_MVBS = pd.read_csv(MVBS_csv_path, index_col=0)

    # Compute slice time range
    end_time = pd.to_datetime(end_time)
    slice_time_min = pd.to_timedelta(f"{slice_time_min}min")
    start_time = sorted([end_time - s * slice_time_min for s in np.arange(num_slices)+1])
    end_time = [st + slice_time_min for st in start_time]    
    
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

    # Save updated MVBS info dataframe
    df_MVBS.to_csv(MVBS_csv_path, date_format="%Y-%m-%dT%H:%M:%S.%f")


@task(log_prints=True)
def task_create_MVBS(MVBS_filename: str, start_time: pd.Timestamp, end_time: pd.Timestamp, Sv_filenames: list[str]):
    """
    Create MVBS from Sv files in the specified time range.
    Parameters
    ----------
    start_time : pd.Timestamp
        The start time for the MVBS computation.
    end_time : pd.Timestamp
        The end time for the MVBS computation.    
    Sv_filenames : list[str]
        List of Sv filenames to process.
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

    return ds_MVBS["ping_time"][0].values, ds_MVBS["ping_time"][-1].values


@flow(log_prints=True)
def flow_predict_hake(
    end_time: str = "2023-08-04T16:05:00",
    slice_time_min: int = 10,
    num_slices: int = 3,
    temperature: float = 0.5,
):
    """
    Predict on MVBS files of specified length.

    Parameters
    ----------
    end_time : str
        The end time for the MVBS computation in ISO format (e.g., "2023-10-01 12:00:00").
    slice_time_len : str
        The length of each slice in a format recognized by pandas (e.g., "20min").
    num_slices : int
        The number of slices to create.
    """
    logger = get_run_logger()

    # Load binary hake models with weights
    model_epoch = 85
    model_folder = "/Users/wujung/code_git/echodataflow/temp_model/model_160_epochs/model_weights"
    model_path = f"{model_folder}/binary_hake_model_1.0m_bottom_offset_1.0m_depth_2017_2019_epoch_{model_epoch:03d}.ckpt"
    model = get_hake_model(model_path)

    # Load parameters from variables or set defaults
    slice_time_min = Variable.get("predict_hake__slice_time_min", default=slice_time_min)
    num_slices = Variable.get("predict_hake__num_slices", default=num_slices)

    # Set end_time to current time - offset if not provided
    curr_time_offset_seconds = Variable.get("curr_time_offset_seconds", default=0)
    if Variable.get("flow_start_time") is not None:
        end_time = round_up_minutes(
            (
                datetime.datetime.now()
                - datetime.timedelta(seconds=curr_time_offset_seconds)
            ),
            interval=slice_time_min,
        )

    logger.info(
        "flow started with parameters:\n"
        f"- end_time: {end_time}\n"
        f"- slice_time_min: {slice_time_min}\n"
        f"- num_slices: {num_slices}\n"
    )

    # Compute slice time range
    end_time = pd.to_datetime(end_time)
    slice_time_min = pd.to_timedelta(f"{slice_time_min}min")
    start_time = sorted([end_time - s * slice_time_min for s in np.arange(num_slices)+1])
    end_time = [st + slice_time_min for st in start_time]

    # Load Sv and MVBS info dataframes
    df_MVBS = pd.read_csv(MVBS_csv_path, index_col=0)

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
        predict_filename_postfix=f"{start_time[snum].strftime("%Y%m%dT%H%M%S")}"
        task_predict_hake.with_options(
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
    MVBS_filenames : str
        Path to the MVBS file.
    model : 
        The model to use for prediction.
    """
    # Combine MVBS files into a single dataset
    ds_MVBS = xr.open_mfdataset(
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
    input_tensor = get_MVBS_tensor(ds_MVBS)

    # Predict using the model
    score_tensor = model(input_tensor).detach().squeeze(0)
    score_tensor_softmax = torch.nn.functional.softmax(score_tensor / temperature, dim=0)

    # Assemble output DataArrays
    da_score = xr.DataArray(
        score_tensor,
        coords={
            "class": ["background", "hake"],
            "depth": ds_MVBS["depth"].sel(depth=slice(None, 590)).values,
            "ping_time": ds_MVBS["ping_time"].values,
        }
    )
    da_score_softmax = xr.DataArray(
        score_tensor_softmax,
        coords={
            "class": ["background", "hake"],
            "depth": ds_MVBS["depth"].sel(depth=slice(None, 590)).values,
            "ping_time": ds_MVBS["ping_time"].values,
        }
    )

    # Save to zarr
    da_score.chunk({"class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=prediction_path / f"score_{predict_filename_postfix}.zarr",
        mode="w",
        consolidated=True,
    )
    da_score_softmax.chunk({"class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=prediction_path / f"softmax_{predict_filename_postfix}.zarr",
        mode="w",
        consolidated=True,
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

        # Get results
        output_lines = file_upload_process.fetch_result()
    


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
    Variable.set("curr_time_offset_seconds", curr_time_offset.total_seconds(), overwrite=True)

    # Populate other variables
    for flow_name, flow_params in config.items():
        for p in flow_params:
            # format: {FLOW_NAME}__{PARAM_NAME}
            Variable.set(f"{flow_name.lower()}__{p}", config[flow_name][p], overwrite=True)

    # freq_min = 2
    deploy(
        copy_raw.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:copy_raw"
        ).to_deployment(
            name="copy-raw",
            # work_pool_name="local",
            # cron=f"*/{freq_min} * * * *",  # run every freq_min
        ),
        flow_raw2Sv.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_raw2Sv",
        ).to_deployment(
            name="raw2Sv",
        ),
        flow_create_MVBS.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_create_MVBS",
        ).to_deployment(
            name="create-MVBS",
            cron=f"*/{config["create_MVBS"]["slice_time_min"]} * * * *",
        ),
        flow_predict_hake.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_predict_hake",
        ).to_deployment(
            name="predict-hake",
            cron=f"*/{config["predict_hake"]["slice_time_min"]} * * * *",
        ),
        flow_file_upload.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_file_upload",
        ).to_deployment(
            name="file-upload",
        ),
        work_pool_name="local",
    )
