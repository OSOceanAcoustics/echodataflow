from pathlib import Path
import datetime
import re

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

from utils import get_MVBS_tensor, get_hake_model

import torch
from src.model.BinaryHakeModel import BinaryHakeModel


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

# Info dataframe for which raw files to process
Sv_csv_path = data_path / "SH2306_Sv_files.csv"
if not Sv_csv_path.exists():
    df_Sv = pd.DataFrame(
        columns=["raw_filename", "Sv_filename", "last_ping_time", "first_ping_time"]
    )
    df_Sv.to_csv(Sv_csv_path)



@flow(log_prints=True)
def flow_loggin_test():
    print("This is flow_loggin_test!")
    print(f"Executed at {datetime.datetime.now(datetime.UTC)}")


@flow(log_prints=True)
def copy_raw():
    print("Copy raw files to simulate data generation")
    print(f"Executed at {datetime.datetime.now(datetime.UTC)}")
    counter = Variable.get("counter_raw_copy")

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

    # # Sequentially process new files
    # results = []
    # for nf in new_files:
    #     Sv_filename, first_ping_time, last_ping_time = raw2Sv.with_options(
    #         task_run_name=nf, name=nf, retries=3
    #     )(raw_path / nf)
    #     results.append([nf, Sv_filename, first_ping_time, last_ping_time])

    # Add new entries to df_Sv
    if len(results) > 0:
        df_new = pd.DataFrame(
            results,
            columns=["raw_filename", "Sv_filename", "last_ping_time", "first_ping_time"]
        )
        
        # Concatenate with existing df_Sv and save
        df_Sv = pd.concat([df_Sv, df_new], ignore_index=True)
        df_Sv.to_csv(Sv_csv_path)
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
    first_ping_time = ds_Sv["ping_time"][-1].values
    last_ping_time = ds_Sv["ping_time"][0].values

    return out_path.name, first_ping_time, last_ping_time


@flow(log_prints=True)
def flow_create_MVBS(
    end_time: str = "2023-08-04T16:00:00",
    slice_time_len: str = "10min",
    num_slices: int = 3
):
    """
    Process raw files to create MVBS files of specified length.

    Parameters
    ----------
    end_time : str
        The end time for the MVBS computation in ISO format (e.g., "2023-10-01 12:00:00").
    slice_time_len : str
        The length of each slice in a format recognized by pandas (e.g., "20min").
    num_slices : int
        The number of slices to create.

    Examples
    --------
    >>> process_MVBS(end_time="2023-10-01 12:00:00", slice_time_len="20min", num_slices=3)
    """
    logger = get_run_logger()

    # Load info dataframe containing raw to Sv correspondence
    df_Sv = pd.read_csv(Sv_csv_path, index_col=0)

    # Compute slice time range
    end_time = pd.to_datetime(end_time)
    slice_time_len = pd.to_timedelta(slice_time_len)
    start_time = sorted([end_time - s * slice_time_len for s in np.arange(num_slices)+1])
    end_time = [st + slice_time_len for st in start_time]    
    
    # Sequentially create MVBS slices
    for ns in range(num_slices):
        logger.info(f"Slice {ns+1}: {start_time[ns]} to {end_time[ns]}")

        # Get Sv files in the specified time range
        Sv_filenames = df_Sv[
            (pd.to_datetime(df_Sv["last_ping_time"]) >= start_time[ns]) &
            (pd.to_datetime(df_Sv["first_ping_time"]) <= end_time[ns])
        ]["Sv_filename"].tolist()
        logger.info(f"Found {len(Sv_filenames)} Sv files in the specified time range")

        # If no Sv files found, skip this slice
        if len(Sv_filenames) == 0:
            logger.info(f"No Sv files found for slice {ns+1}, skipping")
            continue

        MVBS_run_name = (
            f"MVBS_{start_time[ns].strftime("%Y%m%dT%H%M%S")}"
            f"_{end_time[ns].strftime("%Y%m%dT%H%M%S")}"
        )
        task_create_MVBS.with_options(
            task_run_name=MVBS_run_name, name=MVBS_run_name,
        )(start_time=start_time[ns], end_time=end_time[ns], Sv_filenames=Sv_filenames)


@task(log_prints=True)
def task_create_MVBS(start_time: pd.Timestamp, end_time: pd.Timestamp, Sv_filenames: list[str]):
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
    )

    # Compute MVBS for the slice
    out_path = MVBS_path / (
        f"MVBS_{start_time.strftime('%Y%m%dT%H%M%S')}"
        f"_{end_time.strftime('%Y%m%dT%H%M%S')}.zarr"
    )
    ds_MVBS = ep.commongrid.compute_MVBS(
        ds_Sv=ds_Sv.sel(ping_time=slice(start_time, end_time)),  # slice start/end
        range_var="depth",
        range_bin='1m',
        ping_time_bin='5s',
        reindex=False,
        fill_value=np.nan,
    )

    # Save to zarr
    ds_MVBS.chunk({"channel": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=out_path,  # existing MVBS will be overwritten
        mode="w",
        consolidated=True,
        # storage_options=config.output.storage_options_dict,
    )


@flow(log_prints=True)
def flow_predict_MVBS(
    mvbs_file_list: list[str] = ["MVBS_20230804T154000_20230804T155000.zarr"],
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

    # mvbs_file_list = ["MVBS_20230804T153000_20230804T154000.zarr"]

    for mvbs_file in mvbs_file_list:
        logger.info(f"Processing MVBS file: {mvbs_file}")
        
        # Check if the file exists
        mvbs_path = MVBS_path / mvbs_file
        if not mvbs_path.exists():
            logger.warning(f"MVBS file {mvbs_file} does not exist, skipping")
            continue

        # Predict on the MVBS file
        task_predict_MVBS.with_options(
            task_run_name=mvbs_file, name=mvbs_file,
        )(mvbs_file=mvbs_path, model=model, temperature=temperature)



@task(log_prints=True)
def task_predict_MVBS(
    mvbs_file: str,
    model: BinaryHakeModel,
    temperature: int = 0.5,
):
    """
    Predict on a single MVBS file.
    
    Parameters
    ----------
    mvbs_file : str
        Path to the MVBS file.
    model : 
        The model to use for prediction.
    """
    # Load MVBS data
    ds_MVBS = xr.open_dataset(mvbs_file, engine="zarr")

    # Prepare input tensor: slice depth and ensure order of coordinates
    mvbs_tensor = get_MVBS_tensor(ds_MVBS)

    # Predict using the model
    score_tensor = model(mvbs_tensor).detach().squeeze(0)
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
    mvbs_time = re.match(r"MVBS_(\w+).zarr", mvbs_file.name).group(1)
    da_score.chunk({"class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=prediction_path / f"score_{mvbs_time}.zarr",
        mode="w",
        consolidated=True,
    )
    da_score_softmax.chunk({"class": -1, "ping_time": -1, "depth": -1}).to_zarr(
        store=prediction_path / f"softmax_{mvbs_time}.zarr",
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
    freq_min = 2
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
        ),
        flow_predict_MVBS.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_predict_MVBS",
        ).to_deployment(
            name="predict-MVBS",
        ),
        flow_file_upload.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_file_upload",
        ).to_deployment(
            name="file-upload",
        ),
        flow_loggin_test.from_source(
            source=str(Path(__file__).parent),
            entrypoint="prototype.py:flow_loggin_test",
        ).to_deployment(
            name="logging-test",
        ),
        work_pool_name="local",
    )
