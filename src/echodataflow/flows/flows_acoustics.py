from __future__ import annotations

import asyncio
import datetime
from pathlib import Path

import pandas as pd

import echopype as ep

from prefect import flow, get_run_logger, get_client
from prefect_dask import DaskTaskRunner
from prefect.states import Cancelled, Failed
from prefect import runtime

from echodataflow.flows.flows_helper import deployment_already_running
from echodataflow.operations.operations_acoustics import (
    CreateMVBSResult,
    CreateMVBSSettings,
    CreateMVBSWorkItem,
    RawToSvResult,
    RawToSvSettings,
    RawToSvWorkItem,
)
from echodataflow.tasks.task_acoustics import (
    task_create_MVBS,
    task_raw2Sv,
)
from echodataflow.utils.utils import (
    round_up_mins,
    get_slice_start_end_times,
    extract_datetime_from_filename,
)

# Turn on verbose logging for echopype
# otherwise all logging will be muted
ep.utils.log.verbose()


@flow(
    log_prints=True,
    task_runner=DaskTaskRunner()
)
def flow_raw2Sv(
    exclude_before: str|None = None,
    exclude_raw_file: list[str] = [],
    parallel: bool = False,
    encode_mode: str = "power",
    waveform_mode: str = "CW",
    depth_offset: float = 9.5,
    sonar_model: str = "EK80",
    datagram_type: str|None = None,
    nmea_sentence: str|None = None,
    filename_pattern: str = "*.raw",
    path_main: str = "",
    path_raw: str = "",
    file_Sv_csv: str = "Sv_files.csv",
    new_file_num_limit: int = 50,
):

    # Check if the deployment is already running
    already_running = asyncio.run(deployment_already_running())
    if already_running:
        async def cancel_run():
            async with get_client() as client:
                await client.set_flow_run_state(
                    flow_run_id=runtime.flow_run.id,
                    state=Cancelled(message="Another instance of this flow is already running")
                )
        asyncio.run(cancel_run())
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

    # Exclude raw files before exclude_before datetime
    if exclude_before is None:
        raw_files_in_folder = set([filename.name for filename in path_raw.glob(filename_pattern)])
    else:
        raw_files_in_folder = set([
            filename.name for filename in path_raw.glob(filename_pattern)
            if extract_datetime_from_filename(filename.name) >= datetime.datetime.fromisoformat(exclude_before)
        ])

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

    # Reprocess last file in case it was incomplete
    if last_raw_filename:
        print(f"Reprocess {last_raw_filename}")
        new_files.add(last_raw_filename)

    # Skip files in exclude_raw_file list
    if len(exclude_raw_file) > 0:
        print(f"Exclude {exclude_raw_file} from processing")
        new_files.difference_update(set(exclude_raw_file))

    # Sort new files
    new_files = sorted(list(new_files))

    # Limit number of new files to process
    if new_file_num_limit != -1 and len(new_files) > new_file_num_limit:
        print(
            f"More than {new_file_num_limit} new files to process. "
            f"Limiting to first {new_file_num_limit} files."
        )
        new_files = new_files[:new_file_num_limit]
    print(
        f"Files to process: \n"
        + "".join([f"- {nf}\n" for nf in new_files])
    )

    settings = RawToSvSettings(
        output_directory=path_Sv_zarr,
        encode_mode=encode_mode,
        waveform_mode=waveform_mode,
        depth_offset=depth_offset,
        sonar_model=sonar_model,
        datagram_type=datagram_type,
        nmea_sentence=nmea_sentence,
    )
    errors = []
    results: list[RawToSvResult] = []

    if parallel:
        # Convert raw files to Sv in parallel
        print("Processing raw files in parallel")
        future_all = []
        for nf in new_files:
            new_processed_raw = task_raw2Sv.with_options(
                task_run_name=nf, name=nf, retries=3
            )
            future = new_processed_raw.submit(
                RawToSvWorkItem(raw_path=str(path_raw / nf)),
                settings,
            )
            future_all.append(future)

        for ff in future_all:
            task_result = ff.result()
            results.append(task_result)

    else:
        # Convert raw files to Sv sequentially
        print("Processing raw files sequentially")
        for nf in new_files:
            try:
                print(f"Converting {nf}")
                task_result = task_raw2Sv.with_options(
                    task_run_name=nf, name=nf, retries=3
                )(
                    RawToSvWorkItem(raw_path=str(path_raw / nf)),
                    settings,
                )
                results.append(task_result)
            except Exception as e:
                errors.append(e)
                print(f"Error converting {nf}: {e}")

    # Add new entries to df_Sv
    if len(results) > 0:
        df_new = pd.DataFrame(
            [
                {
                    "raw_filename": result.raw_filename,
                    "Sv_filename": result.sv_filename,
                    "first_ping_time": result.first_ping_time,
                    "last_ping_time": result.last_ping_time,
                }
                for result in results
            ]
        )
        
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
        async def set_failed_state():
            async with get_client() as client:
                await client.set_flow_run_state(
                    flow_run_id=runtime.flow_run.id,
                    state=Failed(message=error_msg)
                )
        asyncio.run(set_failed_state())
        raise Exception(error_msg)

@flow(log_prints=True)
async def flow_create_MVBS(
    time_offset_seconds: float = 0.0,
    slice_mins: int = 10,
    num_slices: int = 3,
    range_bin: str = "1m",
    ping_time_bin: str = "5s",
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
        # raise ValueError("Sv zarr store does not exist, check raw2Sv flow!")
        logger.info("Sv zarr store does not exist, check raw2Sv flow! Creating empty folder for now.")
        path_Sv_zarr.mkdir(parents=True, exist_ok=True)
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
    if not df_Sv.empty:
        if df_Sv["last_ping_time"].dt.tz is None:
            df_Sv["last_ping_time"] = df_Sv["last_ping_time"].dt.tz_localize("UTC")
        if df_Sv["first_ping_time"].dt.tz is None:
            df_Sv["first_ping_time"] = df_Sv["first_ping_time"].dt.tz_localize("UTC")
    else:
        logger.info(
            "Sv info csv is empty, raw2Sv flow may have just started! "
            "No MVBS can be created, exiting flow."
        )
        return

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

    settings = CreateMVBSSettings(
        sv_directory=path_Sv_zarr,
        output_directory=path_MVBS_zarr,
        range_bin=range_bin,
        ping_time_bin=ping_time_bin,
    )

    # Sequentially create MVBS slices
    errors = []
    results: list[CreateMVBSResult] = []
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
            result = task_create_MVBS.with_options(
                task_run_name=MVBS_filename,
                name=MVBS_filename,
            )(
                CreateMVBSWorkItem(
                    start_time=start_time[snum],
                    end_time=end_time[snum],
                    sv_filenames=tuple(Sv_filenames),
                    mvbs_filename=MVBS_filename,
                ),
                settings,
            )
            results.append(result)
        except Exception as e:
            errors.append(e)
            logger.error(f"Error during MVBS creation for slice {snum+1}: {e}")

    # Add MVBS slice info to dataframe
    for result in results:
        if result.mvbs_filename in df_MVBS["MVBS_filename"].values:
            logger.info(
                f"MVBS file {result.mvbs_filename} already exists, "
                "updating first and last ping times"
            )
            idx_to_add = df_MVBS.index[
                df_MVBS["MVBS_filename"] == result.mvbs_filename
            ]
        else:
            logger.info(
                f"Adding new MVBS file {result.mvbs_filename} to tracking dataframe"
            )
            idx_to_add = len(df_MVBS)
        df_MVBS.loc[idx_to_add] = [
            result.mvbs_filename,
            result.first_ping_time,
            result.last_ping_time,
        ]

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
