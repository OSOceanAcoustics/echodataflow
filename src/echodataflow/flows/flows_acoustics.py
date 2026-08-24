from __future__ import annotations

import asyncio
import datetime
from pathlib import Path

import pandas as pd

import echopype as ep

from prefect import flow, get_run_logger, get_client
from prefect.futures import as_completed
from prefect.states import Failed
from prefect import runtime

from echodataflow.deployment.task_runners import dask_task_runner_from_environment
from echodataflow.utils.manifests import (
    MVBS_COLUMNS_POSTPROCESSING,
    MVBS_COLUMNS_REALTIME,
    SV_COLUMNS_REALTIME,
    SV_COLUMNS_POSTPROCESSING,
    filter_time_range,
    read_manifest,
    write_manifest,
)
from echodataflow.operations.operations_acoustics import (
    CreateMVBSResult,
    CreateMVBSSettings,
    CreateMVBSWorkItem,
    RawToSvResult,
    RawToSvSettings,
    RawToSvWorkItem,
)
from echodataflow.operations.operations_postprocessing import (
    build_MVBS_ledger,
    build_Sv_ledger,
    plan_mvbs_slices,
    read_or_create_ledger,
)
from echodataflow.operations.operations_storage import S3CopySettings, S3CopyWorkItem
from echodataflow.tasks.tasks_acoustics import (
    task_create_MVBS,
    task_raw2Sv,
)
from echodataflow.tasks.tasks_postprocessing import task_s3_raw2Sv
from echodataflow.utils.utils import (
    round_up_mins,
    get_slice_start_end_times,
    extract_datetime_from_filename,
)

# Turn on verbose logging for echopype
# otherwise all logging will be muted
ep.utils.log.verbose()


@flow(log_prints=True, task_runner=dask_task_runner_from_environment())
def flow_raw2Sv(
    exclude_before: str | None = None,
    exclude_raw_file: list[str] = [],
    parallel: bool = False,
    encode_mode: str = "power",
    waveform_mode: str = "CW",
    depth_offset: float = 9.5,
    sonar_model: str = "EK80",
    datagram_type: str | None = None,
    nmea_sentence: str | None = None,
    filename_pattern: str = "*.raw",
    path_main: str = "",
    path_raw: str = "",
    file_Sv_csv: str = "Sv_files.csv",
    new_file_num_limit: int = 50,
    add_depth: bool = True,
    add_location: bool = True,
    add_splitbeam_angle: bool = False,
):

    # Assemble paths
    path_Sv_zarr = Path(path_main) / "Sv"
    file_Sv_csv = Path(path_main) / file_Sv_csv
    path_raw = Path(path_raw)

    # Set up folder to store converted Sv zarr
    if not path_Sv_zarr.exists():
        path_Sv_zarr.mkdir(parents=True, exist_ok=True)
    path_Sv_zarr = str(path_Sv_zarr)  # convert backto string to pass into task

    # Load info dataframe containing raw to Sv correspondence
    sv_manifest_exists = file_Sv_csv.exists()
    df_Sv = read_manifest(
        file_Sv_csv,
        SV_COLUMNS_REALTIME,
        ["first_ping_time", "last_ping_time"],
    )
    if not sv_manifest_exists:
        write_manifest(df_Sv, file_Sv_csv)
    if not df_Sv.empty:
        df_Sv.sort_values(by="first_ping_time", inplace=True, ignore_index=True)

    # Exclude raw files before exclude_before datetime
    if exclude_before is None:
        raw_files_in_folder = set([filename.name for filename in path_raw.glob(filename_pattern)])
    else:
        raw_files_in_folder = set(
            [
                filename.name
                for filename in path_raw.glob(filename_pattern)
                if extract_datetime_from_filename(filename.name)
                >= datetime.datetime.fromisoformat(exclude_before)
            ]
        )

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
    print(f"Files to process: \n" + "".join([f"- {nf}\n" for nf in new_files]))

    settings = RawToSvSettings(
        output_directory=path_Sv_zarr,
        encode_mode=encode_mode,
        waveform_mode=waveform_mode,
        depth_offset=depth_offset,
        sonar_model=sonar_model,
        datagram_type=datagram_type,
        nmea_sentence=nmea_sentence,
        add_depth=add_depth,
        add_location=add_location,
        add_splitbeam_angle=add_splitbeam_angle,
    )
    errors = []
    results: list[RawToSvResult] = []

    if parallel:
        # Convert raw files to Sv in parallel
        print("Processing raw files in parallel")
        future_all = []
        for nf in new_files:
            new_processed_raw = task_raw2Sv.with_options(task_run_name=nf, name=nf, retries=3)
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
                task_result = task_raw2Sv.with_options(task_run_name=nf, name=nf, retries=3)(
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
                    "raw_filename": result.filename_raw,
                    "Sv_filename": result.filename_Sv,
                    "first_ping_time": result.first_ping_time,
                    "last_ping_time": result.last_ping_time,
                }
                for result in results
            ]
        )
        for column in ["first_ping_time", "last_ping_time"]:
            df_new[column] = pd.to_datetime(df_new[column], utc=True)

        # Concatenate with existing df_Sv and save
        df_Sv = pd.concat([df_Sv, df_new], ignore_index=True)
        df_Sv.sort_values(by=["first_ping_time"], inplace=True, ignore_index=True)
        write_manifest(df_Sv, file_Sv_csv)
        print(f"Added {len(new_files)} new entries to tracking CSV")

    # Set flow to Failed state if any errors occurred
    if len(errors) > 0:
        error_msg = (
            f"{len(errors)} errors during raw to Sv conversion out of {len(new_files)} files"
        )

        async def set_failed_state():
            async with get_client() as client:
                await client.set_flow_run_state(
                    flow_run_id=runtime.flow_run.id, state=Failed(message=error_msg)
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
    ).astimezone(
        datetime.timezone.utc
    )  # convert to UTC

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
        logger.info(
            "Sv zarr store does not exist, check raw2Sv flow! Creating empty folder for now."
        )
        path_Sv_zarr.mkdir(parents=True, exist_ok=True)
    if not path_MVBS_zarr.exists():
        path_MVBS_zarr.mkdir(parents=True, exist_ok=True)
    path_Sv_zarr = str(path_Sv_zarr)  # convert back to string to pass into task
    path_MVBS_zarr = str(path_MVBS_zarr)  # convert back to string to pass into task

    # Load Sv and MVBS info dataframes
    if not file_Sv_csv.exists():
        logger.info("Sv info csv does not exist, check raw2Sv flow!")
        return
    df_Sv = read_manifest(
        file_Sv_csv,
        SV_COLUMNS_REALTIME,
        ["first_ping_time", "last_ping_time"],
    )
    if df_Sv.empty:
        logger.info(
            "Sv info csv is empty, raw2Sv flow may have just started! "
            "No MVBS can be created, exiting flow."
        )
        return

    mvbs_manifest_exists = file_MVBS_csv.exists()
    df_MVBS = read_manifest(
        file_MVBS_csv,
        MVBS_COLUMNS_REALTIME,
        ["first_ping_time", "last_ping_time"],
    )
    if not mvbs_manifest_exists:
        write_manifest(df_MVBS, file_MVBS_csv)

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
                (pd.to_datetime(df_Sv["last_ping_time"]) >= start_time[snum])
                & (pd.to_datetime(df_Sv["first_ping_time"]) <= end_time[snum])
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
        if not result.has_data:
            logger.info(f"No ping data found for {result.mvbs_filename}, skipping")
            continue
        if result.mvbs_filename in df_MVBS["MVBS_filename"].values:
            logger.info(
                f"MVBS file {result.mvbs_filename} already exists, "
                "updating first and last ping times"
            )
            idx_to_add = df_MVBS.index[df_MVBS["MVBS_filename"] == result.mvbs_filename]
        else:
            logger.info(f"Adding new MVBS file {result.mvbs_filename} to tracking dataframe")
            idx_to_add = len(df_MVBS)
        df_MVBS.loc[idx_to_add] = [
            result.mvbs_filename,
            result.first_ping_time,
            result.last_ping_time,
        ]

    # Save updated MVBS info dataframe
    write_manifest(df_MVBS, file_MVBS_csv)

    # Set flow to Failed state if any errors occurred
    if len(errors) > 0:
        error_msg = f"{len(errors)} errors during MVBS creation out of {num_slices} slices"
        async with get_client() as client:
            await client.set_flow_run_state(
                flow_run_id=runtime.flow_run.id, state=Failed(message=error_msg)
            )
        raise Exception(error_msg)


@flow(log_prints=True, task_runner=dask_task_runner_from_environment())
def flow_raw2Sv_postprocessing(
    path_raw_list: str,
    path_main: str,
    s3_bucket: str = "noaa-wcsd-pds",
    endpoint_url: str | None = "https://sdsc.osn.xsede.org",
    start_time: str | None = None,
    end_time: str | None = None,
    new_file_num_limit: int = -1,
    task_retries: int = 3,
    task_retry_delay_seconds: int = 30,
    encode_mode: str = "power",
    waveform_mode: str = "CW",
    depth_offset: float = 9.5,
    sonar_model: str = "EK80",
    datagram_type: str | None = None,
    nmea_sentence: str | None = None,
    file_Sv_csv: str = "Sv_files.csv",
) -> None:
    """Convert raw files and update corresponding rows in the Sv ledger."""
    logger = get_run_logger()
    # Keep temporary raw files separate from persistent Sv outputs
    path_raw_staging = Path(path_main) / "raw_staging"
    path_Sv = Path(path_main) / "Sv"
    path_raw_staging.mkdir(parents=True, exist_ok=True)
    path_Sv.mkdir(parents=True, exist_ok=True)
    file_Sv_csv = Path(path_main) / file_Sv_csv

    # Initialize the complete ledger before selecting work for this run
    df_Sv = read_or_create_ledger(
        ledger_path=file_Sv_csv,
        columns=SV_COLUMNS_POSTPROCESSING,
        date_columns=["timestamp", "first_ping_time", "last_ping_time"],
        builder=lambda: build_Sv_ledger(pd.read_csv(path_raw_list)),
    )
    selected = filter_time_range(
        df=df_Sv,
        column_start_time="timestamp",
        column_end_time=None,
        start_time=start_time,
        end_time=end_time,
    )
    selected = selected[selected["raw2Sv_status"] != "completed"]
    if new_file_num_limit != -1:
        selected = selected.head(new_file_num_limit)
    if selected.empty:
        logger.info("No raw files require processing")
        return

    # Mark submitted work before workers begin completing out of order
    df_Sv.loc[selected.index, ["raw2Sv_status", "error"]] = ["pending", ""]
    write_manifest(df_Sv, file_Sv_csv)

    copy_settings = S3CopySettings(s3_bucket=s3_bucket, endpoint_url=endpoint_url)
    sv_settings = RawToSvSettings(
        output_directory=str(path_Sv),
        encode_mode=encode_mode,
        waveform_mode=waveform_mode,
        depth_offset=depth_offset,
        sonar_model=sonar_model,
        datagram_type=datagram_type,
        nmea_sentence=nmea_sentence,
    )

    # Submit one download-plus-conversion task per raw object
    errors = []
    conversion_futures = {}
    for row in selected.itertuples(index=False):
        key = str(row.s3_path)
        filename = Path(key).name
        future = task_s3_raw2Sv.with_options(
            task_run_name=f"raw2Sv_{filename}",
            retries=task_retries,
            retry_delay_seconds=task_retry_delay_seconds,
        ).submit(
            S3CopyWorkItem(s3_path=key, local_path=str(path_raw_staging / filename)),
            copy_settings,
            sv_settings,
        )
        conversion_futures[future] = key

    # Persist each result immediately so polling MVBS runs can observe progress
    for future in as_completed(conversion_futures):
        key = conversion_futures[future]
        idx = df_Sv.index[df_Sv["s3_path"] == key][0]
        try:
            result = future.result()
            df_Sv.loc[
                idx,
                [
                    "raw_filename",
                    "Sv_filename",
                    "raw2Sv_status",
                    "first_ping_time",
                    "last_ping_time",
                    "error",
                ],
            ] = [
                result.filename_raw,
                result.filename_Sv,
                "completed",
                result.first_ping_time,
                result.last_ping_time,
                "",
            ]
            write_manifest(df_Sv, file_Sv_csv)
            logger.info("Completed %s", key)
        except Exception as exc:
            errors.append(exc)
            df_Sv.loc[idx, ["raw2Sv_status", "error"]] = ["failed", str(exc)]
            write_manifest(df_Sv, file_Sv_csv)
            logger.error("Failed to download or convert %s: %s", key, exc)
    if errors:
        raise RuntimeError(f"{len(errors)} raw-to-Sv conversions failed")


@flow(log_prints=True, task_runner=dask_task_runner_from_environment())
def flow_create_MVBS_postprocessing(
    path_main: str,
    slice_mins: int = 20,
    range_bin: str = "1m",
    ping_time_bin: str = "5s",
    file_Sv_csv: str = "Sv_files.csv",
    file_MVBS_csv: str = "MVBS_files.csv",
) -> None:
    """Create preplanned MVBS slices after all required raw conversions finish."""
    logger = get_run_logger()
    file_Sv_csv = Path(path_main) / file_Sv_csv
    if not file_Sv_csv.exists():
        logger.info("Sv ledger does not yet exist")
        return

    path_MVBS = Path(path_main) / "MVBS"
    path_MVBS.mkdir(parents=True, exist_ok=True)
    file_MVBS_csv = Path(path_main) / file_MVBS_csv
    # Raw-to-Sv owns Sv ledger initialization
    df_Sv = read_manifest(
        path=file_Sv_csv,
        columns=SV_COLUMNS_POSTPROCESSING,
        date_columns=["timestamp", "first_ping_time", "last_ping_time"],
    )
    # Preplan every MVBS row once from the complete raw-file timeline
    df_MVBS = read_or_create_ledger(
        ledger_path=file_MVBS_csv,
        columns=MVBS_COLUMNS_POSTPROCESSING,
        date_columns=["slice_start", "slice_end", "first_ping_time", "last_ping_time"],
        builder=lambda: build_MVBS_ledger(df_Sv, slice_mins),
    )

    # Get MVBS slices to be computed based on raw-to-Sv completions
    planned = plan_mvbs_slices(df_Sv, df_MVBS)
    if not planned:
        logger.info("No newly ready MVBS slices")
        return

    settings = CreateMVBSSettings(
        sv_directory=str(Path(path_main) / "Sv"),
        output_directory=str(path_MVBS),
        range_bin=range_bin,
        ping_time_bin=ping_time_bin,
    )
    # Ready slices are independent and can be computed in parallel
    futures = {}
    for item in planned:
        filename = f"MVBS_{item.start_time:%Y%m%dT%H%M%S}.zarr"
        future = task_create_MVBS.with_options(task_run_name=filename).submit(
            CreateMVBSWorkItem(
                start_time=item.start_time,
                end_time=item.end_time,
                sv_filenames=item.filenames,
                mvbs_filename=filename,
            ),
            settings,
        )
        futures[future] = item

    # Collect task results in memory so this flow remains the sole manifest writer
    errors = []
    for future in as_completed(futures):
        item = futures[future]
        try:
            result = future.result()
            idx = df_MVBS.index[df_MVBS["slice_start"] == item.start_time][0]
            if not result.has_data:
                df_MVBS.loc[idx, ["MVBS_status", "error"]] = [
                    "no_data",
                    "No ping data in the planned slice",
                ]
                logger.info("No ping data found for MVBS slice %s", item.start_time)
                continue
            df_MVBS.loc[
                idx,
                [
                    "MVBS_filename",
                    "first_ping_time",
                    "last_ping_time",
                    "is_partial",
                    "MVBS_status",
                    "error",
                ],
            ] = [
                result.mvbs_filename,
                result.first_ping_time,
                result.last_ping_time,
                item.is_partial,
                "completed",
                "",
            ]
        except Exception as exc:
            errors.append(exc)
            filename = f"MVBS_{item.start_time:%Y%m%dT%H%M%S}.zarr"
            idx = df_MVBS.index[df_MVBS["MVBS_filename"] == filename][0]
            df_MVBS.loc[idx, ["MVBS_status", "error"]] = ["failed", str(exc)]
            logger.error("MVBS slice %s failed: %s", item.start_time, exc)
    write_manifest(df_MVBS.sort_values("slice_start"), file_MVBS_csv)
    if errors:
        raise RuntimeError(f"{len(errors)} MVBS slices failed")
