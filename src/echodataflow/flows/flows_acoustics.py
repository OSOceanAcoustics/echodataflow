from __future__ import annotations

import asyncio
import datetime
from pathlib import Path

import pandas as pd

import echopype as ep

from prefect import flow, get_run_logger, get_client
from prefect.futures import as_completed
from prefect.states import Cancelled, Failed
from prefect import runtime
from prefect.events import emit_event

from echodataflow.flows.flows_helper import deployment_already_running
from echodataflow.deployment.task_runners import dask_task_runner_from_environment
from echodataflow.utils.manifests import (
    MVBS_COLUMNS_POSTPROCESSING,
    MVBS_COLUMNS_REALTIME,
    SV_COLUMNS_POSTPROCESSING,
    filter_time_range,
    read_manifest,
    write_manifest,
)
from echodataflow.operations.operations_acoustics import (
    CreateMVBSResult,
    CreateMVBSSettings,
    CreateMVBSWorkItem,
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

from echodataflow.utils.processing_ledger import (
    get_completed_sv_files,
    get_raw_files_to_process,
    initialize_ledger,
    mark_raw_completed,
    mark_raw_failed,
    mark_raw_processing,
    resolve_database,
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
    path_main: str = "",
    processing_db: str = "processing.db",
    new_file_num_limit: int = 50,
    add_depth: bool = True,
    add_location: bool = True,
    add_splitbeam_angle: bool = False,
):
    # Check if the deployment is already running
    already_running = asyncio.run(deployment_already_running())
    if already_running:

        async def cancel_run():
            async with get_client() as client:
                await client.set_flow_run_state(
                    flow_run_id=runtime.flow_run.id,
                    state=Cancelled(
                        message="Another instance of this flow is already running"
                    ),
                )

        asyncio.run(cancel_run())
        return

    # Assemble paths
    path_Sv_zarr = Path(path_main) / "Sv"
    db_path = resolve_database(path_main, processing_db)

    initialize_ledger(db_path)

    # Set up folder to store converted Sv zarr
    path_Sv_zarr.mkdir(parents=True, exist_ok=True)

    # Get RAW files requiring processing directly from the database
    raw_files = get_raw_files_to_process(
        db_path,
        limit=new_file_num_limit,
    )

    # Exclude files before requested datetime
    if exclude_before is not None:
        exclude_before_dt = datetime.datetime.fromisoformat(exclude_before)
        raw_files = [
            raw_path
            for raw_path in raw_files
            if extract_datetime_from_filename(raw_path.name) >= exclude_before_dt
        ]

    # Skip explicitly excluded RAW files
    if exclude_raw_file:
        excluded = set(exclude_raw_file)
        raw_files = [
            raw_path
            for raw_path in raw_files
            if raw_path.name not in excluded
        ]

    print(f"Found {len(raw_files)} RAW files to process")
    print(
        "Files to process:\n"
        + "".join(f"- {raw_path.name}\n" for raw_path in raw_files)
    )

    if not raw_files:
        return

    settings = RawToSvSettings(
        output_directory=str(path_Sv_zarr),
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

    if parallel:
        print("Processing raw files in parallel")

        futures = {}

        for raw_path in raw_files:
            mark_raw_processing(db_path, raw_path)

            future = task_raw2Sv.with_options(
                task_run_name=raw_path.name,
                name=raw_path.name,
                retries=3,
            ).submit(
                RawToSvWorkItem(raw_path=str(raw_path)),
                settings,
            )

            futures[future] = raw_path

        for future in as_completed(futures):
            raw_path = futures[future]

            try:
                result = future.result()

                mark_raw_completed(
                    db_path,
                    raw_path,
                    result.filename_Sv,
                    result.first_ping_time,
                    result.last_ping_time,
                )

            except Exception as exc:
                mark_raw_failed(
                    db_path,
                    raw_path,
                    str(exc),
                )
                errors.append(exc)
                print(f"Error converting {raw_path.name}: {exc}")

    else:
        print("Processing raw files sequentially")

        for raw_path in raw_files:
            try:
                print(f"Converting {raw_path.name}")

                mark_raw_processing(
                    db_path,
                    raw_path,
                )

                result = task_raw2Sv.with_options(
                    task_run_name=raw_path.name,
                    name=raw_path.name,
                    retries=3,
                )(
                    RawToSvWorkItem(raw_path=str(raw_path)),
                    settings,
                )

                mark_raw_completed(
                    db_path,
                    raw_path,
                    result.filename_Sv,
                    result.first_ping_time,
                    result.last_ping_time,
                )

            except Exception as exc:
                mark_raw_failed(
                    db_path,
                    raw_path,
                    str(exc),
                )
                errors.append(exc)
                print(f"Error converting {raw_path.name}: {exc}")

    # Set flow to Failed state if any conversions failed
    if errors:
        error_msg = (
            f"{len(errors)} errors during raw to Sv conversion "
            f"out of {len(raw_files)} files"
        )

        async def set_failed_state():
            async with get_client() as client:
                await client.set_flow_run_state(
                    flow_run_id=runtime.flow_run.id,
                    state=Failed(message=error_msg),
                )

        asyncio.run(set_failed_state())
        raise RuntimeError(error_msg)

    emit_event(
        event="echodataflow.sv.updated",
        resource={
            "prefect.resource.id": "sv-monitor",
            "prefect.resource.name": "sv-monitor",
        },
    )


@flow(log_prints=True)
async def flow_create_MVBS(
    time_offset_seconds: float = 0.0,
    slice_mins: int = 10,
    num_slices: int = 3,
    range_bin: str = "1m",
    ping_time_bin: str = "5s",
    path_main: str = "",
    processing_db: str = "processing.db",
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
    db_path = resolve_database(path_main, processing_db)
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
        Sv_filenames = get_completed_sv_files(
            db_path,
            start_time=start_time[snum],
            end_time=end_time[snum],
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
