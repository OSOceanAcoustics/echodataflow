from __future__ import annotations

import asyncio
import datetime
from pathlib import Path

import pandas as pd

import echopype as ep

from prefect import flow, get_run_logger, get_client
from prefect.futures import as_completed
from prefect_dask import DaskTaskRunner
from prefect.states import Cancelled, Failed
from prefect import runtime

from echodataflow.flows.flows_helper import deployment_already_running
from echodataflow.utils.manifests import (
    MVBS_COLUMNS,
    RAW_COLUMNS,
    SV_COLUMNS,
    filter_slices,
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
from echodataflow.operations.operations_postprocessing import plan_mvbs_slices
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


@flow(log_prints=True, task_runner=DaskTaskRunner())
def flow_raw2Sv_postprocessing(
    path_raw_list: str,
    path_main: str,
    s3_bucket: str = "noaa-wcsd-pds",
    endpoint_url: str | None = "https://sdsc.osn.xsede.org",
    start_time: str | None = None,
    end_time: str | None = None,
    new_file_num_limit: int = -1,
    overwrite: bool = False,
    task_retries: int = 3,
    task_retry_delay_seconds: int = 30,
    encode_mode: str = "power",
    waveform_mode: str = "CW",
    depth_offset: float = 9.5,
    sonar_model: str = "EK80",
    datagram_type: str | None = None,
    nmea_sentence: str | None = None,
    file_raw_processing_csv: str = "raw_processing.csv",
    file_Sv_csv: str = "Sv_files.csv",
) -> None:
    """Stage selected S3 raw files and convert them concurrently to Sv."""
    logger = get_run_logger()
    # Keep temporary raw files separate from persistent Sv outputs
    staging = Path(path_main) / "raw_staging"
    sv_directory = Path(path_main) / "Sv"
    staging.mkdir(parents=True, exist_ok=True)
    sv_directory.mkdir(parents=True, exist_ok=True)
    raw_manifest_path = Path(path_main) / file_raw_processing_csv
    sv_manifest_path = Path(path_main) / file_Sv_csv

    # Select the requested portion of the reusable S3 source manifest
    source = pd.read_csv(path_raw_list)
    required = {"s3_path", "timestamp"}
    if not required.issubset(source.columns):
        raise ValueError(f"raw list must contain columns: {sorted(required)}")
    source["timestamp"] = pd.to_datetime(source["timestamp"], utc=True)
    source = filter_time_range(
        source,
        "timestamp",
        start_time,
        end_time,
        include_boundary_neighbors=True,
    )
    if source["s3_path"].map(lambda value: Path(str(value)).name).duplicated().any():
        raise ValueError("selected S3 paths contain duplicate basenames")

    # Resume from durable processing and output manifests
    raw_manifest = read_manifest(raw_manifest_path, RAW_COLUMNS, ["timestamp"])
    sv_manifest = read_manifest(
        sv_manifest_path, SV_COLUMNS, ["first_ping_time", "last_ping_time"]
    )
    completed = set(raw_manifest.loc[raw_manifest["status"] == "completed", "s3_path"])
    selected = source if overwrite else source[~source["s3_path"].isin(completed)]
    if new_file_num_limit != -1:
        selected = selected.head(new_file_num_limit)
    if selected.empty:
        logger.info("No raw files require processing")
        return

    # Register all selected inputs before workers begin completing out of order
    for row in selected.itertuples(index=False):
        key = str(row.s3_path)
        values = {
            "s3_path": key,
            "timestamp": row.timestamp,
            "raw_filename": Path(key).name,
            "status": "pending",
            "error": "",
        }
        matches = raw_manifest.index[raw_manifest["s3_path"] == key]
        if len(matches):
            raw_manifest.loc[matches[0], RAW_COLUMNS] = list(values.values())
        else:
            raw_manifest.loc[len(raw_manifest), RAW_COLUMNS] = list(values.values())
    write_manifest(raw_manifest, raw_manifest_path)

    copy_settings = S3CopySettings(s3_bucket=s3_bucket, endpoint_url=endpoint_url)
    sv_settings = RawToSvSettings(
        output_directory=str(sv_directory),
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
            S3CopyWorkItem(s3_path=key, local_path=str(staging / filename)),
            copy_settings,
            sv_settings,
        )
        conversion_futures[future] = key

    # Persist each result immediately so polling MVBS runs can observe progress
    for future in as_completed(conversion_futures):
        key = conversion_futures[future]
        idx = raw_manifest.index[raw_manifest["s3_path"] == key][0]
        try:
            result = future.result()
            record = [
                key,
                result.raw_filename,
                result.sv_filename,
                result.first_ping_time,
                result.last_ping_time,
            ]
            matches = sv_manifest.index[sv_manifest["s3_path"] == key]
            if len(matches):
                sv_manifest.loc[matches[0], SV_COLUMNS] = record
            else:
                sv_manifest.loc[len(sv_manifest), SV_COLUMNS] = record
            raw_manifest.loc[idx, ["status", "error"]] = ["completed", ""]
            write_manifest(sv_manifest, sv_manifest_path)
            write_manifest(raw_manifest, raw_manifest_path)
            logger.info("Completed %s", key)
        except Exception as exc:
            errors.append(exc)
            raw_manifest.loc[idx, ["status", "error"]] = ["failed", str(exc)]
            write_manifest(raw_manifest, raw_manifest_path)
            logger.error("Failed to download or convert %s: %s", key, exc)
    if errors:
        raise RuntimeError(f"{len(errors)} raw-to-Sv conversions failed")


@flow(log_prints=True, task_runner=DaskTaskRunner())
def flow_create_MVBS_postprocessing(
    path_main: str,
    slice_mins: int = 20,
    start_time: str | None = None,
    end_time: str | None = None,
    overwrite: bool = False,
    range_bin: str = "1m",
    ping_time_bin: str = "5s",
    file_raw_processing_csv: str = "raw_processing.csv",
    file_Sv_csv: str = "Sv_files.csv",
    file_MVBS_csv: str = "MVBS_files.csv",
) -> None:
    """Create every newly ready MVBS slice registered by raw-to-Sv runs."""
    logger = get_run_logger()
    root = Path(path_main)
    mvbs_directory = root / "MVBS"
    mvbs_directory.mkdir(parents=True, exist_ok=True)
    # Load input state and previously completed MVBS outputs
    raw = read_manifest(root / file_raw_processing_csv, RAW_COLUMNS, ["timestamp"])
    sv = read_manifest(root / file_Sv_csv, SV_COLUMNS, ["first_ping_time", "last_ping_time"])
    manifest_path = root / file_MVBS_csv
    manifest = read_manifest(
        manifest_path,
        MVBS_COLUMNS,
        ["slice_start", "slice_end", "first_ping_time", "last_ping_time"],
    )
    # Plan only sealed slices, then discard outputs already present in the manifest
    planned = filter_slices(plan_mvbs_slices(raw, sv, slice_mins), start_time, end_time)
    existing = set(manifest["MVBS_filename"]) if not overwrite else set()
    planned = [
        item
        for item in planned
        if f"MVBS_{item.start_time:%Y%m%dT%H%M%S}.zarr" not in existing
    ]
    if not planned:
        logger.info("No newly ready MVBS slices")
        return

    settings = CreateMVBSSettings(
        sv_directory=str(root / "Sv"),
        output_directory=str(mvbs_directory),
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
            record = [
                result.mvbs_filename,
                item.start_time,
                item.end_time,
                result.first_ping_time,
                result.last_ping_time,
                item.is_partial,
            ]
            matches = manifest.index[manifest["MVBS_filename"] == result.mvbs_filename]
            if len(matches):
                manifest.loc[matches[0], MVBS_COLUMNS] = record
            else:
                manifest.loc[len(manifest), MVBS_COLUMNS] = record
        except Exception as exc:
            errors.append(exc)
            logger.error("MVBS slice %s failed: %s", item.start_time, exc)
    write_manifest(manifest.sort_values("slice_start"), manifest_path)
    if errors:
        raise RuntimeError(f"{len(errors)} MVBS slices failed")
