from pathlib import Path
import datetime
import asyncio

import pandas as pd

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from prefect import flow, task, get_client, runtime
from prefect import runtime
from prefect.client.schemas.filters import FlowRunFilter
from prefect.states import Cancelled
from prefect.variables import Variable


@flow(timeout_seconds=600, log_prints=True)
def flow_file_upload(
    src_dir: str,
    dest_dir: str,
    exclude_subdirs: list[str],
    max_age: int = -1,
):
    """
    Upload files via rclone.

    Parameters
    ----------
    src_dir : str
        Source directory to upload files from.
    dest_dir : str, optional
        Destination directory to upload files to, by default "osn_sdsc_hake:/agr230002-bucket01/prefect_test".
    exclude_subdirs : list, optional
        List of subdirectories to exclude from the upload, by default [].
    max_age : int, optional
        Maximum age of files to upload in hours, by default -1 (no limit).
    """ 
    # TODO: need to fix dependency issue
    # TODO: consider moving it back to top imports
    from prefect_shell import ShellOperation

    # Generate upload_exclude_folders.txt
    exclude_filename = (
        f"upload_exclude_folders_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_%H%M%S')}.txt"
    )
    exclude_path = Path(src_dir) / exclude_filename
    with open(exclude_path, "w") as f:
        # Add .DS_Store to exclude list
        f.write(".DS_Store\n")
        # Exclude all upload_exclude_folders_*.txt files (i.e. this file and any leftover ones)
        f.write("upload_exclude_folders_*.txt\n")
        # Add other subdirectories
        for subdir in exclude_subdirs:
            f.write(f"/{subdir}/**\n")

    # Potentially long running so using a context manager
    if max_age == -1:
        command = f"rclone copy -v --no-traverse {src_dir} {dest_dir} --exclude-from {str(exclude_path)}" 
    else:
        command = f"rclone copy -v --max-age {max_age}h --no-traverse {src_dir} {dest_dir} --exclude-from {str(exclude_path)}" 
    with ShellOperation(commands=[command], working_dir=src_dir) as file_upload_operation:

        # Trigger runs the process in the background
        file_upload_process = file_upload_operation.trigger()

        # Wait for the process to finish
        file_upload_process.wait_for_completion()

        # Print results
        file_upload_process.fetch_result()

    # Remove the exclude list file after upload
    exclude_path.unlink(missing_ok=True)


@task(log_prints=True)
async def deployment_already_running() -> bool:
    if runtime.deployment.id is None:
        # Not running as a deployment, so skip the check
        return False

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


def _var_key() -> str:
    deployment_id = runtime.deployment.id or "no_deployment"
    return f"prev_start_time_{deployment_id}"


@flow(log_prints=True)
def flow_copy_raw(
    time_offset_seconds: float = 0.0,
    path_raw_list: str = "",
    path_copy: str = "",
    s3_bucket = "noaa-wcsd-pds",
    exclude_before: str|None = None,
):
    print("Copy raw files to simulate data generation")
    print(f"Executed at {datetime.datetime.now(datetime.UTC)}")

    # Get filename from dataframe
    df_raw = pd.read_csv(
        path_raw_list,
        date_format="ISO8601",
        parse_dates=["timestamp"],
    )
    if df_raw["timestamp"].dt.tz is None:
        df_raw["timestamp"] = df_raw["timestamp"].dt.tz_localize("UTC")

    # Set flow execution time to current time - time_offset_seconds
    flow_time_curr = (
        datetime.datetime.now() - datetime.timedelta(seconds=time_offset_seconds)
    ).astimezone(datetime.timezone.utc)  # convert to UTC
    print(f"Simulated flow run start time: {flow_time_curr}")

    # Get previous flow execution time from Prefect variable, if it exists
    flow_time_prev = Variable.get(_var_key(), default=None)
    flow_time_prev = pd.to_datetime(flow_time_prev, utc=True) if flow_time_prev else None
    print(f"Previous run start time: {flow_time_prev}")

    # Find the last files that would have been generated
    # between the previous and current flow execution times
    idx_wanted = df_raw["timestamp"] < flow_time_curr
    if exclude_before is not None:
        exclude_before_datetime = pd.to_datetime(exclude_before, utc=True)
        idx_wanted &= df_raw["timestamp"] > exclude_before_datetime
    if flow_time_prev is not None:
        idx_wanted &= df_raw["timestamp"] > flow_time_prev
    df_raw = df_raw[idx_wanted]

    if df_raw.empty:
        print("No new files generated since the last flow execution. Skipping file copy.")
        return

    # Configure anonymous access
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    # Download all files in the filtered dataframe
    for index, row in df_raw.iterrows():
        filename = Path(row["s3_path"]).name  # raw filename
        print(f"Copying {filename} to {path_copy}")
        s3.download_file(s3_bucket, row["s3_path"], Path(path_copy) / f"{filename}")

    # Store the current flow execution time in a Prefect variable for future reference
    Variable.set(_var_key(), flow_time_curr.isoformat(), overwrite=True)

