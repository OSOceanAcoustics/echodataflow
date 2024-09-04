import os
import subprocess
import time
from typing import List, Dict, Any, Optional, Union
from prefect import flow, task
from prefect.concurrency.sync import concurrency
import zarr
from prefect.blocks.core import Block
from prefect.client.schemas.filters import (DeploymentFilter,
                                            DeploymentFilterId, FlowRunFilter,
                                            FlowRunFilterState,
                                            FlowRunFilterStateType)
from prefect import flow, get_client, task
from prefect.runtime import deployment
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.states import Cancelled

from echodataflow.utils.config_utils import glob_url, handle_storage_options
from echodataflow.utils.file_utils import extract_fs, make_temp_folder
import shlex


@task
def download_temp_file(file_url: str, storage_options: Dict[str, Any], dest_dir: str, delete_on_transfer: bool, replace: bool) -> str:
    """
    Downloads a file from a URL to a destination directory.

    Args:
        file_url (str): URL of the file to download.
        storage_options (Dict[str, Any]): Dictionary containing storage options for source and destination.
        dest_dir (str): Destination directory where the file will be downloaded.

    Returns:
        str: Local path of the downloaded file.
    """

    fname = os.path.basename(file_url)
    out_path = os.path.join(dest_dir, fname)

    # Ensure destination directory exists
    make_temp_folder(dest_dir, storage_options.get("dest", {}))

    # Extract file systems for source and destination
    file_system_source = extract_fs(file_url, storage_options.get("source", {}))
    file_system_dest = extract_fs(out_path, storage_options.get("dest", {}))

    # Check if file needs to be downloaded
    if not file_system_dest.exists(out_path) or replace:
        with concurrency("edf-data-transfer", occupy=1):
            print(f"Downloading {file_url} to {out_path} ...")
            if file_url.endswith('.zarr'):
                zarr.copy_store(file_system_source.get_mapper(file_url), file_system_dest.get_mapper(out_path), if_exists='replace')
            else:    
                with file_system_source.open(file_url, "rb") as source_file:
                    with file_system_dest.open(out_path, "wb") as dest_file:
                        dest_file.write(source_file.read())
        if delete_on_transfer:
            try:
                file_system_source.rm(file_url, recursive=True)
                print("Cleanup complete")
            except Exception as e:
                print(e)
                print("Failed to cleanup " + file_url)

    return out_path

@task
def sync_with_rclone(command: Optional[str] = None, timeout: Optional[int] = 3600):
    """
    Syncs files using rclone.

    Args:
        source (Union[str, List[str]]): Source directory or list of directories to sync.
        destination (str): Destination directory for the sync.
    """
    if not command:
        raise ValueError("No rclone command provided.")
    else:
        print("Executing Command")
        print(shlex.split(command))
        
    run_subprocess_with_timeout(shlex.split(command), timeout)
    # subprocess.run(command.split(' '), check=True, capture_output=True, text=True)
    print(f"Sync completed.")

def run_subprocess_with_timeout(command, timeout):
    try:
        # Start the subprocess
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Wait for the process to complete within the timeout period
        stdout, stderr = process.communicate(timeout=timeout)
        
        # Process the output if needed
        print("Subprocess completed successfully.")
        print("Output:", stdout.decode())
        print("Errors:", stderr.decode())
    except subprocess.TimeoutExpired:
        # If timeout occurs, terminate the process
        print(f"Subprocess timed out after {timeout} seconds. Terminating...")
        if process and process.pid:
            process.kill()
            stdout, stderr = process.communicate()
            # Optionally handle the partial output
            print("Partial Output:", stdout.decode())
            print("Partial Errors:", stderr.decode())        
            
@flow
def edf_data_transfer(
    source: Union[List[str], str] = "",
    destination: str = "./temp",
    source_storage_options: Dict[str, Any] = {},
    destination_storage_options: Dict[str, Any] = {},
    delete_on_transfer: bool = False,
    replace: bool =True,
    rclone_sync: bool =True,
    command: Optional[str] = None,
    timeout: int = 3600
):
    """
    Downloads multiple files from a list of URLs to a destination directory.

    Args:
        file_urls (List[str]): List of file URLs to download.
        storage_options (Dict[str, Any]): Dictionary containing storage options for source and destination.
        dest_dir (str): Destination directory where the files will be downloaded.
    """
    
    if deployment_already_running():
        return Cancelled()
    
    if rclone_sync:
        sync_with_rclone.submit(command, timeout)
        return
    
    downloaded_files = []
    if not source:
        raise ValueError("No Source Provided.")

    source_storage_options = handle_storage_options(source_storage_options)
    
    destination_storage_options = handle_storage_options(destination_storage_options)

    files = []
    
    if isinstance(source, str):
        files = glob_url(source, source_storage_options)
    else:
        for s in source:
            flist = glob_url(s, source_storage_options)
            files.extend([f for f in flist])
    
    print(source)
    print(files)

    storage_options: Dict[str, Any] = {}
    storage_options["source"] = source_storage_options
    storage_options["dest"] = destination_storage_options

    for file_url in files:
        time.sleep(1)
        local_path = download_temp_file.with_options(task_run_name=os.path.basename(file_url), timeout_seconds=timeout).submit(file_url, storage_options, destination, delete_on_transfer, replace)
        downloaded_files.append(local_path)

    return downloaded_files

@task
async def deployment_already_running() -> bool:
    deployment_id = deployment.get_id()
    async with get_client() as client:
        # find any running flows for this deployment
        running_flows = await client.read_flow_runs(
            deployment_filter=DeploymentFilter(
                id=DeploymentFilterId(any_=[deployment_id])
            ),
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(any_=[StateType.RUNNING])
                ),
            ),
        )
    if len(running_flows) > 1:
        return True
    else:
        return False

if __name__ == "__main__":
    edf_data_transfer.serve(name="edf-data-transfer")
