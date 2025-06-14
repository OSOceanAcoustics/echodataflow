from pathlib import Path
import datetime

from prefect import flow, task, get_client
from prefect_shell import ShellOperation
from prefect import runtime
from prefect.client.schemas.filters import FlowRunFilter
from prefect.states import Cancelled


@flow(timeout_seconds=600, log_prints=True)
def flow_file_upload(
    src_dir: str,
    dest_dir: str,
    exclude_subdirs: list[str],
):
    """
    Upload files via rlcone.

    Parameters
    ----------
    src_dir : str
        Source directory to upload files from.
    dest_dir : str, optional
        Destination directory to upload files to, by default "osn_sdsc_hake:/agr230002-bucket01/prefect_test".
    exclude_subdirs : list, optional
        List of subdirectories to exclude from the upload, by default [].
    """ 
    # Generate upload_exclude_folders.txt
    exclude_filename = f"upload_exclude_folders_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_%H%M%S')}.txt"
    with open(exclude_filename, "w") as f:
        # Add .DS_Store to exclude list
        f.write(".DS_Store\n")
        # Add other subdirectories
        for subdir in exclude_subdirs:
            f.write(f"/{subdir}/**\n")

    # Potentially long running so using a context manager
    command = f"rclone sync -v {src_dir} {dest_dir} --exclude-from {str(Path(__file__).parent / exclude_filename)}" 
    print("command:", command)
    with ShellOperation(commands=[command], working_dir=src_dir) as file_upload_operation:

        # Trigger runs the process in the background
        file_upload_process = file_upload_operation.trigger()

        # Wait for the process to finish
        file_upload_process.wait_for_completion()

        # Print results
        file_upload_process.fetch_result()

    # Remove the exclude list file after upload
    Path(exclude_filename).unlink(missing_ok=True)


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
