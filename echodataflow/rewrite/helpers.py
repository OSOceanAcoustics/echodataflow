from prefect import flow, task, get_client
from prefect_shell import ShellOperation
from prefect import runtime
from prefect.client.schemas.filters import FlowRunFilter


@flow(timeout_seconds=600, log_prints=True)
def flow_file_upload(
    src_dir: str,
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

        # Print results
        output_lines = file_upload_process.fetch_result()
        print(output_lines)


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
