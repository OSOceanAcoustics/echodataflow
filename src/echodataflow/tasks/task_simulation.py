"""Reusable Prefect tasks for simulated data arrival."""

from prefect import task

from echodataflow.operations.operations_simulation import (
    S3CopyResult,
    S3CopySettings,
    S3CopyWorkItem,
    copy_s3_file,
)


@task(log_prints=True)
def task_copy_s3_file(
    item: S3CopyWorkItem,
    settings: S3CopySettings,
) -> S3CopyResult:
    """Copy one remote S3 object as a Prefect task."""
    return copy_s3_file(item, settings)
