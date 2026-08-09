"""Reusable Prefect tasks for acoustic data processing."""

from prefect import task

from echodataflow.operations.ops_acoustics import (
    RawToSvResult,
    RawToSvSettings,
    RawToSvWorkItem,
    convert_raw_to_Sv,
)


@task(log_prints=True)
def task_raw2Sv(
    item: RawToSvWorkItem,
    settings: RawToSvSettings,
) -> RawToSvResult:
    """Run one raw-to-Sv operation as a Prefect task."""
    return convert_raw_to_Sv(item, settings)
