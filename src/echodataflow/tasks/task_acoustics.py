"""Reusable Prefect tasks for acoustic data processing."""

from prefect import get_run_logger, task

from echodataflow.operations.ops_acoustics import (
    CreateMVBSResult,
    CreateMVBSSettings,
    CreateMVBSWorkItem,
    RawToSvResult,
    RawToSvSettings,
    RawToSvWorkItem,
    create_MVBS,
    convert_raw_to_Sv,
)


@task(log_prints=True)
def task_raw2Sv(
    item: RawToSvWorkItem,
    settings: RawToSvSettings,
) -> RawToSvResult:
    """Run one raw-to-Sv operation as a Prefect task."""
    return convert_raw_to_Sv(item, settings)


@task(log_prints=True)
def task_create_MVBS(
    item: CreateMVBSWorkItem,
    settings: CreateMVBSSettings,
) -> CreateMVBSResult:
    """Create one MVBS slice as a Prefect task."""
    logger = get_run_logger()
    logger.info(f"Saving MVBS to {item.mvbs_filename}")
    return create_MVBS(item, settings)
