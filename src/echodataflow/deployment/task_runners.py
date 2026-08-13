"""Runtime task-runner configuration supplied by deployment job variables."""

from __future__ import annotations

import json
import os

from prefect_dask import DaskTaskRunner

TASK_RUNNER_ENV_VAR = "ECHODATAFLOW_TASK_RUNNER"


def dask_task_runner_from_environment() -> DaskTaskRunner:
    """Build the flow's Dask runner when its entrypoint is loaded by a worker."""
    serialized = os.getenv(TASK_RUNNER_ENV_VAR)
    if serialized is None:
        return DaskTaskRunner()

    config = json.loads(serialized)
    if config.get("type") != "dask":
        raise ValueError(f"{TASK_RUNNER_ENV_VAR}.type must be 'dask'")
    return DaskTaskRunner(cluster_kwargs=dict(config.get("cluster_kwargs", {})))
