"""
Deploy the ship data processing flows using Prefect.
"""

import asyncio
import os
from pathlib import Path

from prefect import deploy

from echodataflow.rewrite import flows_acoustics as flows_acoustics_lib
from echodataflow.rewrite import helpers as helpers_lib  # , flow_copy_raw
from echodataflow.rewrite.deployment_engine import (
    build_specs_from_deploy_spec,
    create_deployments,
    get_work_pool_name,
    load_config,
    load_deploy_spec,
    resolve_deployment_source,
    set_prefect_variables,
    validate_flow_coverage,
)



def main() -> None:
    source_dir = Path(__file__).parent
    repo_root = source_dir.parent.parent
    param_cfg = load_config(source_dir / "config_ship.yaml")
    deploy_cfg = load_deploy_spec(source_dir / "deploy_ship.yaml")
    validate_flow_coverage(param_cfg, deploy_cfg)
    set_prefect_variables(deploy_cfg, param_cfg)
    set_concurrency_limit = getattr(flows_acoustics_lib, "set_concurrency_limit", None)
    if callable(set_concurrency_limit):
        asyncio.run(set_concurrency_limit())
    work_pool_name = get_work_pool_name(deploy_cfg)
    source_mode_override = os.getenv("PREFECT_SOURCE_MODE")
    source = resolve_deployment_source(
        deploy_cfg=deploy_cfg,
        default_local_dir=repo_root,
        source_mode_override=source_mode_override,
        log_context="deploy_ship",
    )

    specs = build_specs_from_deploy_spec(
        deploy_cfg=deploy_cfg,
        module_registry={
            "flows_acoustics": flows_acoustics_lib,
            "helpers": helpers_lib,
        },
    )

    grouped, standalone = create_deployments(
        specs=specs,
        param_cfg=param_cfg,
        deploy_cfg=deploy_cfg,
        source=source,
        work_pool_name=work_pool_name,
    )

    deploy(*grouped, work_pool_name=work_pool_name)
    for deployment in standalone:
        deployment.apply()


if __name__ == "__main__":
    main()
