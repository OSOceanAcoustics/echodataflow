"""
Deploy the ship data processing flows using Prefect.
"""

import asyncio
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
    set_prefect_variables,
    validate_flow_coverage,
)



def main() -> None:
    source_dir = Path(__file__).parent
    param_cfg = load_config(source_dir / "config_ship.yaml")
    deploy_cfg = load_deploy_spec(source_dir / "deploy_ship.yaml")
    validate_flow_coverage(param_cfg, deploy_cfg)
    set_prefect_variables(deploy_cfg, param_cfg)
    set_concurrency_limit = getattr(flows_acoustics_lib, "set_concurrency_limit", None)
    if callable(set_concurrency_limit):
        asyncio.run(set_concurrency_limit())
    work_pool_name = get_work_pool_name(deploy_cfg)

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
        source_dir=source_dir,
        work_pool_name=work_pool_name,
    )

    deploy(*grouped, work_pool_name=work_pool_name)
    for deployment in standalone:
        deployment.apply()


if __name__ == "__main__":
    main()
