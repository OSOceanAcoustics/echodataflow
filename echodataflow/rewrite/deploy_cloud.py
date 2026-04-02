"""
Deploy the cloud data processing flows using Prefect.
"""
from pathlib import Path

from prefect import deploy

from echodataflow.rewrite import flows_biology as flows_biology_lib
from echodataflow.rewrite import flows_integration as flows_integration_lib
from echodataflow.rewrite import flows_viz_cloud as flows_viz_cloud_lib
from echodataflow.rewrite.deployment_engine import (
    build_specs_from_deploy_spec,
    create_deployments,
    get_time_offset_targets,
    get_work_pool_name,
    load_config,
    load_deploy_spec,
    prepare_config,
    validate_flow_coverage,
)


def main() -> None:
    source_dir = Path(__file__).parent
    config = load_config(source_dir / "config_cloud.yaml")
    deploy_cfg = load_deploy_spec(source_dir / "deploy_cloud.yaml")
    validate_flow_coverage(config, deploy_cfg)
    work_pool_name = get_work_pool_name(deploy_cfg)
    interval_dict = prepare_config(config, time_offset_targets=get_time_offset_targets(deploy_cfg))

    specs = build_specs_from_deploy_spec(
        deploy_cfg=deploy_cfg,
        module_registry={
            "flows_biology": flows_biology_lib,
            "flows_integration": flows_integration_lib,
            "flows_viz_cloud": flows_viz_cloud_lib,
        },
    )

    grouped, standalone = create_deployments(
        specs=specs,
        config=config,
        interval_dict=interval_dict,
        source_dir=source_dir,
        work_pool_name=work_pool_name,
    )

    deploy(*grouped, work_pool_name=work_pool_name)
    for deployment in standalone:
        deployment.apply()


if __name__ == "__main__":
    main()
