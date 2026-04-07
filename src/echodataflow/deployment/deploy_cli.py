"""CLI entrypoint for rewrite deployments from paired YAML specifications."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import os
from pathlib import Path
from typing import Any

from prefect import deploy
from yaml import safe_load

from echodataflow.deployment.deployment_engine import (
    build_specs_from_deploy_spec,
    create_deployments,
    load_config,
    resolve_deployment_source,
    set_prefect_variables,
    validate_flow_coverage,
)


def _import_module(module_name: str, module_prefix: str | None) -> Any:
    if module_prefix:
        prefixed_name = f"{module_prefix}.{module_name}"
        try:
            return importlib.import_module(prefixed_name)
        except ModuleNotFoundError as exc:
            # Only fall back to bare import when the prefixed module itself is missing.
            # If a dependency imported within that module is missing, surface the real error.
            if exc.name != prefixed_name:
                raise
    return importlib.import_module(module_name)


def _build_module_registry(deploy_cfg: dict[str, Any], module_prefix: str | None) -> dict[str, Any]:
    flows = deploy_cfg.get("flows", {})
    if not isinstance(flows, dict):
        raise ValueError("Deploy config must contain a top-level 'flows' mapping")

    registry: dict[str, Any] = {}
    for flow_meta in flows.values():
        if not isinstance(flow_meta, dict):
            continue
        module_name = flow_meta.get("module")
        if not isinstance(module_name, str):
            continue
        if module_name in registry:
            continue
        registry[module_name] = _import_module(module_name, module_prefix)
    return registry


def _maybe_run_concurrency_setup(module_registry: dict[str, Any]) -> None:
    for module in module_registry.values():
        setup_fn = getattr(module, "set_concurrency_limit", None)
        if not callable(setup_fn):
            continue

        if inspect.iscoroutinefunction(setup_fn):
            asyncio.run(setup_fn())
        else:
            setup_fn()


def _run_from_specs(
    *,
    param_cfg_path: Path,
    deploy_cfg_path: Path,
    module_prefix: str | None,
    source_mode: str | None,
    run_concurrency_setup: bool,
    default_work_pool_name: str = "local",
) -> None:
    # Load configs
    param_cfg = load_config(param_cfg_path)
    deploy_cfg = load_config(deploy_cfg_path)

    # Validate the pair of configs contain the same flows
    validate_flow_coverage(param_cfg, deploy_cfg)

    # Set prefect variables
    set_prefect_variables(deploy_cfg, param_cfg)

    module_registry = _build_module_registry(deploy_cfg, module_prefix)
    if run_concurrency_setup:
        _maybe_run_concurrency_setup(module_registry)

    source = resolve_deployment_source(
        deploy_cfg=deploy_cfg,
        source_mode_override=source_mode,
        log_context="deploy_cli",
    )

    # Use deploy config default work pool name if specified,
    # unless specified for individual flow
    default_work_pool_name = deploy_cfg.get("default_work_pool_name", default_work_pool_name)

    specs = build_specs_from_deploy_spec(
        deploy_cfg=deploy_cfg,
        module_registry=module_registry,
    )
    grouped, standalone = create_deployments(
        specs=specs,
        param_cfg=param_cfg,
        deploy_cfg=deploy_cfg,
        source=source,
    )

    deploy(*grouped, work_pool_name=default_work_pool_name)
    for deployment in standalone:
        deployment.apply()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="deploy_cli",
        description="Create deployments from paired param/deploy YAML specifications.",
    )
    subparsers = parser.add_subparsers(dest="target", required=True)

    run_parser = subparsers.add_parser(
        "run",
        help="Run deployments from explicit YAML file paths.",
    )
    run_parser.add_argument(
        "--source-mode",
        choices=("local", "git"),
        default=None,
        help=(
            "Temporarily override source selection for this run. "
            "Maps to PREFECT_SOURCE_MODE."
        ),
    )
    run_parser.add_argument(
        "--default-work-pool-name",
        required=True,
        default="local",
        help=(
            "Default work pool name for deployments."
        ),
    )
    run_parser.add_argument(
        "--param-config",
        required=True,
        type=Path,
        help="Path to config_*.yaml (parameter config).",
    )
    run_parser.add_argument(
        "--deploy-spec",
        required=True,
        type=Path,
        help="Path to deploy_*.yaml (deployment spec).",
    )
    run_parser.add_argument(
        "--use-concurrency",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run concurrency-limit setup before creating deployments (default: enabled).",
    )

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    source_mode = args.source_mode
    if source_mode is not None:
        os.environ["PREFECT_SOURCE_MODE"] = source_mode

    module_prefix = "echodataflow.flows"

    if args.target == "run":
        _run_from_specs(
            param_cfg_path=args.param_config,
            deploy_cfg_path=args.deploy_spec,
            module_prefix=module_prefix,
            source_mode=source_mode,
            run_concurrency_setup=args.use_concurrency,
            default_work_pool_name=args.default_work_pool_name,
        )
        return

    raise ValueError(f"Unsupported target: {args.target}")


if __name__ == "__main__":
    main()
