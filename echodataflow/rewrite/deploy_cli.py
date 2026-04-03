"""CLI entrypoint for rewrite deployments from paired YAML specifications."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import inspect
import os
from pathlib import Path
from typing import Any

from prefect import deploy

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


def _import_module(module_name: str, module_prefix: str | None) -> Any:
    if module_prefix:
        try:
            return importlib.import_module(f"{module_prefix}.{module_name}")
        except ModuleNotFoundError:
            pass
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


def _default_local_source_root() -> Path:
    """Infer local source root from installed echodataflow package location."""
    spec = importlib.util.find_spec("echodataflow")
    if spec is None:
        raise ValueError("Could not locate installed 'echodataflow' package")

    # Package installs point to .../<root>/echodataflow/__init__.py
    # Local source root should be <root>
    if spec.origin:
        return Path(spec.origin).resolve().parent.parent

    # Namespace package fallback
    if spec.submodule_search_locations:
        first = next(iter(spec.submodule_search_locations), None)
        if first:
            return Path(first).resolve().parent

    raise ValueError("Could not infer local source root from 'echodataflow' package")


def _effective_source_mode(deploy_cfg: dict[str, Any], source_mode_override: str | None) -> str:
    if source_mode_override:
        return source_mode_override

    source_cfg = deploy_cfg.get("source", {})
    if isinstance(source_cfg, dict):
        mode = source_cfg.get("mode")
        if isinstance(mode, str) and mode:
            return mode

    return "local"


def _validate_local_source_layout(local_source_root: Path, deploy_cfg: dict[str, Any]) -> Path:
    root = local_source_root.resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Local source root does not exist or is not a directory: {root}")

    entrypoint_root = deploy_cfg.get("entrypoint_root")
    if isinstance(entrypoint_root, str) and entrypoint_root:
        candidate = root / entrypoint_root
        if not candidate.exists() or not candidate.is_dir():
            raise ValueError(
                "Configured entrypoint_root was not found under local source root: "
                f"entrypoint_root={entrypoint_root!r}, local_source_root={root}"
            )

    return root


def _run_from_specs(
    *,
    param_cfg_path: Path,
    deploy_cfg_path: Path,
    module_prefix: str | None,
    source_mode: str | None,
    run_concurrency_setup: bool,
    local_source_root: Path | None,
) -> None:
    param_cfg = load_config(param_cfg_path)
    deploy_cfg = load_deploy_spec(deploy_cfg_path)
    validate_flow_coverage(param_cfg, deploy_cfg)
    set_prefect_variables(deploy_cfg, param_cfg)

    module_registry = _build_module_registry(deploy_cfg, module_prefix)
    if run_concurrency_setup:
        _maybe_run_concurrency_setup(module_registry)

    default_local_dir = local_source_root or _default_local_source_root()
    if _effective_source_mode(deploy_cfg, source_mode) == "local":
        default_local_dir = _validate_local_source_layout(default_local_dir, deploy_cfg)

    source = resolve_deployment_source(
        deploy_cfg=deploy_cfg,
        default_local_dir=default_local_dir,
        source_mode_override=source_mode,
        log_context="deploy_cli",
    )
    work_pool_name = get_work_pool_name(deploy_cfg)

    specs = build_specs_from_deploy_spec(
        deploy_cfg=deploy_cfg,
        module_registry=module_registry,
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
        "--local-source-root",
        type=Path,
        default=None,
        help=(
            "Base directory to use when source mode resolves to local. "
            "If omitted, inferred from installed echodataflow package location."
        ),
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

    module_prefix = "echodataflow.rewrite"

    if args.target == "run":
        _run_from_specs(
            param_cfg_path=args.param_config,
            deploy_cfg_path=args.deploy_spec,
            module_prefix=module_prefix,
            source_mode=source_mode,
            run_concurrency_setup=args.use_concurrency,
            local_source_root=args.local_source_root,
        )
        return

    raise ValueError(f"Unsupported target: {args.target}")


if __name__ == "__main__":
    main()
