"""Shared deployment helpers for rewrite cloud and ship entrypoints."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, cast
import importlib.util

from prefect.deployments.runner import RunnerDeployment
from prefect.events import DeploymentEventTrigger
from prefect.flows import Flow
from prefect.variables import Variable
from yaml import safe_load


@dataclass(frozen=True)
class DeploymentSpec:
    flow_key: str
    deployment_name: str
    entrypoint: str
    flow_module: ModuleType | None = None
    flow_alias: str | None = None
    cron_offset: int = 0
    apply_separately: bool = False
    work_pool_name: str | None = None
    triggers: list[dict[str, Any]] | None = None


def _discover_flow_map(flow_module: ModuleType) -> dict[str, Flow[..., Any]]:
    discovered: dict[str, Flow[..., Any]] = {}

    for attr_name in dir(flow_module):
        if not attr_name.startswith("flow_"):
            continue

        flow_name = attr_name.removeprefix("flow_")
        discovered[flow_name] = cast(Flow[..., Any], getattr(flow_module, attr_name))

    return discovered


def resolve_flow(spec: DeploymentSpec) -> Flow[..., Any]:
    if spec.flow_module is None:
        raise ValueError(
            f"Deployment spec '{spec.deployment_name}' must provide flow_module"
        )

    flow_name = spec.flow_alias or spec.flow_key
    discovered = _discover_flow_map(spec.flow_module)  # find available flows in flow_module

    if flow_name not in discovered:
        available = ", ".join(sorted(discovered)) or "<none>"
        raise AttributeError(
            f"Flow library '{spec.flow_module.__name__}' has no discovered flow for "
            f"'{flow_name}'. Available flows: {available}"
        )

    return discovered[flow_name]


def load_config(config_path: Path) -> dict[str, Any]:
    with open(config_path, "r") as file:
        return safe_load(file)


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


def _validate_local_source_layout(local_source_root: Path, deploy_cfg: dict[str, Any]) -> Path:
    """
    Validate the local source root and entrypoint exists 
    and return the root to use for local deployments.
    """
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


def resolve_deployment_source(
    deploy_cfg: dict[str, Any],
    source_mode_override: str | None = None,
    log_context: str | None = None,
) -> Any:
    """
    Resolve deployment source based on deploy config and environment variable override.
    """
    source_cfg = deploy_cfg.get("source", {})
    if source_cfg is None:
        source_cfg = {}

    # Priority: 1) env var override, 2) deploy config setting, 3) default to local
    mode = (source_mode_override or source_cfg.get("mode") or "local").lower()

    # Capture the origin of source mode
    if source_mode_override:
        source_mode_origin = "env:PREFECT_SOURCE_MODE"
    elif source_cfg.get("mode"):
        source_mode_origin = "deploy_cfg.source.mode"
    else:
        source_mode_origin = "default:local"

    if mode == "local":
        default_local_dir = _validate_local_source_layout(_default_local_source_root(), deploy_cfg)
        source = str(default_local_dir)
        if log_context:
            print(
                f"[{log_context}] source_mode={mode} "
                f"(origin={source_mode_origin}) target={source}"
            )
        return source

    if mode == "git":
        git_cfg = source_cfg.get("git", {})
        if not isinstance(git_cfg, dict):
            raise ValueError("Deploy source.git must be a mapping")
        url = git_cfg.get("url")
        if not url:
            raise ValueError("Deploy source.git.url is required when source mode is 'git'")

        # Import lazily so tests and local-only runs do not require Git storage objects
        from prefect.runner.storage import GitRepository

        branch = git_cfg.get("branch", "main")  # default to the "main" branch unless specified
        source = GitRepository(url=url, branch=branch)
        if log_context:
            print(
                f"[{log_context}] source_mode={mode} "
                f"(origin={source_mode_origin}) target={url}@{branch}"
            )
        return source

    raise ValueError(f"Unsupported deploy source mode: {mode}")


def get_time_offset_targets(deploy_cfg: dict[str, Any]) -> tuple[str, ...]:
    """Return flow names that should receive time_offset_seconds injection."""
    targets: list[str] = []
    for flow_name, deploy_meta in deploy_cfg.get("flows", {}).items():
        if not isinstance(deploy_meta, dict):
            continue
        if deploy_meta.get("inject_time_offset"):
            targets.append(flow_name)
    return tuple(targets)


def _compute_time_offset_seconds(flow_start_time: str | None) -> float:
    if flow_start_time is None:
        return 0.0

    curr_time_offset = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.datetime.fromisoformat(flow_start_time).astimezone(datetime.timezone.utc)
    )
    return curr_time_offset.total_seconds()


def set_prefect_variables(
    deploy_cfg: dict[str, Any],
    param_cfg: dict[str, Any],
) -> None:
    """Set Prefect Variables from deploy and param specifications."""
    Variable.set("flow_start_time", deploy_cfg.get("flow_start_time"), overwrite=True)
    
    init_dict = param_cfg.get("init", {})
    Variable.set("counter_raw_copy", init_dict.get("counter_raw_copy"), overwrite=True)


def build_cron(interval: int | None, cron_offset: int = 0) -> str | None:
    if interval is None:
        return None
    if cron_offset > 0:
        return f"{cron_offset}-59/{interval} * * * *"
    return f"*/{interval} * * * *"


# TODO: decide if want to keep this
def sanitize_parameters(flow_cfg: dict[str, Any]) -> dict[str, Any]:
    return dict(flow_cfg)


def build_triggers(trigger_items: list[dict[str, Any]]) -> list[Any]:
    return [
        DeploymentEventTrigger(
            expect={item["expect"]},
            match_related={
                "prefect.resource.name": item["resource_name"],
            },
        )
        for item in trigger_items
    ]


def validate_flow_coverage(
    param_cfg: dict[str, Any],
    deploy_cfg: dict[str, Any],
) -> None:
    """Raise ValueError if param/deploy flows do not correspond with each other."""
    flows_cfg = param_cfg.get("flows")
    if not isinstance(flows_cfg, dict):
        raise ValueError("Param config file must contain a top-level 'flows' mapping")

    deploy_flows = deploy_cfg.get("flows")
    if not isinstance(deploy_flows, dict):
        raise ValueError("Deploy config must contain a top-level 'flows' mapping")

    config_flows = set(flows_cfg.keys())
    deploy_flow_keys = set(deploy_flows.keys())
    missing_from_deploy = config_flows - deploy_flow_keys
    missing_from_config = deploy_flow_keys - config_flows
    errors: list[str] = []
    if missing_from_deploy:
        errors.append(
            f"In config but missing from deploy: {sorted(missing_from_deploy)}"
        )
    if missing_from_config:
        errors.append(
            f"In deploy but missing from config: {sorted(missing_from_config)}"
        )
    if errors:
        raise ValueError("Flow coverage mismatch. " + " | ".join(errors))


def build_specs_from_deploy_spec(
    *,
    deploy_cfg: dict[str, Any],
    module_registry: dict[str, Any],
) -> list[DeploymentSpec]:
    specs: list[DeploymentSpec] = []
    entrypoint_root = str(deploy_cfg.get("entrypoint_root", "")).strip("/")
    if entrypoint_root and "." in entrypoint_root and "/" not in entrypoint_root:
        raise ValueError(
            "entrypoint_root must use slash-style package paths, "
            "for example 'echodataflow/flows'"
        )
    entrypoint_file_root = entrypoint_root

    for flow_key, deploy_meta in deploy_cfg.get("flows", {}).items():
        if not isinstance(deploy_meta, dict):
            continue

        module_name = deploy_meta["module"]
        flow_name = deploy_meta.get("flow_alias") or flow_key
        default_entrypoint_file = (
            f"{entrypoint_file_root}/{module_name}.py"
            if entrypoint_file_root
            else f"{module_name}.py"
        )
        entrypoint = deploy_meta.get(
            "entrypoint",
            f"{default_entrypoint_file}:flow_{flow_name}",
        )
        if module_name not in module_registry:
            available = ", ".join(sorted(module_registry)) or "<none>"
            raise KeyError(
                f"Unknown module '{module_name}' for flow '{flow_key}'. "
                f"Available modules: {available}"
            )

        specs.append(
            DeploymentSpec(
                flow_key=flow_key,
                deployment_name=deploy_meta.get("deployment_name", flow_key),
                entrypoint=entrypoint,
                flow_module=module_registry[module_name],
                flow_alias=deploy_meta.get("flow_alias"),
                cron_offset=deploy_meta.get("cron_offset", 0),
                apply_separately=deploy_meta.get("apply_separately", False),
                work_pool_name=deploy_meta.get("work_pool_name"),
                triggers=deploy_meta.get("triggers"),
            )
        )

    return specs


def create_deployments(
    *,
    specs: list[DeploymentSpec],
    param_cfg: dict[str, Any],
    deploy_cfg: dict[str, Any],
    source: Any,
) -> tuple[list[RunnerDeployment], list[RunnerDeployment]]:
    flows_params = param_cfg["flows"]
    flows_deploy_settings = deploy_cfg["flows"]
    time_offset_targets = get_time_offset_targets(deploy_cfg)
    time_offset_seconds = _compute_time_offset_seconds(deploy_cfg.get("flow_start_time"))

    grouped: list[RunnerDeployment] = []
    standalone: list[RunnerDeployment] = []

    for spec in specs:
        flow_obj = resolve_flow(spec)
        deployment_kwargs: dict[str, Any] = {
            "name": spec.deployment_name,
            "parameters": sanitize_parameters(flows_params[spec.flow_key]),
        }

        # Inject time_offset_seconds if this flow is marked for it
        if spec.flow_key in time_offset_targets:
            deployment_kwargs["parameters"]["time_offset_seconds"] = time_offset_seconds

        if spec.triggers is not None:
            deployment_kwargs["triggers"] = build_triggers(spec.triggers)
        else:
            interval = flows_deploy_settings[spec.flow_key].get("interval")
            cron = build_cron(interval, spec.cron_offset)
            if cron is not None:
                deployment_kwargs["cron"] = cron

        if spec.work_pool_name is not None:
            deployment_kwargs["work_pool_name"] = spec.work_pool_name

        deployment = (
            flow_obj.from_source(
                source=source,
                entrypoint=spec.entrypoint,
            ).to_deployment(**deployment_kwargs)
        )

        if spec.apply_separately or spec.work_pool_name is not None:
            standalone.append(deployment)
        else:
            grouped.append(deployment)

    return grouped, standalone
