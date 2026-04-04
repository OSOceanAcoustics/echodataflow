"""Shared deployment helpers for rewrite cloud and ship entrypoints."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from prefect.events import DeploymentEventTrigger
from prefect.variables import Variable
from yaml import safe_load


TriggerBuilder = Callable[[dict[str, Any], dict[str, int | None]], list[Any]]


@dataclass(frozen=True)
class DeploymentSpec:
    flow_key: str
    deployment_name: str
    entrypoint: str
    flow_obj: Any | None = None
    flow_module: Any | None = None
    flow_alias: str | None = None
    cron_offset: int = 0
    apply_separately: bool = False
    include_work_pool_in_deployment: bool = False
    trigger_builder: TriggerBuilder | None = None


def _discover_flow_map(flow_module: Any) -> dict[str, Any]:
    discovered: dict[str, Any] = {}

    for attr_name in dir(flow_module):
        if not attr_name.startswith("flow_"):
            continue

        flow_name = attr_name.removeprefix("flow_")
        discovered[flow_name] = getattr(flow_module, attr_name)

    return discovered


def resolve_flow(spec: DeploymentSpec) -> Any:
    if spec.flow_obj is not None:
        return spec.flow_obj

    if spec.flow_module is None:
        raise ValueError(
            f"Deployment spec '{spec.deployment_name}' must provide flow_obj or flow_module"
        )

    flow_name = spec.flow_alias or spec.flow_key
    discovered = _discover_flow_map(spec.flow_module)

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


def load_deploy_spec(deploy_path: Path) -> dict[str, Any]:
    with open(deploy_path, "r") as file:
        return safe_load(file)


def get_work_pool_name(deploy_cfg: dict[str, Any], default: str = "local") -> str:
    return deploy_cfg.get("work_pool_name", default)


def resolve_deployment_source(
    deploy_cfg: dict[str, Any],
    default_local_dir: Path,
    source_mode_override: str | None = None,
    log_context: str | None = None,
) -> Any:
    source_cfg = deploy_cfg.get("source", {})
    if source_cfg is None:
        source_cfg = {}

    mode = (source_mode_override or source_cfg.get("mode") or "local").lower()
    source_mode_origin = (
        "env:PREFECT_SOURCE_MODE"
        if source_mode_override
        else "deploy_cfg.source.mode"
        if source_cfg.get("mode")
        else "default:local"
    )

    if mode == "local":
        local_path = source_cfg.get("local_path")
        source = str(local_path) if local_path else str(default_local_dir)
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

        # Import lazily so tests and local-only runs do not require Git storage objects.
        from prefect.runner.storage import GitRepository

        branch = git_cfg.get("branch", "main")
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
    """Raise ValueError if param/deploy flow mappings are missing or not aligned."""
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

    for flow_key, deploy_meta in deploy_cfg.get("flows", {}).items():
        if not isinstance(deploy_meta, dict):
            continue

        module_name = deploy_meta["module"]
        flow_name = deploy_meta.get("flow_alias") or flow_key
        default_entrypoint_file = (
            f"{entrypoint_root}/{module_name}.py" if entrypoint_root else f"{module_name}.py"
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
                include_work_pool_in_deployment=deploy_meta.get(
                    "include_work_pool_in_deployment", False
                ),
                trigger_builder=(
                    (lambda flow_triggers: (lambda _cfg, _interval: build_triggers(flow_triggers)))
                    (deploy_meta["triggers"])
                    if "triggers" in deploy_meta
                    else None
                ),
            )
        )

    return specs


def create_deployments(
    *,
    specs: list[DeploymentSpec],
    param_cfg: dict[str, Any],
    deploy_cfg: dict[str, Any],
    source: Any,
    work_pool_name: str,
) -> tuple[list[Any], list[Any]]:
    flows_cfg = param_cfg["flows"]
    deploy_flows = deploy_cfg["flows"]
    time_offset_targets = get_time_offset_targets(deploy_cfg)
    time_offset_seconds = _compute_time_offset_seconds(deploy_cfg.get("flow_start_time"))

    grouped: list[Any] = []
    standalone: list[Any] = []

    for spec in specs:
        flow_obj = resolve_flow(spec)
        deployment_kwargs: dict[str, Any] = {
            "name": spec.deployment_name,
            "parameters": sanitize_parameters(flows_cfg[spec.flow_key]),
        }

        # Inject time_offset_seconds if this flow is marked for it
        if spec.flow_key in time_offset_targets:
            deployment_kwargs["parameters"]["time_offset_seconds"] = time_offset_seconds

        if spec.trigger_builder is not None:
            deployment_kwargs["triggers"] = spec.trigger_builder(param_cfg, deploy_flows)
        else:
            interval = deploy_flows[spec.flow_key].get("interval")
            cron = build_cron(interval, spec.cron_offset)
            if cron is not None:
                deployment_kwargs["cron"] = cron

        if spec.include_work_pool_in_deployment:
            deployment_kwargs["work_pool_name"] = work_pool_name

        deployment = (
            flow_obj.from_source(
                source=source,
                entrypoint=spec.entrypoint,
            ).to_deployment(**deployment_kwargs)
        )

        if spec.apply_separately:
            standalone.append(deployment)
        else:
            grouped.append(deployment)

    return grouped, standalone
