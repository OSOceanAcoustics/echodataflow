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


def get_time_offset_targets(deploy_cfg: dict[str, Any]) -> tuple[str, ...]:
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


def prepare_config(
    config: dict[str, Any],
    *,
    time_offset_targets: tuple[str, ...] = (),
) -> dict[str, int | None]:
    init_dict = config.pop("init")
    flows_cfg = config.get("flows")
    if not isinstance(flows_cfg, dict):
        raise ValueError("Config file must contain a top-level 'flows' mapping")

    Variable.set("flow_start_time", init_dict["flow_start_time"], overwrite=True)
    Variable.set("counter_raw_copy", init_dict["counter_raw_copy"], overwrite=True)

    interval_dict: dict[str, int | None] = {}
    for flow_name, flow_cfg in flows_cfg.items():
        interval_dict[flow_name] = flow_cfg.pop("interval", None)

    time_offset_seconds = _compute_time_offset_seconds(init_dict["flow_start_time"])
    for flow_name in time_offset_targets:
        if flow_name in flows_cfg:
            flows_cfg[flow_name]["time_offset_seconds"] = time_offset_seconds

    return interval_dict


def build_cron(interval: int | None, cron_offset: int = 0) -> str | None:
    if interval is None:
        return None
    if cron_offset > 0:
        return f"{cron_offset}-59/{interval} * * * *"
    return f"*/{interval} * * * *"


def sanitize_parameters(flow_cfg: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in flow_cfg.items() if key != "triggers"}


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


def build_specs_from_deploy_spec(
    *,
    deploy_cfg: dict[str, Any],
    module_registry: dict[str, Any],
) -> list[DeploymentSpec]:
    specs: list[DeploymentSpec] = []

    for flow_key, deploy_meta in deploy_cfg.get("flows", {}).items():
        if not isinstance(deploy_meta, dict):
            continue

        module_name = deploy_meta["module"]
        flow_name = deploy_meta.get("flow_alias") or flow_key
        entrypoint = deploy_meta.get("entrypoint", f"{module_name}.py:flow_{flow_name}")
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
    config: dict[str, Any],
    interval_dict: dict[str, int | None],
    source_dir: Path,
    work_pool_name: str,
) -> tuple[list[Any], list[Any]]:
    flows_cfg = config.get("flows")
    if not isinstance(flows_cfg, dict):
        raise ValueError("Config file must contain a top-level 'flows' mapping")

    grouped: list[Any] = []
    standalone: list[Any] = []

    for spec in specs:
        flow_obj = resolve_flow(spec)
        deployment_kwargs: dict[str, Any] = {
            "name": spec.deployment_name,
            "parameters": sanitize_parameters(flows_cfg[spec.flow_key]),
        }

        if spec.trigger_builder is not None:
            deployment_kwargs["triggers"] = spec.trigger_builder(config, interval_dict)
        elif "triggers" in flows_cfg[spec.flow_key]:
            deployment_kwargs["triggers"] = build_triggers(flows_cfg[spec.flow_key]["triggers"])
        else:
            cron = build_cron(interval_dict[spec.flow_key], spec.cron_offset)
            if cron is not None:
                deployment_kwargs["cron"] = cron

        if spec.include_work_pool_in_deployment:
            deployment_kwargs["work_pool_name"] = work_pool_name

        deployment = (
            flow_obj.from_source(
                source=str(source_dir),
                entrypoint=spec.entrypoint,
            ).to_deployment(**deployment_kwargs)
        )

        if spec.apply_separately:
            standalone.append(deployment)
        else:
            grouped.append(deployment)

    return grouped, standalone
